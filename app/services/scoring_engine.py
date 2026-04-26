"""
Void Market — Opportunity Scoring Engine

Combines all data sources to answer:
"What should I build, buy, or import to make the most ISK?"

Scoring factors:
1. Doctrine demand       — How many units do doctrines need?
2. Market deficit         — How understocked is the local market?
3. Contract coverage      — What's already on alliance contracts?
4. Recent burn rate       — How fast are these items being lost in fights?
5. Market velocity        — How fast do items sell?
6. Profit margin          — What's the spread between cost and local sell?
7. Recommendation         — Build (if cheaper), buy+import (if faster), or hold

Higher score = more urgent opportunity.
"""
import logging
from datetime import datetime, timezone
from sqlalchemy import select, delete, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import (
    Opportunity, SdeType, DoctrineFit, DoctrineFitItem,
    MarketOrder, JitaPrice, ContractItem,
)
from app.services.market_service import market_service
from app.services.contract_service import contract_service
from app.services.zkill_service import zkill_service
from app.config import settings

logger = logging.getLogger("void_market.scoring")


class OpportunityScoringEngine:
    """Calculates and ranks market opportunities."""

    async def calculate_all(self, db: AsyncSession) -> dict:
        """
        Run the full scoring pipeline across all doctrine items.

        Steps:
        1. Aggregate doctrine demand (BOM × min_stock)
        2. Pull current market stock
        3. Pull contract stock
        4. Pull recent loss data
        5. Calculate velocity and margins
        6. Score and rank everything
        """
        logger.info("Starting opportunity scoring run...")

        # Clear previous scores
        await db.execute(delete(Opportunity))

        # 1. Aggregate doctrine demand
        demand = await self._get_doctrine_demand(db)
        logger.info(f"Doctrine demand: {len(demand)} unique items")

        # 2. Current market stock (sell orders)
        market_stock = await self._get_market_stock(db)

        # 3. Contract stock (ESI + manual entries)
        contract_stock = await contract_service.get_contract_item_counts(db)

        # Merge manual contract counts
        from app.models.database import ManualContract
        from sqlalchemy import func as sqlfunc
        manual_result = await db.execute(
            select(
                ManualContract.ship_type_id,
                sqlfunc.sum(ManualContract.quantity).label("total"),
            ).group_by(ManualContract.ship_type_id)
        )
        for row in manual_result.fetchall():
            contract_stock[row[0]] = contract_stock.get(row[0], 0) + row[1]

        # 4. Recent losses (items destroyed in fights)
        recent_losses = await zkill_service.get_module_loss_summary(db, days=7)

        # 5. Score each item
        scored = 0
        for type_id, needed in demand.items():
            on_market = market_stock.get(type_id, 0)
            on_contracts = contract_stock.get(type_id, 0)
            lost_recently = recent_losses.get(type_id, 0)

            # Available supply
            total_supply = on_market + on_contracts

            # Deficit: how many more do we need?
            deficit = max(0, needed - total_supply)

            # Velocity
            velocity = await market_service.calculate_velocity(db, type_id)

            # Prices
            jita = await db.get(JitaPrice, type_id)
            jita_buy = jita.sell_min if jita else None  # What we'd pay in Jita

            # Local sell price (lowest current sell order)
            sell_result = await db.execute(
                select(func.min(MarketOrder.price))
                .where(MarketOrder.type_id == type_id)
                .where(MarketOrder.location_id == settings.keepstar_structure_id)
                .where(MarketOrder.is_buy_order == False)
            )
            local_sell = sell_result.scalar()

            # Build cost
            build_cost = await market_service.estimate_build_cost(db, type_id)

            # Best acquisition cost
            best_cost = None
            recommendation = "import"

            if build_cost and jita_buy:
                if build_cost < jita_buy:
                    best_cost = build_cost
                    recommendation = "build"
                else:
                    best_cost = jita_buy
                    recommendation = "import"
            elif build_cost:
                best_cost = build_cost
                recommendation = "build"
            elif jita_buy:
                best_cost = jita_buy
                recommendation = "import"

            # Profit margin
            profit_margin = None
            if local_sell and best_cost and best_cost > 0:
                profit_margin = ((local_sell - best_cost) / best_cost) * 100

            # ─── SCORING ────────────────────────────────────
            score = self._compute_score(
                deficit=deficit,
                needed=needed,
                velocity=velocity,
                lost_recently=lost_recently,
                profit_margin=profit_margin,
                on_market=on_market,
            )

            db.add(Opportunity(
                type_id=type_id,
                score=score,
                doctrine_demand=needed,
                market_stock=on_market,
                contract_stock=on_contracts,
                recent_losses=lost_recently,
                sell_velocity=velocity,
                jita_buy_price=jita_buy,
                local_sell_price=local_sell,
                build_cost=build_cost,
                profit_margin=profit_margin,
                recommendation=recommendation,
                calculated_at=datetime.now(timezone.utc),
            ))
            scored += 1

        await db.commit()
        logger.info(f"Scored {scored} items")
        return {"items_scored": scored}

    def _compute_score(
        self,
        deficit: int,
        needed: int,
        velocity: float,
        lost_recently: int,
        profit_margin: float | None,
        on_market: int,
    ) -> float:
        """
        Compute opportunity score. Higher = better opportunity.

        Components:
        - Deficit score: Items we're short on (0-40 points)
        - Velocity score: Items that sell fast (0-20 points)
        - Loss score: Items being destroyed in fights (0-20 points)
        - Margin score: Items with good profit margins (0-20 points)
        """
        score = 0.0

        # Deficit score (0-40)
        # Max score when completely out of stock on a high-demand item
        if needed > 0:
            deficit_ratio = deficit / needed
            score += deficit_ratio * 40

        # Velocity score (0-20)
        # Items selling >5/day get full score
        if velocity > 0:
            velocity_score = min(velocity / 5.0, 1.0) * 20
            score += velocity_score

        # Loss score (0-20)
        # Items lost in fights indicate urgent need for restocking
        if lost_recently > 0:
            loss_score = min(lost_recently / 20.0, 1.0) * 20
            score += loss_score

        # Margin score (0-20)
        # 20%+ margin gets full score
        if profit_margin and profit_margin > 0:
            margin_score = min(profit_margin / 20.0, 1.0) * 20
            score += margin_score

        # Bonus: completely out of stock
        if on_market == 0 and needed > 0:
            score += 10

        return round(score, 2)

    async def _get_doctrine_demand(self, db: AsyncSession) -> dict[int, int]:
        """
        Aggregate demand across all doctrines.
        Returns: {type_id: total_units_needed}

        Calculates: item_qty_per_fit × min_stock for each fit,
        summed across all doctrines.
        """
        result = await db.execute(
            select(
                DoctrineFitItem.type_id,
                func.sum(DoctrineFitItem.quantity * DoctrineFit.min_stock).label("total"),
            )
            .join(DoctrineFit, DoctrineFitItem.fit_id == DoctrineFit.id)
            .group_by(DoctrineFitItem.type_id)
        )

        return {row[0]: row[1] for row in result.fetchall()}

    async def _get_market_stock(self, db: AsyncSession) -> dict[int, int]:
        """
        Get current sell order volume by type in the Keepstar.
        Returns: {type_id: total_sell_volume}
        """
        result = await db.execute(
            select(
                MarketOrder.type_id,
                func.sum(MarketOrder.volume_remain).label("total"),
            )
            .where(MarketOrder.location_id == settings.keepstar_structure_id)
            .where(MarketOrder.is_buy_order == False)
            .group_by(MarketOrder.type_id)
        )

        return {row[0]: row[1] for row in result.fetchall()}

    async def get_ranked_opportunities(
        self,
        db: AsyncSession,
        limit: int = 50,
        min_score: float = 0,
    ) -> list[dict]:
        """Get opportunities ranked by score."""
        result = await db.execute(
            select(Opportunity)
            .where(Opportunity.score >= min_score)
            .order_by(Opportunity.score.desc())
            .limit(limit)
        )
        opportunities = result.scalars().all()

        output = []
        for opp in opportunities:
            item = await db.get(SdeType, opp.type_id)
            output.append({
                "type_id": opp.type_id,
                "name": item.name if item else f"Type {opp.type_id}",
                "score": opp.score,
                "doctrine_demand": opp.doctrine_demand,
                "market_stock": opp.market_stock,
                "contract_stock": opp.contract_stock,
                "recent_losses": opp.recent_losses,
                "sell_velocity": opp.sell_velocity,
                "jita_buy_price": opp.jita_buy_price,
                "local_sell_price": opp.local_sell_price,
                "build_cost": opp.build_cost,
                "profit_margin": round(opp.profit_margin, 1) if opp.profit_margin else None,
                "recommendation": opp.recommendation,
                "calculated_at": opp.calculated_at.isoformat() if opp.calculated_at else None,
            })

        return output


# Singleton
scoring_engine = OpportunityScoringEngine()
