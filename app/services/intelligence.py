"""
Void Market — Market Intelligence Service (Complete)

All QSNA-style reports plus EVE Tycoon features:
- Trending Up/Down (weekly average comparison)
- Most Underpriced (current vs 30d average)
- Volume Anomaly (recent vs historical volume)
- Items to Seed (high Jita volume, zero local)
- Over Supply with 10% price filter (only stock within 10% of min sell)
- Market cap % on volume leaders
- Daily + Hourly profit charts
- Min ISK / min orders filtering on all reports
"""
import logging
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import (
    MarketHistory, MarketOrder, MarketSnapshot, JitaPrice,
    GoonmetricsCache, SdeType, AppSetting,
)
from app.config import settings

logger = logging.getLogger("void_market.intelligence")


class IntelligenceService:

    # ─── Trending (Weekly Avg Comparison) ────────────────

    async def get_trending(self, db: AsyncSession, market: str = "jita", limit: int = 20,
                           min_isk: float = 0, min_orders: int = 0) -> dict:
        today = datetime.now(timezone.utc).date()
        tw_start = (today - timedelta(days=7)).isoformat()
        lw_start = (today - timedelta(days=14)).isoformat()
        lw_end = (today - timedelta(days=7)).isoformat()

        this_week = await db.execute(text("""
            SELECT type_id, AVG(average) as avg_price, SUM(volume) as total_vol, SUM(order_count) as total_orders
            FROM market_history WHERE date >= :start AND region_id = 10000002
            GROUP BY type_id HAVING total_vol > 100
        """).bindparams(start=tw_start))
        this_data = {r[0]: {"avg": r[1], "vol": r[2], "orders": r[3]} for r in this_week.fetchall()}

        last_week = await db.execute(text("""
            SELECT type_id, AVG(average) as avg_price, SUM(volume) as total_vol
            FROM market_history WHERE date >= :start AND date < :end AND region_id = 10000002
            GROUP BY type_id HAVING total_vol > 100
        """).bindparams(start=lw_start, end=lw_end))
        last_data = {r[0]: {"avg": r[1], "vol": r[2]} for r in last_week.fetchall()}

        changes = []
        for tid, tw in this_data.items():
            if tid not in last_data or last_data[tid]["avg"] <= 0:
                continue
            # Apply min ISK filter (daily trade value)
            daily_isk = tw["avg"] * (tw["vol"] / 7)
            if min_isk > 0 and daily_isk < min_isk:
                continue
            # Apply min orders filter
            daily_orders = (tw["orders"] or 0) / 7
            if min_orders > 0 and daily_orders < min_orders:
                continue
            pct = ((tw["avg"] - last_data[tid]["avg"]) / last_data[tid]["avg"]) * 100
            changes.append({"type_id": tid, "price": round(tw["avg"], 2), "change": round(pct, 2), "volume": tw["vol"]})

        type_ids = [c["type_id"] for c in changes]
        if type_ids:
            names = dict((await db.execute(select(SdeType.type_id, SdeType.name).where(SdeType.type_id.in_(type_ids[:500])))).fetchall())
            for c in changes:
                c["name"] = names.get(c["type_id"], f"Type {c['type_id']}")

        up = sorted([c for c in changes if c["change"] > 0], key=lambda x: -x["change"])[:limit]
        down = sorted([c for c in changes if c["change"] < 0], key=lambda x: x["change"])[:limit]
        return {"up": up, "down": down}

    # ─── Underpriced ─────────────────────────────────────

    async def get_underpriced(self, db: AsyncSession, days: int = 30, limit: int = 30,
                              min_isk: float = 0, min_orders: int = 0) -> list:
        today = datetime.now(timezone.utc).date()
        cutoff = (today - timedelta(days=days)).isoformat()

        hist = await db.execute(text("""
            SELECT type_id, AVG(average) as hist_avg, AVG(volume) as avg_vol, AVG(order_count) as avg_orders
            FROM market_history WHERE date >= :cutoff AND region_id = 10000002
            GROUP BY type_id HAVING avg_vol > 10
        """).bindparams(cutoff=cutoff))
        hist_data = {r[0]: {"avg": r[1], "vol": r[2], "orders": r[3]} for r in hist.fetchall()}

        current = dict((await db.execute(select(JitaPrice.type_id, JitaPrice.sell_min).where(JitaPrice.sell_min > 0))).fetchall())

        items = []
        for tid, h in hist_data.items():
            if tid not in current or h["avg"] <= 0:
                continue
            daily_isk = h["avg"] * h["vol"]
            if min_isk > 0 and daily_isk < min_isk:
                continue
            if min_orders > 0 and (h["orders"] or 0) < min_orders:
                continue
            cp = current[tid]
            idx = (cp / h["avg"]) * 100
            if idx < 90:
                items.append({"type_id": tid, "current_price": round(cp, 2), "avg_price": round(h["avg"], 2),
                              "price_index": round(idx, 1), "discount_pct": round(100 - idx, 1), "avg_volume": round(h["vol"], 0)})

        type_ids = [i["type_id"] for i in items]
        if type_ids:
            names = dict((await db.execute(select(SdeType.type_id, SdeType.name).where(SdeType.type_id.in_(type_ids[:500])))).fetchall())
            for i in items:
                i["name"] = names.get(i["type_id"], f"Type {i['type_id']}")

        return sorted(items, key=lambda x: x["price_index"])[:limit]

    # ─── Volume Anomaly ──────────────────────────────────

    async def get_volume_anomaly(self, db: AsyncSession, min_ratio: float = 2.0, limit: int = 30,
                                  min_isk: float = 0, min_orders: int = 0) -> list:
        # Use the actual latest date in market_history, not 'now'
        # ESI history updates daily at downtime — data may lag 1-4 days
        latest_date_result = await db.execute(text(
            "SELECT MAX(date) FROM market_history WHERE region_id = 10000002"
        ))
        latest_date = latest_date_result.scalar()
        if not latest_date:
            return []

        recent_start = (datetime.fromisoformat(latest_date) - timedelta(days=3)).date().isoformat()
        hist_start = (datetime.fromisoformat(latest_date) - timedelta(days=30)).date().isoformat()

        recent = await db.execute(text("""
            SELECT type_id, AVG(volume) as recent_vol FROM market_history
            WHERE date >= :start AND region_id = 10000002 GROUP BY type_id HAVING recent_vol > 10
        """).bindparams(start=recent_start))
        recent_data = {r[0]: r[1] for r in recent.fetchall()}

        hist = await db.execute(text("""
            SELECT type_id, AVG(volume) as hist_vol, AVG(average) as avg_price FROM market_history
            WHERE date >= :start AND date < :recent AND region_id = 10000002 GROUP BY type_id HAVING hist_vol > 5
        """).bindparams(start=hist_start, recent=recent_start))
        hist_data = {r[0]: {"vol": r[1], "price": r[2]} for r in hist.fetchall()}

        items = []
        for tid, rv in recent_data.items():
            if tid not in hist_data or hist_data[tid]["vol"] <= 0:
                continue
            h = hist_data[tid]
            if min_isk > 0 and h["price"] * h["vol"] < min_isk:
                continue
            ratio = rv / h["vol"]
            if ratio >= min_ratio:
                items.append({"type_id": tid, "recent_volume": round(rv, 0), "avg_volume": round(h["vol"], 0), "ratio": round(ratio, 1)})

        type_ids = [i["type_id"] for i in items]
        if type_ids:
            names = dict((await db.execute(select(SdeType.type_id, SdeType.name).where(SdeType.type_id.in_(type_ids[:500])))).fetchall())
            for i in items:
                i["name"] = names.get(i["type_id"], f"Type {i['type_id']}")

        return sorted(items, key=lambda x: -x["ratio"])[:limit]

    # ─── Items to Seed ───────────────────────────────────

    async def get_items_to_seed(self, db: AsyncSession, limit: int = 50) -> list:
        today = datetime.now(timezone.utc).date()
        week_start = (today - timedelta(days=7)).isoformat()

        jita_vol = await db.execute(text("""
            SELECT type_id, SUM(volume) as wk_vol, AVG(average) as avg_price
            FROM market_history WHERE date >= :start AND region_id = 10000002
            GROUP BY type_id HAVING wk_vol > 50 ORDER BY wk_vol DESC
        """).bindparams(start=week_start))
        jita_data = {r[0]: {"vol": r[1], "price": r[2]} for r in jita_vol.fetchall()}

        local_stock = dict((await db.execute(
            select(MarketOrder.type_id, func.sum(MarketOrder.volume_remain))
            .where(MarketOrder.location_id == settings.keepstar_structure_id, MarketOrder.is_buy_order == False)
            .group_by(MarketOrder.type_id)
        )).fetchall())

        items = []
        for tid, j in jita_data.items():
            local = local_stock.get(tid, 0)
            if local <= 2:
                vol = 0
                try:
                    from app.models.database import SdeType as ST
                    st = await db.get(ST, tid)
                    vol = st.volume or 0 if st else 0
                except:
                    pass
                freight = vol * settings.freight_cost_per_m3
                items.append({"type_id": tid, "jita_weekly_vol": round(j["vol"], 0), "jita_price": round(j["price"], 2),
                              "local_stock": local, "import_cost": round(j["price"] + freight, 2),
                              "weekly_isk": round(j["vol"] * j["price"], 2)})

        type_ids = [i["type_id"] for i in items]
        if type_ids:
            names = dict((await db.execute(select(SdeType.type_id, SdeType.name).where(SdeType.type_id.in_(type_ids[:500])))).fetchall())
            for i in items:
                i["name"] = names.get(i["type_id"], f"Type {i['type_id']}")

        return sorted(items, key=lambda x: -x["weekly_isk"])[:limit]

    # ─── Volume Leaders with Market Cap % ────────────────

    async def get_volume_leaders(self, db: AsyncSession, market: str = "jita", limit: int = 30,
                                  min_isk: float = 0, min_orders: int = 0) -> list:
        today = datetime.now(timezone.utc).date()
        week_start = (today - timedelta(days=7)).isoformat()

        result = await db.execute(text("""
            SELECT type_id, AVG(average) as avg_price, AVG(volume) as avg_vol, AVG(order_count) as avg_orders
            FROM market_history WHERE date >= :start AND region_id = 10000002
            GROUP BY type_id HAVING avg_vol > 0
        """).bindparams(start=week_start))
        rows = result.fetchall()

        items = []
        total_market_isk = 0
        for r in rows:
            daily_isk = r[1] * r[2]
            total_market_isk += daily_isk
            if min_isk > 0 and daily_isk < min_isk:
                continue
            if min_orders > 0 and (r[3] or 0) < min_orders:
                continue
            items.append({"type_id": r[0], "price": round(r[1], 2), "vol_day": round(r[2], 0),
                          "isk_day": round(daily_isk, 2), "orders_day": round(r[3] or 0, 0)})

        # Add market cap %
        for i in items:
            i["market_cap_pct"] = round((i["isk_day"] / total_market_isk * 100) if total_market_isk > 0 else 0, 2)

        # Get names
        type_ids = [i["type_id"] for i in items]
        if type_ids:
            names = dict((await db.execute(select(SdeType.type_id, SdeType.name).where(SdeType.type_id.in_(type_ids[:500])))).fetchall())
            for i in items:
                i["name"] = names.get(i["type_id"], f"Type {i['type_id']}")

        return {
            "items": sorted(items, key=lambda x: -x["isk_day"])[:limit],
            "total_market_isk_day": round(total_market_isk, 2),
        }

    # ─── Over Supply with 10% Price Filter ───────────────

    async def get_over_supply(self, db: AsyncSession, market: str = "jita", limit: int = 30) -> list:
        """
        Over supply items. For oversupply, only count stock within 10% of the lowest sell price.
        This filters out ridiculously overpriced outlier orders.
        """
        if market != "local":
            return []  # Only meaningful for local market

        # Get all sell orders on Keepstar grouped by type
        result = await db.execute(text("""
            SELECT type_id, MIN(price) as min_price
            FROM market_orders
            WHERE location_id = :loc AND is_buy_order = 0
            GROUP BY type_id
        """).bindparams(loc=settings.keepstar_structure_id))
        min_prices = {r[0]: r[1] for r in result.fetchall()}

        # For each type, count volume within 10% of min price
        filtered_stock = {}
        for tid, mp in min_prices.items():
            threshold = mp * 1.10
            vol_result = await db.execute(text("""
                SELECT SUM(volume_remain) FROM market_orders
                WHERE type_id = :tid AND location_id = :loc AND is_buy_order = 0 AND price <= :threshold
            """).bindparams(tid=tid, loc=settings.keepstar_structure_id, threshold=threshold))
            filtered_stock[tid] = vol_result.scalar() or 0

        # Get weekly volumes from Goonmetrics
        gm_result = await db.execute(select(GoonmetricsCache.type_id, GoonmetricsCache.weekly_movement))
        weekly = dict(gm_result.fetchall())

        items = []
        for tid, stock in filtered_stock.items():
            wv = weekly.get(tid, 0)
            if wv <= 0:
                continue
            days = (stock / wv) * 7
            if days > 14:  # More than 2 weeks of stock = oversupplied
                items.append({"type_id": tid, "stock": stock, "weekly_vol": round(wv, 0),
                              "days": round(days, 1), "min_price": min_prices.get(tid, 0)})

        type_ids = [i["type_id"] for i in items]
        if type_ids:
            names = dict((await db.execute(select(SdeType.type_id, SdeType.name).where(SdeType.type_id.in_(type_ids[:500])))).fetchall())
            for i in items:
                i["name"] = names.get(i["type_id"], f"Type {i['type_id']}")

        return sorted(items, key=lambda x: -x["days"])[:limit]

    # ─── Daily Profit Chart Data ─────────────────────────

    async def get_daily_profits(self, db: AsyncSession, days: int = 30) -> list:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        trade = dict((await db.execute(text("""
            SELECT DATE(date) as day, SUM(profit) as total FROM trade_profits
            WHERE date >= :cutoff GROUP BY DATE(date) ORDER BY day
        """).bindparams(cutoff=cutoff))).fetchall())

        mfg = dict((await db.execute(text("""
            SELECT DATE(date) as day, SUM(profit) as total FROM manufacturing_profits
            WHERE date >= :cutoff GROUP BY DATE(date) ORDER BY day
        """).bindparams(cutoff=cutoff))).fetchall())

        contracts = dict((await db.execute(text("""
            SELECT DATE(date) as day, SUM(profit) as total FROM contract_profits
            WHERE date >= :cutoff GROUP BY DATE(date) ORDER BY day
        """).bindparams(cutoff=cutoff))).fetchall())

        all_days = sorted(set(list(trade.keys()) + list(mfg.keys()) + list(contracts.keys())))
        result = []
        running = 0
        for day in all_days:
            t = trade.get(day, 0) or 0
            m = mfg.get(day, 0) or 0
            c = contracts.get(day, 0) or 0
            running += t + m + c
            result.append({"date": str(day), "trade": round(t, 2), "manufacturing": round(m, 2),
                           "contracts": round(c, 2), "total": round(t + m + c, 2), "running_total": round(running, 2)})
        return result

    # ─── Hourly Profit Chart Data ────────────────────────

    async def get_hourly_profits(self, db: AsyncSession, days: int = 1) -> list:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        trade = await db.execute(text("""
            SELECT strftime('%Y-%m-%d %H:00', date) as hour, SUM(profit) as total
            FROM trade_profits WHERE date >= :cutoff GROUP BY hour ORDER BY hour
        """).bindparams(cutoff=cutoff))
        trade_data = dict(trade.fetchall())

        mfg = await db.execute(text("""
            SELECT strftime('%Y-%m-%d %H:00', date) as hour, SUM(profit) as total
            FROM manufacturing_profits WHERE date >= :cutoff GROUP BY hour ORDER BY hour
        """).bindparams(cutoff=cutoff))
        mfg_data = dict(mfg.fetchall())

        all_hours = sorted(set(list(trade_data.keys()) + list(mfg_data.keys())))
        result = []
        running = 0
        for hour in all_hours:
            t = trade_data.get(hour, 0) or 0
            m = mfg_data.get(hour, 0) or 0
            running += t + m
            result.append({"hour": hour, "trade": round(t, 2), "manufacturing": round(m, 2),
                           "total": round(t + m, 2), "running_total": round(running, 2)})
        return result

    # ─── Doctrine Cost Comparison ────────────────────────

    async def get_doctrine_fit_costs(self, db: AsyncSession, fit_id: int) -> dict:
        """Per-module cost comparison for a doctrine fit: Jita vs Import vs Local."""
        from app.models.database import DoctrineFitItem, DoctrineFit

        fit = await db.get(DoctrineFit, fit_id)
        if not fit:
            return {"error": "Fit not found"}

        items_result = await db.execute(
            select(DoctrineFitItem, SdeType.name, SdeType.volume)
            .outerjoin(SdeType, SdeType.type_id == DoctrineFitItem.type_id)
            .where(DoctrineFitItem.fit_id == fit_id)
        )
        items = items_result.all()

        result_items = []
        total_jita = 0
        total_import = 0
        total_local = 0

        for item, name, vol in items:
            jp = await db.get(JitaPrice, item.type_id)
            jita_unit = jp.sell_min if jp and jp.sell_min else 0

            freight = (vol or 0) * settings.freight_cost_per_m3
            import_unit = jita_unit + freight

            local_r = await db.execute(
                select(func.min(MarketOrder.price))
                .where(MarketOrder.type_id == item.type_id, MarketOrder.is_buy_order == False,
                       MarketOrder.location_id == settings.keepstar_structure_id)
            )
            local_unit = local_r.scalar() or 0

            stock_r = await db.execute(
                select(func.sum(MarketOrder.volume_remain))
                .where(MarketOrder.type_id == item.type_id, MarketOrder.is_buy_order == False,
                       MarketOrder.location_id == settings.keepstar_structure_id)
            )
            stock = stock_r.scalar() or 0

            jt = jita_unit * item.quantity
            it = import_unit * item.quantity
            lt = local_unit * item.quantity
            total_jita += jt
            total_import += it
            total_local += lt

            result_items.append({
                "type_id": item.type_id, "name": name or f"Type {item.type_id}",
                "quantity": item.quantity, "slot_type": item.slot_type,
                "jita_price": round(jita_unit, 2), "import_price": round(import_unit, 2),
                "local_price": round(local_unit, 2),
                "jita_total": round(jt, 2), "import_total": round(it, 2), "local_total": round(lt, 2),
                "local_stock": stock,
                "in_stock": stock >= item.quantity,
            })

        return {
            "fit_name": fit.name, "items": result_items,
            "totals": {"jita": round(total_jita, 2), "import": round(total_import, 2), "local": round(total_local, 2),
                       "import_savings": round(total_local - total_import, 2)},
        }

    # ─── Fight → Restock Pipeline ────────────────────────

    async def get_restock_from_fights(self, db: AsyncSession, days: int = 7) -> dict:
        """Generate a restock shopping list from recent fight losses."""
        from app.models.database import FightLoss, Fight
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        # Get all losses in recent fights
        losses = await db.execute(text("""
            SELECT fl.ship_type_id, fl.fit_items, COUNT(*) as lost_count
            FROM fight_losses fl JOIN fights f ON fl.fight_id = f.id
            WHERE f.started_at >= :cutoff
            GROUP BY fl.ship_type_id
            ORDER BY lost_count DESC
        """).bindparams(cutoff=cutoff))

        restock = {}
        ship_losses = []

        for ship_type_id, fit_items_json, count in losses.fetchall():
            # Get ship name
            ship = await db.get(SdeType, ship_type_id)
            ship_name = ship.name if ship else f"Type {ship_type_id}"
            ship_losses.append({"type_id": ship_type_id, "name": ship_name, "lost": count})

            # Add ship hull to restock
            restock[ship_type_id] = restock.get(ship_type_id, 0) + count

            # Parse fit items from killmail data
            if fit_items_json and isinstance(fit_items_json, list):
                for fi in fit_items_json:
                    tid = fi.get("item_type_id") or fi.get("type_id")
                    qty = fi.get("quantity_destroyed", 0) + fi.get("quantity_dropped", 0)
                    if tid and qty > 0:
                        restock[tid] = restock.get(tid, 0) + qty

        # Get names and prices for restock items
        restock_items = []
        total_cost = 0
        for tid, qty in sorted(restock.items(), key=lambda x: -x[1]):
            sde = await db.get(SdeType, tid)
            name = sde.name if sde else f"Type {tid}"
            jp = await db.get(JitaPrice, tid)
            price = jp.sell_min if jp and jp.sell_min else 0
            cost = price * qty
            total_cost += cost

            # Check local stock
            stock_r = await db.execute(
                select(func.sum(MarketOrder.volume_remain))
                .where(MarketOrder.type_id == tid, MarketOrder.is_buy_order == False,
                       MarketOrder.location_id == settings.keepstar_structure_id)
            )
            stock = stock_r.scalar() or 0

            restock_items.append({
                "type_id": tid, "name": name, "quantity_needed": qty,
                "jita_price": round(price, 2), "total_cost": round(cost, 2),
                "local_stock": stock, "needs_import": stock < qty,
            })

        # Generate multibuy string
        multibuy = "\n".join(f"{i['name']} {i['quantity_needed']}" for i in restock_items if i["quantity_needed"] > 0)

        return {
            "ship_losses": ship_losses,
            "restock_items": restock_items[:100],
            "total_cost": round(total_cost, 2),
            "total_items": len(restock_items),
            "multibuy": multibuy,
        }


intelligence_service = IntelligenceService()
