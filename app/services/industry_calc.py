"""
Void Market — Industry Calculator Service

Full industry cost calculator like EVE Tycoon's:
- Blueprint + ME/TE lookup
- Material costs from Jita prices or local prices
- Facility bonuses (Station/Raitaru/Azbel/Sotiyo)
- Structure rig bonuses (material reduction)
- System cost index from ESI
- Job installation cost breakdown
- Export: Bill of Materials, Shopping List (Multibuy format)
"""
import logging
import math
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import (
    SdeBlueprint, SdeBlueprintMaterial, SdeType,
    JitaPrice, MarketOrder, StructureRig,
)
from app.services.esi_client import esi_client
from app.config import settings

logger = logging.getLogger("void_market.industry_calc")

# Facility material bonuses (percentage reduction)
FACILITY_BONUSES = {
    "station": 0.0,
    "raitaru": 1.0,   # -1% materials
    "azbel": 1.0,     # -1% materials (same as raitaru for most)
    "sotiyo": 1.0,    # -1% materials
}

# Facility cost bonuses
FACILITY_COST_BONUSES = {
    "station": 0.0,
    "raitaru": 3.0,   # -3% job cost
    "azbel": 4.0,     # -4% job cost
    "sotiyo": 5.0,    # -5% job cost
}

# Rig material efficiency bonuses by security status
# Format: {sec_class: bonus_multiplier}
RIG_SEC_MULTIPLIER = {
    "high": 1.0,
    "low": 1.9,
    "null": 2.1,
    "wh": 2.1,
}


class IndustryCalculator:
    """Full industry cost calculator."""

    async def calculate(
        self,
        db: AsyncSession,
        blueprint_type_id: int = None,
        product_type_id: int = None,
        runs: int = 1,
        me: int = 0,
        te: int = 0,
        facility: str = "station",
        rig_bonus_pct: float = 0.0,
        system_cost_index: float = 0.0,
        facility_tax: float = 0.0,
        price_source: str = "jita_sell",
        sell_region: str = "jita",
        broker_buy_pct: float = 1.0,
        broker_sell_pct: float = 1.0,
        sales_tax_pct: float = 3.36,
    ) -> dict:
        """
        Calculate manufacturing cost and profit for a blueprint.

        Args:
            blueprint_type_id: The blueprint type_id (or product_type_id to look up)
            product_type_id: The product being manufactured
            runs: Number of manufacturing runs
            me: Material Efficiency level (0-10)
            te: Time Efficiency level (0-20)
            facility: station, raitaru, azbel, sotiyo
            rig_bonus_pct: Total rig material reduction %
            system_cost_index: System cost index (0-1, typically 0.01-0.10)
            facility_tax: Facility tax % (typically 0-10)
            price_source: jita_sell, jita_buy, local_sell
            sell_region: jita, local — where you plan to sell the output
        """
        # Find blueprint
        bp = None
        if blueprint_type_id:
            bp = await db.get(SdeBlueprint, blueprint_type_id)
        elif product_type_id:
            result = await db.execute(
                select(SdeBlueprint)
                .where(SdeBlueprint.product_type_id == product_type_id)
            )
            bp = result.scalar_one_or_none()

        if not bp:
            return {"error": "Blueprint not found"}

        # Get materials
        mat_result = await db.execute(
            select(SdeBlueprintMaterial, SdeType.name)
            .outerjoin(SdeType, SdeType.type_id == SdeBlueprintMaterial.material_type_id)
            .where(SdeBlueprintMaterial.blueprint_type_id == bp.blueprint_type_id)
        )
        materials_raw = mat_result.all()

        if not materials_raw:
            return {"error": "No materials found for blueprint"}

        # Get product info
        product = await db.get(SdeType, bp.product_type_id)
        product_name = product.name if product else f"Type {bp.product_type_id}"

        # Calculate material quantities with ME + facility + rig bonuses
        me_factor = 1.0 - (me / 100.0)
        facility_mat_bonus = FACILITY_BONUSES.get(facility, 0) / 100.0
        rig_factor = 1.0 - (rig_bonus_pct / 100.0)

        materials = []
        total_material_cost_gross = 0
        total_material_cost_net = 0

        for bpmat, mat_name in materials_raw:
            base_qty = bpmat.quantity

            # Apply ME: ceil(base_qty * runs * me_factor)
            # But minimum 1 per run for non-zero base
            if base_qty > 0:
                adjusted_qty = max(runs, math.ceil(base_qty * runs * me_factor))
            else:
                adjusted_qty = 0

            # Waste = what ME saved
            waste = (base_qty * runs) - adjusted_qty

            # Apply facility + rig bonus (further reduction)
            final_qty = max(runs, math.ceil(adjusted_qty * (1.0 - facility_mat_bonus) * rig_factor))

            # Get unit price
            unit_price = await self._get_material_price(db, bpmat.material_type_id, price_source)

            gross_cost = unit_price * adjusted_qty
            net_cost = unit_price * final_qty

            materials.append({
                "type_id": bpmat.material_type_id,
                "name": mat_name or f"Type {bpmat.material_type_id}",
                "base_quantity": base_qty,
                "quantity": final_qty,
                "waste": max(0, waste),
                "unit_price": round(unit_price, 2),
                "gross_cost": round(gross_cost, 2),
                "net_cost": round(net_cost, 2),
            })

            total_material_cost_gross += gross_cost
            total_material_cost_net += net_cost

        # Job installation cost
        # EIV (Estimated Item Value) is used for system cost calculation
        # Simplified: sum of adjusted quantities × adjusted prices
        eiv = total_material_cost_gross  # Approximation
        system_cost = eiv * system_cost_index
        facility_cost_bonus = FACILITY_COST_BONUSES.get(facility, 0) / 100.0
        structure_bonus = system_cost * facility_cost_bonus
        gross_install = system_cost - structure_bonus
        facility_tax_cost = eiv * (facility_tax / 100.0)
        scc_surcharge = eiv * 0.04  # 4% SCC surcharge
        net_install = gross_install + facility_tax_cost + scc_surcharge

        total_cost = total_material_cost_net + net_install

        # Output sell price
        sell_price = await self._get_sell_price(db, bp.product_type_id, sell_region)
        total_sell_gross = sell_price * runs * bp.product_quantity
        sell_broker = total_sell_gross * (broker_sell_pct / 100.0)
        sell_tax = total_sell_gross * (sales_tax_pct / 100.0)
        total_sell_net = total_sell_gross - sell_broker - sell_tax

        profit = total_sell_net - total_cost
        margin = (profit / total_sell_gross * 100) if total_sell_gross > 0 else 0

        # Build shopping list (Multibuy format)
        shopping_list = "\n".join(
            f"{m['name']} {m['quantity']}" for m in materials
        )

        # Bill of materials per job
        bom = "\n".join(
            f"{m['name']}: {m['quantity']} × {m['unit_price']:,.2f} = {m['net_cost']:,.2f} ISK"
            for m in materials
        )

        return {
            "blueprint_type_id": bp.blueprint_type_id,
            "product_type_id": bp.product_type_id,
            "product_name": product_name,
            "product_quantity": bp.product_quantity,
            "runs": runs,
            "me": me,
            "te": te,
            "facility": facility,

            "materials": materials,
            "total_material_cost_gross": round(total_material_cost_gross, 2),
            "total_material_cost_net": round(total_material_cost_net, 2),

            "job_install": {
                "estimated_item_value": round(eiv, 2),
                "system_cost_index": system_cost_index,
                "system_cost": round(system_cost, 2),
                "structure_bonus": round(structure_bonus, 2),
                "gross_install_cost": round(gross_install, 2),
                "facility_tax": round(facility_tax_cost, 2),
                "scc_surcharge": round(scc_surcharge, 2),
                "net_install_cost": round(net_install, 2),
            },

            "output": {
                "unit_price": round(sell_price, 2),
                "gross_sell": round(total_sell_gross, 2),
                "broker_fee": round(sell_broker, 2),
                "sales_tax": round(sell_tax, 2),
                "net_sell": round(total_sell_net, 2),
            },

            "results": {
                "total_cost": round(total_cost, 2),
                "total_sell": round(total_sell_net, 2),
                "profit": round(profit, 2),
                "margin_pct": round(margin, 2),
                "unit_cost": round(total_cost / max(runs * bp.product_quantity, 1), 2),
                "unit_profit": round(profit / max(runs * bp.product_quantity, 1), 2),
            },

            "export": {
                "shopping_list": shopping_list,
                "bill_of_materials": bom,
            },
        }

    async def _get_material_price(self, db: AsyncSession, type_id: int, source: str) -> float:
        """Get material price from configured source."""
        if source in ("jita_sell", "jita_buy"):
            jp = await db.get(JitaPrice, type_id)
            if jp:
                return jp.sell_min if source == "jita_sell" else (jp.buy_max or jp.sell_min or 0)
        elif source == "local_sell":
            from sqlalchemy import func
            result = await db.execute(
                select(func.min(MarketOrder.price))
                .where(MarketOrder.type_id == type_id)
                .where(MarketOrder.is_buy_order == False)
                .where(MarketOrder.location_id == settings.keepstar_structure_id)
            )
            price = result.scalar()
            if price:
                return price
            # Fallback to Jita
            jp = await db.get(JitaPrice, type_id)
            if jp:
                return jp.sell_min or 0
        return 0

    async def _get_sell_price(self, db: AsyncSession, type_id: int, region: str) -> float:
        """Get expected sell price for output."""
        if region == "local":
            from sqlalchemy import func
            result = await db.execute(
                select(func.min(MarketOrder.price))
                .where(MarketOrder.type_id == type_id)
                .where(MarketOrder.is_buy_order == False)
                .where(MarketOrder.location_id == settings.keepstar_structure_id)
            )
            price = result.scalar()
            if price:
                return price

        # Default to Jita
        jp = await db.get(JitaPrice, type_id)
        if jp:
            return jp.sell_min or 0
        return 0

    async def search_blueprints(self, db: AsyncSession, query: str, limit: int = 20) -> list:
        """Search for blueprints by product name."""
        result = await db.execute(
            select(SdeBlueprint, SdeType.name)
            .join(SdeType, SdeType.type_id == SdeBlueprint.product_type_id)
            .where(SdeType.name.ilike(f"%{query}%"))
            .limit(limit)
        )
        return [{
            "blueprint_type_id": bp.blueprint_type_id,
            "product_type_id": bp.product_type_id,
            "product_name": name,
            "product_quantity": bp.product_quantity,
        } for bp, name in result.all()]


industry_calculator = IndustryCalculator()
