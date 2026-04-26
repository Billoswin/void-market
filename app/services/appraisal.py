"""
Void Market — Appraisal Service

Parses item lists from multiple formats:
- EFT fits (fitting text blocks)
- Item lists ("Item Name x Quantity" or "Item Name\tQuantity")
- Cargo scans (from in-game clipboard)
- Multibuy format ("Item Name 100")

Returns per-item valuation at Jita, Import, and Local prices.
"""
import re
import logging
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import SdeType, JitaPrice, MarketOrder
from app.config import settings

logger = logging.getLogger("void_market.appraisal")


class AppraisalService:

    async def appraise(self, db: AsyncSession, text: str) -> dict:
        """Parse text input and return item valuations."""
        items = self._parse_items(text)
        if not items:
            return {"error": "No items found in input", "items": []}

        results = []
        total_jita = 0
        total_import = 0
        total_local = 0

        for name, qty in items:
            # Look up type_id by name
            r = await db.execute(
                select(SdeType).where(func.lower(SdeType.name) == func.lower(name)).limit(1)
            )
            sde = r.scalar_one_or_none()
            if not sde:
                # Try fuzzy match
                r = await db.execute(
                    select(SdeType).where(SdeType.name.ilike(f"%{name}%")).limit(1)
                )
                sde = r.scalar_one_or_none()

            if not sde:
                results.append({
                    "name": name, "quantity": qty, "type_id": None,
                    "jita_price": None, "import_price": None, "local_price": None,
                    "jita_total": None, "import_total": None, "local_total": None,
                    "error": "Item not found",
                })
                continue

            # Get Jita price
            jp = await db.get(JitaPrice, sde.type_id)
            jita_unit = jp.sell_min if jp and jp.sell_min else 0

            # Import price = Jita + freight
            vol = sde.volume or 0
            freight = vol * settings.freight_cost_per_m3
            import_unit = jita_unit + freight

            # Local price (C-J6MT min sell)
            local_r = await db.execute(
                select(func.min(MarketOrder.price))
                .where(MarketOrder.type_id == sde.type_id)
                .where(MarketOrder.is_buy_order == False)
                .where(MarketOrder.location_id == settings.keepstar_structure_id)
            )
            local_unit = local_r.scalar() or 0

            # Local stock
            stock_r = await db.execute(
                select(func.sum(MarketOrder.volume_remain))
                .where(MarketOrder.type_id == sde.type_id)
                .where(MarketOrder.is_buy_order == False)
                .where(MarketOrder.location_id == settings.keepstar_structure_id)
            )
            local_stock = stock_r.scalar() or 0

            jita_total = jita_unit * qty
            import_total = import_unit * qty
            local_total = local_unit * qty

            total_jita += jita_total
            total_import += import_total
            total_local += local_total

            results.append({
                "name": sde.name,
                "type_id": sde.type_id,
                "quantity": qty,
                "jita_price": round(jita_unit, 2),
                "import_price": round(import_unit, 2),
                "local_price": round(local_unit, 2),
                "jita_total": round(jita_total, 2),
                "import_total": round(import_total, 2),
                "local_total": round(local_total, 2),
                "local_stock": local_stock,
                "import_savings": round(local_total - import_total, 2) if local_total > 0 else None,
            })

        return {
            "items": results,
            "totals": {
                "jita": round(total_jita, 2),
                "import": round(total_import, 2),
                "local": round(total_local, 2),
                "import_savings": round(total_local - total_import, 2),
            },
            "item_count": len(results),
            "found": sum(1 for r in results if r["type_id"]),
            "not_found": sum(1 for r in results if not r["type_id"]),
        }

    def _parse_items(self, text: str) -> list[tuple[str, int]]:
        """
        Parse various EVE item list formats.
        Returns list of (item_name, quantity) tuples.
        """
        lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
        if not lines:
            return []

        # Detect EFT format: first line is [Ship Name, Fit Name]
        if lines[0].startswith("[") and "," in lines[0]:
            return self._parse_eft(lines)

        items = []
        for line in lines:
            name, qty = self._parse_line(line)
            if name:
                # Merge duplicates
                found = False
                for i, (n, q) in enumerate(items):
                    if n.lower() == name.lower():
                        items[i] = (n, q + qty)
                        found = True
                        break
                if not found:
                    items.append((name, qty))

        return items

    def _parse_eft(self, lines: list[str]) -> list[tuple[str, int]]:
        """Parse EFT fitting format."""
        items = {}

        # First line: [Ship Name, Fit Name]
        header = lines[0]
        ship_match = re.match(r'\[(.+?),', header)
        if ship_match:
            ship_name = ship_match.group(1).strip()
            items[ship_name] = items.get(ship_name, 0) + 1

        for line in lines[1:]:
            if not line or line.startswith("["):
                continue

            # Handle "Module Name x2" or "Charge Name x100"
            match = re.match(r'^(.+?)\s+x(\d+)\s*$', line)
            if match:
                name = match.group(1).strip().rstrip(",")
                qty = int(match.group(2))
            else:
                # Handle "Module Name, Charge Name" (module with loaded charge)
                parts = line.split(",")
                name = parts[0].strip()
                qty = 1
                # If there's a charge after the comma, add it separately
                if len(parts) > 1:
                    charge = parts[1].strip()
                    if charge and charge != name:
                        items[charge] = items.get(charge, 0) + 1

            if name and name not in ("[Empty High slot]", "[Empty Med slot]",
                                       "[Empty Low slot]", "[Empty Rig slot]",
                                       "[Empty Subsystem slot]"):
                items[name] = items.get(name, 0) + qty

        return list(items.items())

    def _parse_line(self, line: str) -> tuple[str, int]:
        """Parse a single line in various formats."""
        # Tab-separated: "Item Name\t42"
        if "\t" in line:
            parts = line.split("\t")
            name = parts[0].strip()
            try:
                qty = int(parts[-1].strip().replace(",", ""))
            except ValueError:
                qty = 1
            return (name, qty)

        # "Item Name x 42" or "Item Name x42"
        match = re.match(r'^(.+?)\s+x\s*(\d[\d,]*)\s*$', line, re.IGNORECASE)
        if match:
            return (match.group(1).strip(), int(match.group(2).replace(",", "")))

        # "42x Item Name" or "42 x Item Name"
        match = re.match(r'^(\d[\d,]*)\s*x?\s+(.+)$', line)
        if match:
            return (match.group(2).strip(), int(match.group(1).replace(",", "")))

        # "Item Name 42" (number at end, Multibuy format)
        match = re.match(r'^(.+?)\s+(\d[\d,]*)\s*$', line)
        if match:
            name = match.group(1).strip()
            # Make sure name doesn't end with a roman numeral or common suffix
            if not re.search(r'\s[IVX]+$', name) and not re.search(r'\s\d+mm$', name):
                return (name, int(match.group(2).replace(",", "")))

        # Just an item name
        return (line.strip(), 1)

    async def appraise_fit_cost(self, db: AsyncSession, eft_text: str) -> dict:
        """
        Goonmetrics-style EFT fit cost comparison.
        Returns per-item: Jita, Import, Local with stock status.
        """
        return await self.appraise(db, eft_text)


appraisal_service = AppraisalService()
