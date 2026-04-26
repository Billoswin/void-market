"""
Void Market — EFT Fitting Parser

Parses EFT (EVE Fitting Tool) format into structured data.

EFT format example:
    [Muninn, Muninn - DPS]

    Gyrostabilizer II
    Damage Control II

    50MN Microwarpdrive II
    Large Shield Extender II

    720mm Howitzer Artillery II, Republic Fleet Phased Plasma M
    720mm Howitzer Artillery II, Republic Fleet Phased Plasma M

    Medium Core Defense Field Extender II
    Medium Core Defense Field Extender II

    Warrior II x5

    Republic Fleet Phased Plasma M x1000

Slot groups are separated by blank lines, in order:
low slots, mid slots, high slots, rigs, drones, cargo/ammo
"""
import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FitItem:
    name: str
    quantity: int = 1
    charge_name: Optional[str] = None
    slot_type: Optional[str] = None  # high, mid, low, rig, drone, cargo, subsystem
    type_id: Optional[int] = None
    charge_type_id: Optional[int] = None


@dataclass
class ParsedFit:
    ship_type: str
    fit_name: str
    ship_type_id: Optional[int] = None
    items: list[FitItem] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def bill_of_materials(self) -> dict[str, int]:
        """Aggregate all items (including charges) into a {name: quantity} dict."""
        bom: dict[str, int] = {}
        # The hull itself
        bom[self.ship_type] = bom.get(self.ship_type, 0) + 1
        for item in self.items:
            bom[item.name] = bom.get(item.name, 0) + item.quantity
            if item.charge_name:
                bom[item.charge_name] = bom.get(item.charge_name, 0) + item.quantity
        return bom


# Slot order in EFT: low, mid, high, rigs, drones, cargo
# Some fittings also have a subsystem section (T3 cruisers) before the main slots
SLOT_ORDER = ["low", "mid", "high", "rig", "drone", "cargo"]

# T3 cruisers have 4 subsystem slots that appear before the normal slots
T3_CRUISERS = {
    "Tengu", "Legion", "Proteus", "Loki",
}

# Quantity pattern: " x5", " x100", " x 5"
QUANTITY_RE = re.compile(r"\s+x\s*(\d+)\s*$", re.IGNORECASE)

# Header pattern: [Ship Type, Fit Name]
HEADER_RE = re.compile(r"^\[(.+?),\s*(.+?)\]\s*$")

# Offline module indicator
OFFLINE_RE = re.compile(r"\s*/OFFLINE\s*$", re.IGNORECASE)


def parse_eft(eft_text: str) -> ParsedFit:
    """
    Parse an EFT format fitting string into a ParsedFit.

    Args:
        eft_text: Raw EFT text (as copied from PyFa or in-game)

    Returns:
        ParsedFit with ship info and all items
    """
    lines = eft_text.strip().splitlines()
    if not lines:
        return ParsedFit(ship_type="Unknown", fit_name="Unknown",
                         errors=["Empty fitting text"])

    # Parse header
    header_match = HEADER_RE.match(lines[0].strip())
    if not header_match:
        return ParsedFit(ship_type="Unknown", fit_name="Unknown",
                         errors=[f"Invalid header line: {lines[0]}"])

    ship_type = header_match.group(1).strip()
    fit_name = header_match.group(2).strip()
    fit = ParsedFit(ship_type=ship_type, fit_name=fit_name)

    # Determine if T3 (has subsystem section)
    is_t3 = ship_type in T3_CRUISERS
    if is_t3:
        slot_order = ["subsystem"] + SLOT_ORDER
    else:
        slot_order = SLOT_ORDER

    # Split remaining lines into sections by blank lines
    sections: list[list[str]] = []
    current_section: list[str] = []

    for line in lines[1:]:
        stripped = line.strip()
        if stripped == "":
            if current_section:
                sections.append(current_section)
                current_section = []
        elif stripped.startswith("["):
            # Start of a new fit — stop here
            break
        else:
            current_section.append(stripped)

    if current_section:
        sections.append(current_section)

    # Assign slot types based on section index
    for i, section in enumerate(sections):
        if i < len(slot_order):
            slot_type = slot_order[i]
        else:
            slot_type = "cargo"

        for line in section:
            item = _parse_item_line(line, slot_type)
            if item:
                fit.items.append(item)
            else:
                fit.errors.append(f"Could not parse: {line}")

    return fit


def parse_eft_multi(eft_text: str) -> list[ParsedFit]:
    """
    Parse multiple EFT fittings from a single text block.
    Fittings are separated by their [Ship, Name] headers.
    """
    fits = []
    current_block: list[str] = []

    for line in eft_text.strip().splitlines():
        stripped = line.strip()
        if HEADER_RE.match(stripped) and current_block:
            # New fit starting — parse the previous block
            fits.append(parse_eft("\n".join(current_block)))
            current_block = [stripped]
        else:
            current_block.append(line)

    if current_block:
        fits.append(parse_eft("\n".join(current_block)))

    return fits


def _parse_item_line(line: str, slot_type: str) -> Optional[FitItem]:
    """Parse a single item line from an EFT fitting."""
    if not line or line.startswith("["):
        return None

    # Strip [Empty * Slot] lines
    if re.match(r"^\[Empty \w+ slot\]$", line, re.IGNORECASE):
        return None

    # Strip offline indicator
    line = OFFLINE_RE.sub("", line)

    # Check for quantity suffix (drones, cargo, ammo)
    quantity = 1
    qty_match = QUANTITY_RE.search(line)
    if qty_match:
        quantity = int(qty_match.group(1))
        line = line[:qty_match.start()].strip()

    # Check for charge (module, charge format)
    charge_name = None
    if slot_type in ("high", "mid", "low") and ", " in line:
        parts = line.rsplit(", ", 1)
        if len(parts) == 2:
            line = parts[0].strip()
            charge_name = parts[1].strip()

    if not line:
        return None

    return FitItem(
        name=line,
        quantity=quantity,
        charge_name=charge_name,
        slot_type=slot_type,
    )


def resolve_fit_type_ids(fit: ParsedFit, type_lookup: dict[str, int]) -> ParsedFit:
    """
    Resolve item names to type IDs using a name→id lookup dict.
    Modifies the fit in place and returns it.

    Args:
        fit: Parsed fitting
        type_lookup: Dict mapping lowercase item name → type_id
    """
    # Resolve ship
    ship_key = fit.ship_type.lower()
    if ship_key in type_lookup:
        fit.ship_type_id = type_lookup[ship_key]
    else:
        fit.errors.append(f"Unknown ship type: {fit.ship_type}")

    # Resolve items
    for item in fit.items:
        item_key = item.name.lower()
        if item_key in type_lookup:
            item.type_id = type_lookup[item_key]
        else:
            fit.errors.append(f"Unknown item: {item.name}")

        if item.charge_name:
            charge_key = item.charge_name.lower()
            if charge_key in type_lookup:
                item.charge_type_id = type_lookup[charge_key]
            else:
                fit.errors.append(f"Unknown charge: {item.charge_name}")

    return fit
