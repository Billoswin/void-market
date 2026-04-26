"""
Void Market — Forum Doctrine Post Parser

Parses pasted forum doctrine posts (like Goonswarm's forum format) into
structured doctrine data with EFT fittings, roles, and fleet composition.

The challenge: forum copy-paste collapses newlines into spaces, so
"Damage Control II\n1600mm Steel Plates II" becomes
"Damage Control II 1600mm Steel Plates II"

We solve this by using the SDE as a dictionary to do greedy forward matching
against known item names, reconstructing the original EFT format.

Also extracts:
- Fleet composition: "In a 250 man fleet we want about 170 of these"
- Roles: "Ship of the line", "Logi", "Ewar support", "Boosters"
- Doctrine title from the header
"""
import re
import logging
from dataclasses import dataclass, field

logger = logging.getLogger("void_market.forum_parser")

# Pattern to match EFT headers: [Ship, Fit Name]
EFT_HEADER_RE = re.compile(r'\[([^,\]]+),\s*([^\]]+)\]')

# Fleet composition pattern: "want about N" or "want about N-M"
FLEET_COMP_RE = re.compile(
    r'(?:want\s+about|we\s+want)\s+(?:about\s+)?(\d+)(?:\s*-\s*(\d+))?',
    re.IGNORECASE
)

# Role keywords that appear before fleet comp notes
ROLE_PATTERNS = [
    (re.compile(r'ship\s+of\s+the\s+line', re.I), 'dps'),
    (re.compile(r'\blogi\b', re.I), 'logi'),
    (re.compile(r'\bewar\s+support\b', re.I), 'ewar'),
    (re.compile(r'\btackle\b', re.I), 'tackle'),
    (re.compile(r'\bbooster', re.I), 'booster'),
    (re.compile(r'\bsupport\b', re.I), 'support'),
    (re.compile(r'\bDPS\b'), 'dps'),
    (re.compile(r'\bLOGI\b'), 'logi'),
    (re.compile(r'\bSUPP\b'), 'support'),
    (re.compile(r'\bBOOST\b'), 'booster'),
]

# Noise words to strip
NOISE = {'Spoiler', 'Hide', 'Show'}

# Quantity suffix
QTY_RE = re.compile(r'\s+x(\d+)$')

# Charge separator in module lines: "Module Name, Charge Name"
CHARGE_RE = re.compile(r'^(.+?),\s+(.+)$')

# Empty slot
EMPTY_SLOT_RE = re.compile(r'^\[Empty \w+ slot\]$', re.I)


@dataclass
class ParsedDoctrineShip:
    """A single ship fit extracted from a forum post."""
    ship_type: str
    fit_name: str
    eft_text: str  # Reconstructed EFT format
    role: str = ""
    fleet_count: int = 0  # Desired number in fleet
    fleet_count_max: int = 0  # Upper bound if range given
    section_label: str = ""  # "Ship of the line", "Logi", etc.


@dataclass
class ParsedDoctrine:
    """Full doctrine extracted from a forum post."""
    title: str = ""
    fleet_size: int = 250
    ships: list[ParsedDoctrineShip] = field(default_factory=list)
    parse_errors: list[str] = field(default_factory=list)


def parse_forum_post(raw_text: str, sde_names: dict[str, int] | None = None) -> ParsedDoctrine:
    """
    Parse a forum doctrine post into structured data.

    Args:
        raw_text: Raw copy-pasted forum post text
        sde_names: Dict of lowercase item name → type_id for item matching

    Returns:
        ParsedDoctrine with extracted ships and metadata
    """
    doctrine = ParsedDoctrine()

    # Clean up the text
    text = raw_text.strip()

    # Try to extract doctrine title from first meaningful line
    lines = text.split('\n')
    for line in lines[:5]:
        line = line.strip()
        if line and len(line) < 50 and not line.startswith('[') and line not in NOISE:
            doctrine.title = line
            break

    # Extract fleet size if mentioned
    fleet_match = re.search(r'(\d+)\s*man\s*fleet', text, re.I)
    if fleet_match:
        doctrine.fleet_size = int(fleet_match.group(1))

    # Find all EFT headers and their positions
    headers = list(EFT_HEADER_RE.finditer(text))
    if not headers:
        doctrine.parse_errors.append("No EFT fitting headers found (expected [Ship, Name] format)")
        return doctrine

    logger.info(f"Found {len(headers)} EFT headers in forum post")

    for i, header_match in enumerate(headers):
        ship_type = header_match.group(1).strip()
        fit_name = header_match.group(2).strip()

        # Get the text between this header and the next header (or end)
        start = header_match.end()
        if i + 1 < len(headers):
            end = headers[i + 1].start()
        else:
            end = len(text)

        fit_blob = text[start:end]

        # Look backwards from the header for role/fleet comp info
        lookback_start = headers[i - 1].end() if i > 0 else 0
        lookback_text = text[lookback_start:header_match.start()]

        role = _extract_role(lookback_text, fit_name)
        fleet_count, fleet_count_max = _extract_fleet_count(lookback_text)
        section_label = _extract_section_label(lookback_text)

        # Parse the fit blob into EFT format
        if sde_names:
            eft_text = _reconstruct_eft(ship_type, fit_name, fit_blob, sde_names)
        else:
            # Without SDE, just do basic cleanup
            eft_text = _basic_cleanup(ship_type, fit_name, fit_blob)

        ship = ParsedDoctrineShip(
            ship_type=ship_type,
            fit_name=fit_name,
            eft_text=eft_text,
            role=role,
            fleet_count=fleet_count,
            fleet_count_max=fleet_count_max,
            section_label=section_label,
        )
        doctrine.ships.append(ship)

    return doctrine


def _extract_role(text: str, fit_name: str) -> str:
    """Extract role from surrounding text or fit name."""
    # Check fit name first (e.g., "GSF - Typhoon - DPS - v26.1")
    for pattern, role in ROLE_PATTERNS:
        if pattern.search(fit_name):
            return role

    # Check surrounding text
    for pattern, role in ROLE_PATTERNS:
        if pattern.search(text):
            return role

    return ""


def _extract_fleet_count(text: str) -> tuple[int, int]:
    """Extract desired fleet count from text like 'want about 170'."""
    match = FLEET_COMP_RE.search(text)
    if match:
        count = int(match.group(1))
        count_max = int(match.group(2)) if match.group(2) else count
        return count, count_max
    return 0, 0


def _extract_section_label(text: str) -> str:
    """Extract section label like 'Ship of the line', 'Logi', etc."""
    for pattern, label in ROLE_PATTERNS:
        if pattern.search(text):
            return label
    return ""


def _reconstruct_eft(ship_type: str, fit_name: str, blob: str, sde_names: dict[str, int]) -> str:
    """
    Reconstruct proper EFT format from a collapsed text blob using SDE matching.

    Strategy:
    1. Split on triple+ spaces (these are slot group separators)
    2. Within each group, use greedy SDE matching to find item boundaries
    3. Handle quantity suffixes (x5) and charges (Module, Charge)
    """
    # Build header
    eft_lines = [f"[{ship_type}, {fit_name}]"]

    # Clean noise
    blob = _strip_noise(blob)

    # Split into sections by triple+ space (EFT slot group separator)
    # Also split on "   " which is how forums represent blank lines
    sections = re.split(r'\s{3,}', blob)

    for section in sections:
        section = section.strip()
        if not section:
            continue

        items = _match_items_greedy(section, sde_names)

        if items:
            eft_lines.append("")  # Blank line = section separator
            for item_name, qty in items:
                if qty > 1:
                    eft_lines.append(f"{item_name} x{qty}")
                else:
                    eft_lines.append(item_name)

    return "\n".join(eft_lines)


def _match_items_greedy(text: str, sde_names: dict[str, int]) -> list[tuple[str, int]]:
    """
    Greedy forward match against SDE item names.

    At each position, try to match the longest possible item name.
    Handle:
    - Charge format: "Module Name, Charge Name" (keep as single line)
    - Quantity suffix: "Item Name x5"
    - Empty slot markers
    """
    items = []
    words = text.split()
    i = 0

    while i < len(words):
        # Skip noise words
        if words[i] in NOISE:
            i += 1
            continue

        # Check for [Empty X slot]
        if words[i] == '[Empty':
            # Skip to closing ]
            while i < len(words) and not words[i].endswith(']'):
                i += 1
            i += 1
            continue

        best_match = None
        best_len = 0
        best_qty = 1
        best_charge = None

        # Try progressively longer combinations of words
        max_words = min(10, len(words) - i)  # Item names rarely exceed 10 words

        for length in range(1, max_words + 1):
            candidate_words = words[i:i + length]
            candidate = " ".join(candidate_words)

            # Check for quantity suffix on last word
            qty = 1
            qty_match = QTY_RE.search(candidate)
            if qty_match:
                qty = int(qty_match.group(1))
                candidate = candidate[:qty_match.start()].strip()

            # Check for charge (comma-separated)
            charge = None
            if ", " in candidate:
                parts = candidate.split(", ", 1)
                module_part = parts[0].strip()
                charge_part = parts[1].strip()
                if module_part.lower() in sde_names and charge_part.lower() in sde_names:
                    # Both parts are valid items — it's a module + charge
                    if length > best_len:
                        best_match = f"{module_part}, {charge_part}"
                        best_len = length
                        best_qty = qty
                    continue

            # Direct match
            if candidate.lower() in sde_names:
                if length > best_len:
                    best_match = candidate
                    best_len = length
                    best_qty = qty

        if best_match:
            items.append((best_match, best_qty))
            i += best_len
        else:
            # No match — skip this word
            i += 1

    return items


def _strip_noise(text: str) -> str:
    """Remove forum formatting noise."""
    # Remove "Spoiler", "Hide", "Show" markers
    for word in NOISE:
        text = text.replace(word, ' ')

    # Remove "Edited by..." at the end
    text = re.sub(r'Edited by.*$', '', text, flags=re.I)

    # Collapse multiple spaces (but preserve triple+ for section detection)
    # First mark triple+ spaces
    text = re.sub(r' {4,}', '   ', text)

    return text.strip()


def _basic_cleanup(ship_type: str, fit_name: str, blob: str) -> str:
    """Basic cleanup without SDE — just remove noise and format."""
    blob = _strip_noise(blob)
    lines = [f"[{ship_type}, {fit_name}]"]

    # Split on triple spaces for sections
    sections = re.split(r'\s{3,}', blob)
    for section in sections:
        section = section.strip()
        if section:
            lines.append("")
            lines.append(section)

    return "\n".join(lines)
