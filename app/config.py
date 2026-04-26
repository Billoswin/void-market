"""
Void Market — Configuration
"""
from pydantic_settings import BaseSettings
from pathlib import Path
import secrets


class Settings(BaseSettings):
    # App
    app_name: str = "Void Market"
    app_version: str = "0.1.0"
    debug: bool = False
    secret_key: str = secrets.token_urlsafe(32)

    # Paths
    base_dir: Path = Path(__file__).resolve().parent.parent
    data_dir: Path = base_dir / "data"
    sde_dir: Path = data_dir / "sde"
    db_path: Path = data_dir / "void_market.db"

    # EVE SSO / ESI
    esi_client_id: str = ""
    esi_client_secret: str = ""
    esi_callback_url: str = "http://localhost:8585/auth/callback"
    esi_base_url: str = "https://esi.evetech.net/latest"
    esi_login_url: str = "https://login.eveonline.com"
    esi_scopes: str = " ".join([
        "esi-markets.structure_markets.v1",
        "esi-contracts.read_character_contracts.v1",
        "esi-markets.read_character_orders.v1",
        "esi-wallet.read_character_wallet.v1",
        "esi-industry.read_character_jobs.v1",
        "esi-assets.read_corporation_assets.v1",
        "esi-killmails.read_killmails.v1",
        "esi-universe.read_structures.v1",
        # Added for skills, fittings, assets, mining
        "esi-skills.read_skills.v1",
        "esi-fittings.read_fittings.v1",
        "esi-assets.read_assets.v1",
        "esi-industry.read_character_mining.v1",
    ])

    # Structure IDs (configured after first auth)
    keepstar_structure_id: int = 0

    # zKillboard
    zkill_base_url: str = "https://zkillboard.com/api"
    alliance_id: int = 0  # Set to your alliance ID

    # Market analysis
    market_refresh_minutes: int = 30
    contract_refresh_minutes: int = 60
    zkill_refresh_minutes: int = 120
    jita_region_id: int = 10000002  # The Forge
    jita_station_id: int = 60003760  # Jita 4-4

    # Fight filter
    min_fight_losses: int = 30  # Minimum alliance losses to count as a major fight
    fight_window_minutes: int = 30  # Rolling window to group kills into fights

    # Freight / Import
    freight_cost_per_m3: float = 1350.0  # ISK per m3 for JF freight (adjust to your service)
    default_markup_pct: float = 20.0  # Target markup percentage

    # Server
    host: str = "0.0.0.0"
    port: int = 8585

    class Config:
        env_file = ".env"
        env_prefix = "VM_"


settings = Settings()

# Ensure directories exist
settings.data_dir.mkdir(parents=True, exist_ok=True)
settings.sde_dir.mkdir(parents=True, exist_ok=True)
