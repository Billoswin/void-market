"""
Void Market — Main Application

EVE Online market intelligence tool for alliance logistics.
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from app.config import settings
from app.models.session import init_db, async_session
from app.models.killmail_session import init_killmail_db
from app.services.esi_auth import esi_auth
from app.services.esi_client import esi_client
from app.services.sde_service import sde_service
from app.services.zkill_service import zkill_service
from app.services.zkill_listener import zkill_listener
from app.services.scheduler import start_scheduler, stop_scheduler
from app.api.routes import router, auth_router, sde_router, doctrine_router
from app.api.data_routes import market_router, contract_router, fight_router, opportunity_router, refresh_router, manual_contract_router, analysis_router, browser_router, esi_market_router, config_router, deployment_router
from app.api.trading_routes import trading_router
from app.api.intelligence_routes import intelligence_router, industry_calc_router
from app.api.settings_routes import settings_router, tools_router
from app.api.skills_routes import skills_router
from app.api.fittings_routes import fittings_router
from app.api.assets_routes import assets_router
from app.api.mining_routes import mining_router
from app.api.debug_routes import debug_router
from app.services.evetycoon import evetycoon
from app.services.goonmetrics import goonmetrics_service

logging.basicConfig(
    level=logging.DEBUG if settings.debug else logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("void_market")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown."""
    logger.info(f"Starting {settings.app_name} v{settings.app_version}")

    # Initialize databases
    await init_db()
    await init_killmail_db()
    logger.info("Databases initialized (market + killmail)")

    # Start ESI auth service
    await esi_auth.start()
    logger.info("ESI auth service started")

    # Start ESI client
    await esi_client.start()
    logger.info("ESI client started")

    # Start zKillboard services
    await zkill_service.start()  # Legacy HTTP client (kept for manual lookups)
    await zkill_listener.start()  # Real-time RedisQ listener
    logger.info("zKillboard services started")

    # Start EVE Tycoon client
    await evetycoon.start()
    logger.info("EVE Tycoon client started")

    # Start Goonmetrics client
    await goonmetrics_service.start()
    logger.info("Goonmetrics client started")

    # Build SDE lookups if data exists
    async with async_session() as db:
        stats = await sde_service.get_sde_stats(db)
        if stats["types"] > 0:
            await sde_service.build_lookups(db)
            logger.info(f"SDE lookups loaded: {len(sde_service.type_name_lookup)} types")
            # Auto-populate packaged volumes for ships
            pv_count = await sde_service.populate_packaged_volumes(db)
            await db.commit()
            if pv_count > 0:
                logger.info(f"Updated {pv_count} ship packaged volumes")
        else:
            logger.warning("No SDE data loaded. Upload SDE files via /api/sde/upload")

    logger.info(f"{settings.app_name} ready on {settings.host}:{settings.port}")

    # Start background scheduler
    start_scheduler()

    yield

    # Shutdown
    stop_scheduler()
    await esi_auth.stop()
    await esi_client.stop()
    await zkill_listener.stop()
    await zkill_service.stop()
    await evetycoon.stop()
    await goonmetrics_service.stop()
    logger.info(f"{settings.app_name} stopped")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
)

# Static files and templates
app.mount("/static", StaticFiles(directory=str(settings.base_dir / "app" / "static")), name="static")
templates = Jinja2Templates(directory=str(settings.base_dir / "templates"))

# API routes
app.include_router(router)
app.include_router(auth_router)
app.include_router(sde_router, prefix="/api")
app.include_router(doctrine_router, prefix="/api")
app.include_router(market_router, prefix="/api")
app.include_router(contract_router, prefix="/api")
app.include_router(fight_router, prefix="/api")
app.include_router(deployment_router, prefix="/api")
app.include_router(opportunity_router, prefix="/api")
app.include_router(refresh_router, prefix="/api")
app.include_router(manual_contract_router, prefix="/api")
app.include_router(analysis_router, prefix="/api")
app.include_router(browser_router, prefix="/api")
app.include_router(esi_market_router, prefix="/api")
app.include_router(config_router, prefix="/api")
app.include_router(trading_router, prefix="/api")
app.include_router(intelligence_router, prefix="/api")
app.include_router(industry_calc_router, prefix="/api")
app.include_router(settings_router, prefix="/api")
app.include_router(tools_router, prefix="/api")
app.include_router(skills_router, prefix="/api")
app.include_router(fittings_router, prefix="/api")
app.include_router(assets_router, prefix="/api")
app.include_router(mining_router, prefix="/api")
app.include_router(debug_router)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Serve the main application page."""
    return templates.TemplateResponse("index.html", {
        "request": request,
        "app_name": settings.app_name,
        "version": settings.app_version,
    })
