import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.routers import webhook, admin
from app.config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Heritage Trail Race Engine...")

    from app.database import engine, Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    import asyncio
    from app.services.telegram_service import bot

    polling_task = None
    try:
        if settings.telegram_webhook_url:
            await bot.set_webhook(settings.telegram_webhook_url)
            logger.info(f"Telegram webhook set to {settings.telegram_webhook_url}")
        else:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook cleared — starting polling mode")

            from httpx import AsyncClient, ASGITransport

            async def _poll():
                transport = ASGITransport(app=app)
                async with AsyncClient(transport=transport, base_url="http://localhost") as client:
                    offset = 0
                    while True:
                        try:
                            updates = await bot.get_updates(
                                offset=offset, timeout=30,
                                allowed_updates=["message", "callback_query"],
                            )
                            for update in updates:
                                offset = update.update_id + 1
                                await client.post("/webhook/telegram", json=update.model_dump(exclude_none=True, mode="json"))
                        except asyncio.CancelledError:
                            break
                        except Exception as e:
                            logger.error(f"Polling error: {e}")
                            await asyncio.sleep(3)

            polling_task = asyncio.create_task(_poll())
    except Exception as e:
        logger.warning(f"Could not start bot: {e}")

    yield

    if polling_task:
        polling_task.cancel()

    from app.services.telegram_service import bot
    try:
        await bot.delete_webhook()
    except Exception:
        pass
    await bot.session.close()
    logger.info("Heritage Trail Race Engine stopped.")


app = FastAPI(
    title="Heritage Trail Race Engine",
    description="Amazing Race event platform backend",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(webhook.router)
app.include_router(admin.router)

static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="dashboard")


@app.get("/health")
async def health():
    return {"status": "ok", "service": "heritage_trail_engine"}
