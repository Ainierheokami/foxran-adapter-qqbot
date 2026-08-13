from fastapi import APIRouter

from .adapter import QQBotAdapter
from .routes import router as webhook_router

router = APIRouter()
router.include_router(webhook_router)


def register_adapter(registry):
    adapter = QQBotAdapter()
    registry.register_adapter("qqbot", adapter)
    registry.register_adapter("qq_openapi", adapter)


async def enable():
    from .gateway import qqbot_gateway
    await qqbot_gateway.start()


async def disable():
    from .gateway import qqbot_gateway
    await qqbot_gateway.stop()


async def shutdown():
    await disable()
