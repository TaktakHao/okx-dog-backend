"""
OKX-Dog 模块化路由注册中心
统一导出所有领域 APIRouter 实例供 main.py 一键挂载
"""

from routers.health import router as health_router
from routers.vault import router as vault_router
from routers.market import router as market_router
from routers.ai import router as ai_router
from routers.trade import router as trade_router
from routers.auto_pilot import router as auto_pilot_router
from routers.paper import router as paper_router
from routers.risk import router as risk_router
from routers.system_config import router as system_config_router
from routers.antigravity import router as antigravity_router
from routers.websocket import router as websocket_router

from routers.data_lake import router as data_lake_router
from routers.backtest import router as backtest_router
from routers.news import router as news_router
from routers.review import router as review_router
from routers.knowledge_base import router as knowledge_base_router
from routers.consensus import router as consensus_router
from routers.telegram import router as telegram_router
from routers.studio import router as studio_router
from routers.evolution import router as evolution_router
from routers.finance import router as finance_router
from routers.paperclip import router as paperclip_router

__all__ = [
    "health_router",
    "vault_router",
    "market_router",
    "ai_router",
    "trade_router",
    "auto_pilot_router",
    "paper_router",
    "risk_router",
    "system_config_router",
    "antigravity_router",
    "websocket_router",
    "data_lake_router",
    "backtest_router",
    "news_router",
    "review_router",
    "knowledge_base_router",
    "consensus_router",
    "telegram_router",
    "studio_router",
    "evolution_router",
    "finance_router",
    "paperclip_router",
]



