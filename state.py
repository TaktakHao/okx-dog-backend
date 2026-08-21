"""
OKX-Dog 个人量化智能交易系统 - 全局运行时状态与单例中枢
模块: okx-dog-backend/state.py
集中管理 REST 客户端、WebSocket 长连接管理器、硬风控中枢、安全密盒、模拟撮合引擎与自动操盘管理器
"""

import logging
import sys
from pathlib import Path

# 确保 backend 与 ai 模块路径在 sys.path 中
BACKEND_DIR = Path(__file__).resolve().parent
ROOT_DIR = BACKEND_DIR.parent
AI_DIR = ROOT_DIR / "okx-dog-ai"

if str(BACKEND_DIR) in sys.path:
    sys.path.remove(str(BACKEND_DIR))
sys.path.insert(0, str(BACKEND_DIR))

if str(AI_DIR) not in sys.path:
    sys.path.append(str(AI_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    import okx_dog_ai
except (ImportError, PermissionError, Exception):
    okx_dog_ai = None

from okx_rest import OKXRestClient
from okx_ws import ws_manager, OKXWSManager
from risk_controller import risk_controller, RiskController
from vault import vault, CredentialsVault
from paper_trading import paper_engine, PaperTradingEngine
from auto_pilot import auto_pilot_manager, AutoPilotManager

from services.data_lake_service import DataLakeService
from services.backtest_engine import BacktestEngine
from services.news_service import NewsService
from services.knowledge_base_service import KnowledgeBaseService
from services.daily_review_service import DailyReviewService
from services.consensus_service import ConsensusService
from services.telegram_bot import TelegramBotService

try:
    from okx_dog_ai import antigravity_bridge, AntigravityBridge
except (ImportError, PermissionError, Exception):
    antigravity_bridge = None

# 日志记录器
logger = logging.getLogger("okx_dog.gateway")

# 全局 REST 客户端单例 (默认模拟盘模式)
rest_client = OKXRestClient(is_live_mode=False)

# v3.0 核心服务单例
data_lake_service = DataLakeService(rest_client=rest_client)
backtest_engine = BacktestEngine(data_lake=data_lake_service)
news_service = NewsService()
knowledge_base_service = KnowledgeBaseService()
daily_review_service = DailyReviewService(kb_service=knowledge_base_service)
consensus_service = ConsensusService(kb_service=knowledge_base_service, news_service=news_service)
telegram_bot_service = TelegramBotService()

__all__ = [
    "logger",
    "rest_client",
    "ws_manager",
    "risk_controller",
    "vault",
    "paper_engine",
    "auto_pilot_manager",
    "antigravity_bridge",
    "data_lake_service",
    "backtest_engine",
    "news_service",
    "knowledge_base_service",
    "daily_review_service",
    "consensus_service",
    "telegram_bot_service",
    "BACKEND_DIR",
    "ROOT_DIR",
    "AI_DIR",
]

