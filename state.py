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

# 动态别名兼容: 将 okx-dog-ai 注册为 okx_dog_ai 模块，解决 Python 标识符连字符问题
import importlib.util
if "okx_dog_ai" not in sys.modules:
    init_file = AI_DIR / "__init__.py"
    if init_file.exists():
        try:
            spec = importlib.util.spec_from_file_location("okx_dog_ai", str(init_file), submodule_search_locations=[str(AI_DIR)])
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                sys.modules["okx_dog_ai"] = mod
                spec.loader.exec_module(mod)
        except Exception as _e:
            pass

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

# 全局 REST 客户端单例 (默认模拟盘模式，优先初始化避免服务引入产生循环依赖)
rest_client = OKXRestClient(is_live_mode=False)

from services.data_lake_service import DataLakeService
from services.backtest_engine import BacktestEngine
from services.news_service import NewsService
from services.knowledge_base_service import KnowledgeBaseService
from services.daily_review_service import DailyReviewService
from services.telegram_bot import TelegramBotService
from services.reconciliation_service import ReconciliationService
from services.execution_engine import SmartExecutionEngine
from services.portfolio_allocator import PortfolioAllocator
from services.funding_carry_service import FundingCarryService
from services.dual_engine_driver import DualEngineShadowDriver

try:
    if okx_dog_ai and hasattr(okx_dog_ai, "antigravity_bridge"):
        antigravity_bridge = okx_dog_ai.antigravity_bridge
        AntigravityBridge = getattr(okx_dog_ai, "AntigravityBridge", None)
    else:
        from antigravity_bridge import antigravity_bridge, AntigravityBridge
except (ImportError, PermissionError, Exception):
    antigravity_bridge = None
    AntigravityBridge = None

# 日志记录器
logger = logging.getLogger("okx_dog.gateway")

# 核心量化与离线投研服务单例
data_lake_service = DataLakeService(rest_client=rest_client)
backtest_engine = BacktestEngine(data_lake=data_lake_service)
news_service = NewsService()
knowledge_base_service = KnowledgeBaseService()
daily_review_service = DailyReviewService(kb_service=knowledge_base_service)
telegram_bot_service = TelegramBotService()
reconciliation_service = ReconciliationService(
    rest_client=rest_client,
    paper_engine=paper_engine,
    auto_pilot_manager=auto_pilot_manager,
    risk_controller=risk_controller
)
smart_execution_engine = SmartExecutionEngine(
    rest_client=rest_client,
    risk_controller=risk_controller
)
portfolio_allocator = PortfolioAllocator(
    trend_weight=0.60,
    carry_weight=0.30,
    buffer_weight=0.10,
    trend_4h_subweight=0.50,
    trend_1d_subweight=0.50,
)
funding_carry_service = FundingCarryService(
    rest_client=rest_client,
    execution_engine=smart_execution_engine,
    data_lake=data_lake_service,
    portfolio_allocator=portfolio_allocator,
)
dual_engine_driver = DualEngineShadowDriver(
    rest_client=rest_client,
    paper_engine=paper_engine,
    risk_controller=risk_controller,
    portfolio_allocator=portfolio_allocator,
    funding_carry_service=funding_carry_service,
)
from services.position_sizer import position_sizer

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
    "telegram_bot_service",
    "reconciliation_service",
    "smart_execution_engine",
    "portfolio_allocator",
    "funding_carry_service",
    "dual_engine_driver",
    "position_sizer",
    "BACKEND_DIR",
    "ROOT_DIR",
    "AI_DIR",
]

