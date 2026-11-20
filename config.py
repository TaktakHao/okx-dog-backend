"""
OKX-Dog 个人量化智能交易系统 - 全局配置模块
模块: okx-dog-backend/config.py
基于 pydantic-settings 实现环境配置读取与金融级安全校验
"""

import os
from pathlib import Path
from typing import Literal, Tuple
from pydantic import BaseModel, ConfigDict, Field, model_validator

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:
    BaseSettings = BaseModel  # type: ignore

    def SettingsConfigDict(**kwargs):  # type: ignore
        return ConfigDict(extra=kwargs.get("extra", "ignore"))


# 项目根路径与数据持久化目录
BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_DB_PATH = DATA_DIR / "okx_dog.db"


class SystemSettings(BaseSettings):
    """系统全局配置项"""

    # 运行环境与网络配置
    APP_NAME: str = "OKX-Dog Trading Copilot"
    APP_VERSION: str = "1.0.0"
    ENV: Literal["development", "production", "testing"] = "development"
    DEBUG: bool = False
    HOST: str = "127.0.0.1"  # 默认绑定本地回环，保障私有化安全
    PORT: int = 8000

    # 数据库配置 (SQLite + WAL 模式)
    DATABASE_URL: str = f"sqlite+aiosqlite:///{DEFAULT_DB_PATH}"

    # 本地安全密盒主密钥盐与派生参数
    MASTER_PASSWORD: str = Field(
        default="OKX_DOG_DEFAULT_MASTER_PASS_2026",
        description="用于派生 AES-256 密盒密钥的主密码，生产环境务必通过环境变量覆盖"
    )
    PBKDF2_ITERATIONS: int = 100_000

    # OKX API 默认端点
    OKX_LIVE_REST_URL: str = "https://www.okx.com"
    OKX_DEMO_REST_URL: str = "https://www.okx.com"
    OKX_LIVE_WS_PUBLIC: str = "wss://ws.okx.com:8443/ws/v5/public"
    OKX_LIVE_WS_PRIVATE: str = "wss://ws.okx.com:8443/ws/v5/private"
    OKX_DEMO_WS_PUBLIC: str = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
    OKX_DEMO_WS_PRIVATE: str = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"

    # Stage 0 安全底座与高精度影子盘/模拟盘开关
    USE_LIVE_PUBLIC_MARKET_DATA: bool = True         # 模拟盘公共行情统一走 OKX 实盘公共接口，杜绝 demo 脏 tick
    AUTO_BOOT_LIVE_MODE: bool = False                # 禁止服务启动时自动进入实盘模式，防止误触现有手动仓位
    AUTO_TAKEOVER_EXTERNAL_POSITIONS: bool = False   # 禁止对账中枢默认静默接管交易所外部手动持仓
    EVOLUTION_AUTO_MUTATE_ENABLED: bool = False      # 冻结线上自驱进化自动改参与规则写入，杜绝样本内过拟合
    USE_DUAL_ENGINE_AUTOPILOT: bool = True           # 默认由 Gate 2 验真通过的冻结双引擎 (4H凯尔特纳+1D双均线+期现套利) 接管自动操盘

    # 硬风控默认阈值 ($10k 进取趋势型基线: 单日 3% 熔断 + 总回撤 18% 全局停机)
    DEFAULT_MAX_ORDER_USDT: float = 3000.0           # 单笔最大下单金额上限 (3,000 USDT)
    DEFAULT_MAX_DAILY_LOSS_USDT: float = 300.0       # 单日最大亏损熔断阈值 (300 USDT / 3%)
    DEFAULT_MAX_LEVERAGE: int = 5                    # 最大允许杠杆限制 (5x)
    DEFAULT_MAX_SLIPPAGE_PCT: float = 0.5            # 允许最大滑点百分比 (0.5%)
    DEFAULT_EQUITY_FLOOR_RATIO: float = 0.82         # 全局权益保护底线比例 (0.82 => 回撤 18% 强制停机)

    # 模拟盘真实撮合校准参数
    PAPER_DEFAULT_SLIPPAGE_BPS: float = 2.0          # 市价单默认单边滑点 (2 bps = 0.02%)
    PAPER_DEFAULT_FUNDING_RATE_8H: float = 0.0001    # 无实时费率推送时的 8h 默认资金费率 (0.01%)

    # 核心交易标的池 (Top 5 高流动性主流加密永续) 与永久禁开仓合成合约前缀
    CORE_TRADING_UNIVERSE: Tuple[str, ...] = (
        "BTC-USDT-SWAP",
        "ETH-USDT-SWAP",
        "SOL-USDT-SWAP",
        "BNB-USDT-SWAP",
        "DOGE-USDT-SWAP",
    )
    BLOCKED_SYNTHETIC_PREFIXES: Tuple[str, ...] = (
        "AMD-",
        "NVDA-",
        "OPENAI-",
        "TSLA-",
        "AAPL-",
        "MSFT-",
        "GOOG-",
        "AMZN-",
        "META-",
        "SAMSUNG-",
    )


    # 大模型默认配置 (默认使用本地 Antigravity CLI 驱动)
    DEFAULT_LLM_PROVIDER: str = "antigravity"
    DEFAULT_LLM_BASE_URL: str = "http://127.0.0.1:8001"
    DEFAULT_LLM_MODEL: str = "gemini-3.7-flash"
    DEFAULT_LLM_TEMPERATURE: float = 0.2
    DEFAULT_LLM_MAX_TOKENS: int = 4096

    @model_validator(mode="after")
    def validate_production_security(self):
        """生产环境安全硬隔离：禁止使用默认弱口令或空密码启动"""
        if self.ENV == "production":
            if not self.MASTER_PASSWORD or self.MASTER_PASSWORD == "OKX_DOG_DEFAULT_MASTER_PASS_2026":
                raise ValueError(
                    "【安全红线阻断】生产环境 (ENV=production) 禁止使用默认或空 MASTER_PASSWORD！"
                    "请在系统环境变量或 .env 中设置安全主密码后重新启动。"
                )
        return self

    model_config = SettingsConfigDict(
        env_file=[str(ROOT_DIR / ".env"), str(BASE_DIR / ".env")],
        env_file_encoding="utf-8",
        extra="ignore"
    )


# 单例全局配置实例
settings = SystemSettings()
