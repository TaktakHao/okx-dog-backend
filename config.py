"""
OKX-Dog 个人量化智能交易系统 - 全局配置模块
模块: okx-dog-backend/config.py
基于 pydantic-settings 实现环境配置读取与金融级安全校验
"""

import os
from pathlib import Path
from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


# 项目根路径与数据持久化目录
BASE_DIR = Path(__file__).resolve().parent
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

    # 硬风控默认阈值
    DEFAULT_MAX_ORDER_USDT: float = 1000.0       # 单笔最大下单金额
    DEFAULT_MAX_DAILY_LOSS_USDT: float = 300.0   # 单日最大亏损熔断阈值
    DEFAULT_MAX_LEVERAGE: int = 5                # 最大杠杆硬限制
    DEFAULT_MAX_SLIPPAGE_PCT: float = 0.5        # 允许最大滑点百分比 (0.5%)

    # 大模型默认配置
    DEFAULT_LLM_BASE_URL: str = "https://api.deepseek.com"
    DEFAULT_LLM_MODEL: str = "deepseek-reasoner"
    DEFAULT_LLM_TEMPERATURE: float = 0.2
    DEFAULT_LLM_MAX_TOKENS: int = 4096

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


# 单例全局配置实例
settings = SystemSettings()
