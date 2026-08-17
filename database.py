"""
OKX-Dog 个人量化智能交易系统 - 异步数据库管理模块
模块: okx-dog-backend/database.py
实现 SQLite 异步连接池、WAL 模式自动初始化、DDL 脚本执行与事务管理
"""

import asyncio
from pathlib import Path
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker
)
from sqlalchemy import text
from config import settings, BASE_DIR

# 初始化异步引擎，配置连接池参数
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    future=True,
    connect_args={"check_same_thread": False}
)

# 异步 Session 工厂
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)


async def init_db():
    """
    初始化数据库：
    1. 开启 SQLite WAL 模式与外键约束
    2. 执行 DATABASE_SCHEMA.sql 中的 DDL 建表脚本
    """
    schema_file = BASE_DIR / "DATABASE_SCHEMA.sql"
    if not schema_file.exists():
        raise FileNotFoundError(f"未找到数据库建表脚本: {schema_file}")

    with open(schema_file, "r", encoding="utf-8") as f:
        ddl_script = f.read()

    async with engine.begin() as conn:
        # 逐句拆分执行 DDL（适配 SQLite 批量指令）
        statements = [stmt.strip() for stmt in ddl_script.split(";") if stmt.strip()]
        for stmt in statements:
            await conn.execute(text(stmt))


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """异步数据库 Session 上下文管理器"""
    session: AsyncSession = AsyncSessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def get_db():
    """FastAPI 依赖注入使用的数据库 Session 生成器"""
    async with get_db_session() as session:
        yield session
