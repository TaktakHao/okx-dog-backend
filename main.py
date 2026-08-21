"""
OKX-Dog 个人量化智能交易系统 - FastAPI 主服务入口与网关中枢
模块: okx-dog-backend/main.py
职责: 系统生命周期装配、CORS 与全局切面中间件、异常处理器注册与模块化路由分发
"""

import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from config import settings
from core.background_tasks import (
    auto_pilot_snapshot_loop,
    auto_pilot_worker_loop,
    daily_review_and_macro_watch_loop,
)
from core.exceptions import register_exception_handlers
from database import get_db_session, init_db
from routers import (
    ai_router,
    antigravity_router,
    auto_pilot_router,
    backtest_router,
    consensus_router,
    data_lake_router,
    health_router,
    knowledge_base_router,
    market_router,
    news_router,
    paper_router,
    review_router,
    risk_router,
    system_config_router,
    telegram_router,
    trade_router,
    vault_router,
    websocket_router,
    studio_router,
)
from vault import install_sensitive_data_filter
from state import (
    antigravity_bridge,
    auto_pilot_manager,
    logger,
    paper_engine,
    rest_client,
    risk_controller,
    vault,
    ws_manager,
)

# 尽早安装敏感日志脱敏过滤器
install_sensitive_data_filter()


# =============================================================================
# 1. 应用生命周期管理 (FastAPI Lifespan)
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    应用生命周期管理：
    1. 初始化 SQLite 数据库与 WAL 模式
    2. 加载硬风控规则与 AES-256-GCM 凭证密盒
    3. 启动 OKX WebSocket 长连接管理器与订阅自愈
    4. 启动自动操盘时序采样与平仓监听后台协程
    5. 优雅关闭与断开连接池
    """
    logger.info("正在启动 OKX-Dog 个人量化智能交易系统核心网关...")
    snapshot_task = None
    try:
        await init_db()
        logger.info("SQLite 数据库连接池与 WAL 模式初始化成功")

        # 加载风控规则
        await risk_controller.load_config_from_db()

        # 尝试从凭证密盒加载已保存的 OKX 凭证
        async with get_db_session() as db:
            res = await db.execute(
                text("SELECT * FROM credentials_vault WHERE credential_type = 'OKX_V5' AND is_valid = 1 LIMIT 1")
            )
            cred = res.mappings().first()
            if cred:
                try:
                    dec_key, dec_sec, dec_pass = vault.decrypt_credentials(
                        cred["encrypted_api_key"],
                        cred["encrypted_secret_key"],
                        cred["encrypted_passphrase"],
                        cred["salt"],
                        cred["nonce"]
                    )
                    is_live = bool(cred["is_live_mode"])
                    rest_client.update_credentials(dec_key, dec_sec, dec_pass, is_live)
                    ws_manager.update_credentials(dec_key, dec_sec, dec_pass)
                    logger.info(f"成功从本地密盒恢复 OKX 凭证 (实盘={is_live}, 脱敏Key={cred['api_key_masked']})")
                except Exception as dec_err:
                    logger.warning(f"自动解密本地凭证失败 (可能使用了自定义主密码，切换实盘时请输入主密码): {dec_err}")

        # 启动 WebSocket 管理器
        await ws_manager.start()

        # 注册默认标的行情与私有订阅
        await ws_manager.subscribe_market_symbol("BTC-USDT-SWAP")
        await ws_manager.subscribe_private_channels("SWAP")

        # 注册模拟操盘平仓事件监听至自动操盘中枢
        def on_paper_trade_closed(trade_record: dict):
            asyncio.create_task(
                auto_pilot_manager.record_trade(
                    symbol=trade_record["symbol"],
                    pos_side=trade_record["pos_side"],
                    entry_price=trade_record["entry_price"],
                    exit_price=trade_record["exit_price"],
                    size=trade_record["size"],
                    leverage=trade_record["leverage"],
                    realized_pnl=trade_record["realized_pnl"],
                    roi_pct=trade_record["roi_pct"],
                    reason=trade_record.get("reason", "模拟策略自动平仓"),
                )
            )

        paper_engine.register_trade_closed_callback(on_paper_trade_closed)

        # 启动自动操盘收益采样时序后台任务 (每 5 秒记录一次快照)
        snapshot_task = asyncio.create_task(auto_pilot_snapshot_loop())
        # 启动 7x24h 脱机多标的量化巡检与移动保本守护协程
        worker_task = asyncio.create_task(auto_pilot_worker_loop())
        # 启动每日盘后自动复盘与宏观事件监控协程
        review_task = asyncio.create_task(daily_review_and_macro_watch_loop())

    except Exception as e:
        logger.critical(f"系统启动初始化失败: {e}", exc_info=True)
        sys.exit(1)

    yield

    logger.info("正在优雅关闭 OKX-Dog 核心服务，释放连接池资源...")
    if snapshot_task and not snapshot_task.done():
        snapshot_task.cancel()
    if worker_task and not worker_task.done():
        worker_task.cancel()
    if review_task and not review_task.done():
        review_task.cancel()
    await ws_manager.stop()
    await rest_client.close()
    logger.info("OKX-Dog 核心服务已安全停止")


# =============================================================================
# 2. FastAPI 应用实例化与中间件配置
# =============================================================================

app = FastAPI(
    title=settings.APP_NAME,
    description="金融级个人量化分析与智能决策系统后端网关",
    version=settings.APP_VERSION,
    lifespan=lifespan
)

# 允许跨域请求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册全局异常处理器
register_exception_handlers(app)


# =============================================================================
# 3. 模块化领域路由挂载 (Router Inclusions)
# =============================================================================

app.include_router(health_router)
app.include_router(vault_router)
app.include_router(market_router)
app.include_router(ai_router)
app.include_router(trade_router)
app.include_router(auto_pilot_router)
app.include_router(paper_router)
app.include_router(risk_router)
app.include_router(system_config_router)
app.include_router(antigravity_router)
app.include_router(websocket_router)

# v3.0 核心新增领域路由
app.include_router(data_lake_router)
app.include_router(backtest_router)
app.include_router(news_router)
app.include_router(review_router)
app.include_router(knowledge_base_router)
app.include_router(consensus_router)
app.include_router(telegram_router)

# v3.5 AI Quant Studio & Codex Harness 策略实验室路由
app.include_router(studio_router)



# =============================================================================
# 4. 服务启动入口
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )
