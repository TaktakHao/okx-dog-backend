"""
OKX-Dog 个人量化智能交易系统 - 系统运行配置路由
模块: okx-dog-backend/routers/system_config.py
处理系统协同模式 (MANUAL / SEMI_AUTO / FULL_AUTO)、活跃交易标的与硬风控全局参数查询与更新
"""

import time
from typing import Any, Dict
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import get_db
from models import ApiResponse, UpdateSystemConfigRequest
from state import risk_controller

router = APIRouter(prefix="/api/v1/config", tags=["系统运行配置"])


@router.get("/system", response_model=ApiResponse[Dict[str, Any]], summary="获取系统综合运行配置")
async def get_system_config_endpoint(db: AsyncSession = Depends(get_db)):
    """获取当前系统运行配置与硬风控规则"""
    now_ms = int(time.time() * 1000)
    result = await db.execute(text("SELECT * FROM system_config ORDER BY id DESC LIMIT 1"))
    row = result.mappings().first()

    if not row:
        return ApiResponse(
            code=0,
            msg="success",
            data={
                "copilot_mode": "MANUAL",
                "active_symbol": "BTC-USDT-SWAP",
                "supported_timeframes": ["15m", "1h", "4h", "1d"],
                "llm_provider": "deepseek",
                "llm_base_url": settings.DEFAULT_LLM_BASE_URL,
                "llm_model": settings.DEFAULT_LLM_MODEL,
                "risk_rules": risk_controller.rules.model_dump()
            },
            timestamp=now_ms
        )

    row_dict = dict(row)
    # 移除敏感字段
    row_dict.pop("llm_api_key_encrypted", None)
    row_dict["risk_rules"] = risk_controller.rules.model_dump()

    return ApiResponse(code=0, msg="success", data=row_dict, timestamp=now_ms)


@router.post("/system", response_model=ApiResponse[Dict[str, Any]], summary="更新系统运行配置")
async def update_system_config(
    req: UpdateSystemConfigRequest,
    db: AsyncSession = Depends(get_db)
):
    """更新协同模式 (MANUAL / SEMI_AUTO / FULL_AUTO) 与活跃标的"""
    now_ms = int(time.time() * 1000)

    sql = text("""
        UPDATE system_config SET
            copilot_mode = COALESCE(:mode, copilot_mode),
            active_symbol = COALESCE(:symbol, active_symbol),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = (SELECT id FROM system_config ORDER BY id DESC LIMIT 1)
    """)
    await db.execute(sql, {
        "mode": req.copilot_mode.value if req.copilot_mode else None,
        "symbol": req.active_symbol
    })
    await db.commit()

    return ApiResponse(
        code=0,
        msg="系统运行配置已更新",
        data={
            "copilot_mode": req.copilot_mode.value if req.copilot_mode else "MANUAL",
            "active_symbol": req.active_symbol or "BTC-USDT-SWAP"
        },
        timestamp=now_ms
    )
