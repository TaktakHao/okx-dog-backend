"""
OKX-Dog 个人量化智能交易系统 - 系统健康检查路由
模块: okx-dog-backend/routers/health.py
"""

import time
from typing import Any, Dict
from fastapi import APIRouter

from config import settings
from models import ApiResponse
from state import rest_client, risk_controller, ws_manager

router = APIRouter(tags=["系统与健康检查"])


@router.get("/api/v1/health", response_model=ApiResponse[Dict[str, Any]], summary="系统健康检查")
async def health_check():
    """返回网关健康状态、版本与环境信息"""
    now_ms = int(time.time() * 1000)
    return ApiResponse(
        code=0,
        msg="OKX-Dog 核心服务运行正常",
        data={
            "status": "healthy",
            "app_name": settings.APP_NAME,
            "version": settings.APP_VERSION,
            "env": settings.ENV,
            "ws_public_connected": ws_manager.public_client.is_connected,
            "ws_private_connected": ws_manager.private_client.is_connected,
            "is_live_mode": rest_client.is_live_mode,
            "risk_status": risk_controller.risk_status.value
        },
        timestamp=now_ms
    )
