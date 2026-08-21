"""
OKX-Dog Telegram 移动端配置与测试 REST API 路由
模块: okx-dog-backend/routers/telegram.py
"""

from fastapi import APIRouter, HTTPException
from models import (
    ApiResponse,
    TelegramConfig,
    TelegramConfigSaveRequest,
    TelegramTestMessageRequest,
)
from state import telegram_bot_service

router = APIRouter(prefix="/api/v1/telegram", tags=["Telegram Bot"])

@router.get("/config", response_model=ApiResponse[TelegramConfig], summary="获取当前 Telegram Bot 配置")
async def get_tg_config():
    try:
        data = telegram_bot_service.get_config()
        return ApiResponse(code=0, msg="获取配置成功", data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/config", response_model=ApiResponse[TelegramConfig], summary="更新并保存 Telegram Bot 配置")
async def save_tg_config(req: TelegramConfigSaveRequest):
    try:
        data = telegram_bot_service.save_config(req)
        return ApiResponse(code=0, msg="保存 Telegram 配置成功", data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/test", response_model=ApiResponse[dict], summary="发送测试通知消息至 Telegram")
async def test_tg_message(req: TelegramTestMessageRequest):
    try:
        success = await telegram_bot_service.send_message(req.message or "🚀 OKX-Dog v3.0 移动端已成功连通！")
        return ApiResponse(code=0, msg="已发送测试请求", data={"sent": success})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))