"""
OKX-Dog Paperclip 虚拟量化公司与 CFO 战绩配额 REST API 路由
模块: okx-dog-backend/routers/paperclip.py
"""

import sys
import os
from pathlib import Path
from typing import Any, Dict
from fastapi import APIRouter, HTTPException, Body
from models import ApiResponse

# 确保 okx-dog-ai 模块可被导入
AI_DIR = str(Path(__file__).resolve().parent.parent.parent / "okx-dog-ai")
if AI_DIR not in sys.path:
    sys.path.insert(0, AI_DIR)

router = APIRouter(prefix="/api/v1/paperclip", tags=["Paperclip Virtual Fund"])


@router.get("/org", summary="获取虚拟量化公司组织架构与 6 大员工资料")
async def get_company_org():
    try:
        from agent.paperclip_adapter import paperclip_adapter
        data = paperclip_adapter.get_company_org()
        return ApiResponse(code=0, msg="获取组织架构成功", data=data.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取组织架构失败: {e}")


@router.get("/quota/overview", summary="获取团队及员工剩余可用奖励额度概览")
async def get_company_quota_overview():
    try:
        from agent.evolution.cfo_performance_manager import CfoPerformanceManager
        cfo = CfoPerformanceManager.get_instance()
        data = cfo.get_company_quota_overview()
        return ApiResponse(code=0, msg="获取员工战绩配额成功", data=data.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取战绩配额失败: {e}")


@router.post("/task/anomaly", summary="接收指标哨兵异动事件并触发投决会仲裁工单")
async def trigger_anomaly_task(payload: Dict[str, Any] = Body(...)):
    try:
        from agent.paperclip_adapter import paperclip_adapter, MarketAnomalyTriggerRequest
        req = MarketAnomalyTriggerRequest(**payload)
        res = await paperclip_adapter.handle_anomaly_event(req)
        return ApiResponse(code=0, msg="投决会工单仲裁完成", data=res.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"异动任务触发失败: {e}")
