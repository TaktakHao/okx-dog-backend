"""
OKX-Dog 多智能体强化奖励与 AI 员工看板 REST API 路由
模块: okx-dog-backend/routers/evolution.py
角色: 后端架构师 (agency-backend-architect)
"""

import logging
import os
import sys
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

# 导入 okx-dog-ai 模块
ai_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "okx-dog-ai"))
if ai_path not in sys.path:
    sys.path.insert(0, ai_path)

from agent.evolution.evolution_manager import (
    AgentEvolutionManager,
    TeamEvolutionStatusModel,
    EvolutionSnapshotModel,
)

logger = logging.getLogger("okx_dog.backend.routers.evolution")
router = APIRouter(prefix="/api/v1/evolution", tags=["Agent Evolution & Team Hub"])


class ApiResponse(BaseModel):
    code: int = 0
    msg: str = "success"
    data: Optional[Any] = None



@router.get("/team", response_model=ApiResponse, summary="获取决策层 AI 员工团队档案与当前话语权")
async def get_team_status():
    """获取所有 AI 员工角色、等级、话语权占比、战绩与近期沉淀规则"""
    try:
        manager = AgentEvolutionManager.get_instance()
        status = manager.get_team_status()
        return ApiResponse(code=0, msg="获取员工团队数据成功", data=status.dict())
    except Exception as e:
        logger.exception("获取员工团队数据异常: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history", response_model=ApiResponse, summary="获取智能体进化历史版本快照与基准测试结果")
async def get_evolution_history():
    """获取演化版本快照列表"""
    try:
        manager = AgentEvolutionManager.get_instance()
        snaps = manager.get_snapshots_history()
        return ApiResponse(code=0, msg="获取进化快照成功", data=[s.dict() for s in snaps])
    except Exception as e:
        logger.exception("获取进化快照异常: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rollback", response_model=ApiResponse, summary="一键回滚决策权重与经验库至历史稳定黄金快照")
async def rollback_to_baseline():
    """触发一键安全熔断回滚"""
    try:
        manager = AgentEvolutionManager.get_instance()
        status = manager.rollback_to_baseline()
        return ApiResponse(code=0, msg="已成功回滚至出厂黄金基准版本", data=status.dict())
    except Exception as e:
        logger.exception("回滚异常: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reset", response_model=ApiResponse, summary="恢复出厂初始权重与默认决策经验配置")
async def reset_team():
    """恢复出厂默认配置"""
    try:
        manager = AgentEvolutionManager.get_instance()
        status = manager.reset_team()
        return ApiResponse(code=0, msg="已恢复出厂初始设置", data=status.dict())
    except Exception as e:
        logger.exception("恢复出厂设置异常: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


