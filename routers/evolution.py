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


class SimulateReinforcementReq(BaseModel):
    symbol: str = Field(default="BTC-USDT-SWAP")
    pos_side: str = Field(default="LONG")
    entry_price: float = Field(default=96000.0)
    exit_price: float = Field(default=97500.0)
    sl_price: float = Field(default=95200.0)
    realized_pnl: float = Field(default=150.0)
    mfe_pct: float = Field(default=0.018)
    mae_pct: float = Field(default=0.003)
    is_crisis_defended: bool = Field(default=False)


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


@router.post("/simulate-reinforcement", response_model=ApiResponse, summary="模拟单笔平仓触发强化奖励与进化 (用于演示联调)")
async def simulate_reinforcement(req: SimulateReinforcementReq):
    """模拟一笔交易结算并触发动态进化"""
    try:
        manager = AgentEvolutionManager.get_instance()
        mock_opinions = {
            "bull_specialist": {"role_name": "多头辩护专家", "stance": "BULLISH", "confidence": 0.82},
            "bear_critic": {"role_name": "空头风控专家", "stance": "BEARISH", "confidence": 0.45},
            "macro_news": {"role_name": "宏观舆情专家", "stance": "BULLISH", "confidence": 0.70},
            "micro_sniper": {"role_name": "微观盘口分析师", "stance": "BULLISH", "confidence": 0.78},
            "chief_arbiter": {"role_name": "首席量化仲裁官", "stance": "BULLISH", "confidence": 0.85},
        }
        outcome = manager.process_trade_reinforcement(
            trade_id=f"sim_{int(req.entry_price)}",
            symbol=req.symbol,
            pos_side=req.pos_side,
            entry_price=req.entry_price,
            exit_price=req.exit_price,
            sl_price=req.sl_price,
            realized_pnl=req.realized_pnl,
            mfe_pct=req.mfe_pct,
            mae_pct=req.mae_pct,
            agent_opinions=mock_opinions,
            is_crisis_defended=req.is_crisis_defended
        )
        return ApiResponse(code=0, msg="演进结算完成", data=outcome.dict())
    except Exception as e:
        logger.exception("模拟进化异常: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
