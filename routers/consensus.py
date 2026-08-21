"""
OKX-Dog 多智能体对抗博弈 REST API 路由
模块: okx-dog-backend/routers/consensus.py
"""

from typing import Any, Dict
from fastapi import APIRouter, HTTPException, Body
from models import ApiResponse, MultiAgentConsensusResponse
from state import consensus_service

router = APIRouter(prefix="/api/v1/consensus", tags=["Consensus Debate"])

@router.post("/analyze", response_model=ApiResponse[MultiAgentConsensusResponse], summary="触发多智能体对抗博弈辩论与仲裁")
async def analyze_consensus(payload: Dict[str, Any] = Body(...)):
    try:
        snapshot = payload.get("snapshot") or payload
        symbol = payload.get("symbol")
        data = await consensus_service.analyze_market_consensus(snapshot=snapshot, symbol=symbol)
        return ApiResponse(code=0, msg="多智能体博弈研判完成", data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))