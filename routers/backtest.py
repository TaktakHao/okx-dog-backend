"""
OKX-Dog 极速量化回测沙盒 REST API 路由
模块: okx-dog-backend/routers/backtest.py
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text
from database import get_db_session
from models import (
    ApiResponse,
    BacktestResultResponse,
    BacktestRunRequest,
    GridSearchRequest,
    GridSearchResponse
)
from state import backtest_engine

router = APIRouter(prefix="/api/v1/backtest", tags=["Backtest"])

@router.post("/run", response_model=ApiResponse[BacktestResultResponse], summary="执行单次极速向量化回测")
async def run_backtest(req: BacktestRunRequest):
    try:
        res = await backtest_engine.run_backtest(req)
        return ApiResponse(code=0, msg="回测实验执行完成", data=res)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/grid-search", response_model=ApiResponse[GridSearchResponse], summary="执行参数网格搜索与优化")
async def run_grid_search(req: GridSearchRequest):
    try:
        res = await backtest_engine.run_grid_search(req)
        return ApiResponse(code=0, msg="网格搜索优化完成", data=res)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/experiments", response_model=ApiResponse[List[dict]], summary="查询历史回测实验记录")
async def get_experiments(limit: int = Query(20, ge=1, le=100)):
    try:
        async with get_db_session() as db:
            res = await db.execute(text("""
                SELECT id, strategy_name, symbol, timeframe, total_return_pct, 
                       sharpe_ratio, max_drawdown_pct, win_rate, profit_factor, 
                       total_trades, created_at
                FROM backtest_experiments
                ORDER BY created_at DESC LIMIT :limit
            """), {"limit": limit})
            rows = res.mappings().all()
            return ApiResponse(code=0, msg="查询实验记录成功", data=[dict(r) for r in rows])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))