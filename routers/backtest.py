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
from config import settings
from state import backtest_engine, funding_carry_service, paper_engine, portfolio_allocator, risk_controller

router = APIRouter(prefix="/api/v1/backtest", tags=["Backtest"])


@router.get("/portfolio/slots", response_model=ApiResponse[dict], summary="查询 60/30/10 双引擎资金槽位隔离与风控预算")
async def get_portfolio_slots():
    bal = paper_engine.get_account_balance()
    overview = portfolio_allocator.compute_slot_budgets(
        total_equity_usdt=bal.total_equity_usdt,
        initial_equity_usdt=paper_engine.initial_equity,
        today_realized_loss_usdt=risk_controller.today_realized_loss,
    )
    return ApiResponse(code=0, msg="双引擎资金槽位全景查询成功", data=overview)


@router.get("/carry/scan", response_model=ApiResponse[dict], summary="实时扫描 Top 5 期现资金费率套利机会")
async def scan_carry_opportunities():
    try:
        res = await funding_carry_service.scan_live_opportunities()
        return ApiResponse(code=0, msg="期现资金费率机会扫描完成", data=res)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/carry/mode", response_model=ApiResponse[dict], summary="切换引擎 A 套利执行模式 (SEMI_AUTO / FULL_AUTO)")
async def set_carry_mode(mode: str = Query("SEMI_AUTO", description="SEMI_AUTO 或 FULL_AUTO")):
    try:
        cur = funding_carry_service.set_execution_mode(mode)
        return ApiResponse(code=0, msg=f"引擎 A 执行模式已切换为 {cur}", data={"execution_mode": cur})
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))


@router.post("/carry/confirm/{proposal_id}", response_model=ApiResponse[dict], summary="确认执行半自动期现套利提案")
async def confirm_carry_proposal(proposal_id: str):
    res = await funding_carry_service.confirm_proposal(proposal_id)
    if not res.get("success"):
        raise HTTPException(status_code=400, detail=res.get("reason") or res.get("message") or "提案执行失败")
    return ApiResponse(code=0, msg="半自动套利提案已执行完成", data=res)


@router.post("/gate2b-verify", response_model=ApiResponse[dict], summary="执行底座引擎 A 期现套利 Gate 2B 真实数据验真")
async def run_gate2b_verify(initial_balance: float = Query(3000.0, ge=100.0)):
    try:
        res = await funding_carry_service.run_gate2b_backtest(initial_balance=initial_balance)
        return ApiResponse(code=0, msg="底座引擎 A Gate 2B 验真完成", data=res)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gate2a-verify", response_model=ApiResponse[dict], summary="执行主引擎 B 双周期趋势 Gate 2A 样本外门禁校验")
async def run_gate2a_verify(
    days_back: int = Query(1095, ge=365, le=1825),
    initial_balance: float = Query(6000.0, ge=100.0),
    leverage: int = Query(3, ge=1, le=5),
):
    try:
        res = await backtest_engine.run_dual_tf_trend_gate2a(
            symbols=list(settings.CORE_TRADING_UNIVERSE),
            days_back=days_back,
            initial_balance=initial_balance,
            leverage=leverage,
        )
        return ApiResponse(code=0, msg="主引擎 B 双周期 Gate 2A 验真完成", data=res)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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