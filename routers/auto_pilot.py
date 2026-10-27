"""
OKX-Dog 个人量化智能交易系统 - 全自动量化操盘路由
模块: okx-dog-backend/routers/auto_pilot.py
处理自动操盘启动、暂停/停止、实时收益全景状态查询、收益曲线与流水追踪、策略参数更新及统计重置
"""

import time
from typing import Optional
from fastapi import APIRouter

from models import (
    ApiResponse,
    AutoPilotConfigUpdateRequest,
    AutoPilotHistoryResponse,
    AutoPilotStartRequest,
    AutoPilotStatusModel,
    WSPushPayload,
    WSServerEvent,
)
from state import (
    auto_pilot_manager,
    paper_engine,
    rest_client,
    ws_manager,
)

router = APIRouter(prefix="/api/v1/trade/auto-pilot", tags=["全自动量化操盘与盈利追踪"])


@router.post("/start", response_model=ApiResponse[AutoPilotStatusModel], summary="开启全自动量化操盘")
async def start_auto_pilot(req: Optional[AutoPilotStartRequest] = None):
    """
    开启自动量化操盘：
    以当前账户总权益作为基准初始本金，记录起始时间戳，开启全景收益与胜率跟踪
    """
    now_ms = int(time.time() * 1000)
    is_live = rest_client.is_live_mode and bool(rest_client.api_key)
    if is_live:
        try:
            bal = await rest_client.get_account_balance()
            cur_eq = bal.total_equity_usdt
        except Exception:
            cur_eq = 10000.0
    else:
        bal = paper_engine.get_account_balance()
        cur_eq = bal.total_equity_usdt

    min_conf = req.min_confidence if req and req.min_confidence is not None else 0.70
    auto_lev = req.auto_leverage_enabled if req and req.auto_leverage_enabled is not None else True
    agg_mode = req.aggressiveness_mode if req and req.aggressiveness_mode is not None else "BALANCED"
    sym = req.symbol if req and req.symbol else "BTC-USDT-SWAP"
    symbols = req.symbols if req and req.symbols is not None else None
    focus_symbols = req.focus_symbols if req and req.focus_symbols is not None else None
    focus_mode = req.focus_mode if req and req.focus_mode is not None else None
    max_scale = req.max_scale_layers if req and req.max_scale_layers is not None else None
    scale_strat = req.scale_in_strategy if req and req.scale_in_strategy is not None else None
    auto_rev = req.auto_reversal_enabled if req and req.auto_reversal_enabled is not None else None
    max_pos = req.max_concurrent_positions if req and req.max_concurrent_positions is not None else None
    trailing = req.trailing_stop_enabled if req and req.trailing_stop_enabled is not None else None
    be_ratio = req.breakeven_trigger_ratio if req and req.breakeven_trigger_ratio is not None else None
    tp_thresh = req.trailing_profit_threshold if req and req.trailing_profit_threshold is not None else None
    tp_callback = req.trailing_profit_callback if req and req.trailing_profit_callback is not None else None
    reentry_cd = req.reentry_cooldown_seconds if req and req.reentry_cooldown_seconds is not None else None
    funding_guard = req.funding_fee_guard_enabled if req and req.funding_fee_guard_enabled is not None else None
    floor_eq = req.stop_loss_equity_floor if req and req.stop_loss_equity_floor is not None else None
    max_loss = req.max_loss_limit_usdt if req and req.max_loss_limit_usdt is not None else None

    status = await auto_pilot_manager.start(
        initial_equity=cur_eq,
        is_live_mode=is_live,
        aggressiveness_mode=agg_mode,
        symbol=sym,
        symbols=symbols,
        focus_symbols=focus_symbols,
        focus_mode=focus_mode,
        max_scale_layers=max_scale,
        scale_in_strategy=scale_strat,
        auto_reversal_enabled=auto_rev,
        max_concurrent_positions=max_pos,
        trailing_stop_enabled=trailing,
        breakeven_trigger_ratio=be_ratio,
        trailing_profit_threshold=tp_thresh,
        trailing_profit_callback=tp_callback,
        reentry_cooldown_seconds=reentry_cd,
        funding_fee_guard_enabled=funding_guard,
        min_confidence=min_conf,
        auto_leverage_enabled=auto_lev,
        stop_loss_equity_floor=floor_eq,
        max_loss_limit_usdt=max_loss,
    )

    # 广播最新状态
    await ws_manager.broadcast_to_subscribers(WSPushPayload[AutoPilotStatusModel](
        channel="auto_pilot",
        event=WSServerEvent.UPDATE,
        data=status,
        timestamp=now_ms,
    ))

    return ApiResponse(code=0, msg="【AI全自动操盘】7x24h 后端常驻守护引擎已启动！", data=status, timestamp=now_ms)


@router.post("/stop", response_model=ApiResponse[AutoPilotStatusModel], summary="暂停/停止全自动操盘")
async def stop_auto_pilot():
    """暂停或停止自动操盘"""
    now_ms = int(time.time() * 1000)
    is_live = rest_client.is_live_mode and bool(rest_client.api_key)
    if is_live:
        try:
            bal = await rest_client.get_account_balance()
            cur_eq = bal.total_equity_usdt
        except Exception:
            cur_eq = auto_pilot_manager.get_state(True).current_equity
    else:
        bal = paper_engine.get_account_balance()
        cur_eq = bal.total_equity_usdt

    status = await auto_pilot_manager.stop(current_equity=cur_eq, is_live_mode=is_live)

    await ws_manager.broadcast_to_subscribers(WSPushPayload[AutoPilotStatusModel](
        channel="auto_pilot",
        event=WSServerEvent.UPDATE,
        data=status,
        timestamp=now_ms,
    ))

    return ApiResponse(code=0, msg="【AI全自动操盘】已暂停", data=status, timestamp=now_ms)


@router.get("/status", response_model=ApiResponse[AutoPilotStatusModel], summary="获取自动操盘实时收益全景")
async def get_auto_pilot_status():
    """获取自开启自动操盘以来的实时收益、收益率、已实现盈亏、胜率与运行时长"""
    now_ms = int(time.time() * 1000)
    is_live = rest_client.is_live_mode and bool(rest_client.api_key)
    if is_live:
        try:
            bal = await rest_client.get_account_balance()
            cur_eq = bal.total_equity_usdt
            upl = bal.unrealized_pnl_usdt
        except Exception:
            cur_eq = auto_pilot_manager.get_state(True).current_equity
            upl = 0.0
    else:
        bal = paper_engine.get_account_balance()
        cur_eq = bal.total_equity_usdt
        upl = bal.unrealized_pnl_usdt

    status = auto_pilot_manager.get_status(current_equity=cur_eq, unrealized_pnl=upl, is_live_mode=is_live)
    return ApiResponse(code=0, msg="自动操盘实时收益全景", data=status, timestamp=now_ms)


@router.get("/history", response_model=ApiResponse[AutoPilotHistoryResponse], summary="获取自动操盘收益曲线与流水")
async def get_auto_pilot_history(limit: int = 3000):
    """获取开启以来的时序收益曲线采样点与自动交易流水"""
    now_ms = int(time.time() * 1000)
    is_live = rest_client.is_live_mode and bool(rest_client.api_key)
    history = await auto_pilot_manager.get_history_async(is_live_mode=is_live, limit=limit)
    return ApiResponse(code=0, msg="自动操盘收益曲线与流水", data=history, timestamp=now_ms)


@router.post("/config", response_model=ApiResponse[AutoPilotStatusModel], summary="更新自动操盘策略配置")
async def update_auto_pilot_config(req: AutoPilotConfigUpdateRequest):
    """更新跟单阈值、杠杆设置、专注模式与加仓策略"""
    now_ms = int(time.time() * 1000)
    is_live = rest_client.is_live_mode and bool(rest_client.api_key)
    await auto_pilot_manager.update_config(
        aggressiveness_mode=req.aggressiveness_mode,
        min_confidence=req.min_confidence,
        auto_leverage_enabled=req.auto_leverage_enabled,
        symbol=req.symbol,
        symbols=req.symbols,
        focus_symbols=req.focus_symbols,
        focus_mode=req.focus_mode,
        max_scale_layers=req.max_scale_layers,
        scale_in_strategy=req.scale_in_strategy,
        auto_reversal_enabled=req.auto_reversal_enabled,
        max_concurrent_positions=req.max_concurrent_positions,
        trailing_stop_enabled=req.trailing_stop_enabled,
        breakeven_trigger_ratio=req.breakeven_trigger_ratio,
        trailing_profit_threshold=req.trailing_profit_threshold,
        trailing_profit_callback=req.trailing_profit_callback,
        reentry_cooldown_seconds=req.reentry_cooldown_seconds,
        funding_fee_guard_enabled=req.funding_fee_guard_enabled,
        stop_loss_equity_floor=req.stop_loss_equity_floor,
        max_loss_limit_usdt=req.max_loss_limit_usdt,
        is_live_mode=is_live,
    )
    if is_live:
        try:
            bal = await rest_client.get_account_balance()
            cur_eq = bal.total_equity_usdt
        except Exception:
            cur_eq = auto_pilot_manager.get_state(True).current_equity
    else:
        cur_eq = paper_engine.get_account_balance().total_equity_usdt

    status = auto_pilot_manager.get_status(current_equity=cur_eq, is_live_mode=is_live)
    return ApiResponse(code=0, msg="自动操盘策略配置已更新", data=status, timestamp=now_ms)


@router.post("/reset-stats", response_model=ApiResponse[AutoPilotStatusModel], summary="重置自动操盘收益统计")
async def reset_auto_pilot_stats():
    """重置收益统计基准，以当前资金作为新起点重新累计"""
    now_ms = int(time.time() * 1000)
    is_live = rest_client.is_live_mode and bool(rest_client.api_key)
    if is_live:
        try:
            bal = await rest_client.get_account_balance()
            cur_eq = bal.total_equity_usdt
        except Exception:
            cur_eq = 10000.0
    else:
        bal = paper_engine.get_account_balance()
        cur_eq = bal.total_equity_usdt

    status = await auto_pilot_manager.reset_stats(current_equity=cur_eq, is_live_mode=is_live)
    return ApiResponse(code=0, msg="自动操盘收益统计基准已重置", data=status, timestamp=now_ms)

