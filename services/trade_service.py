"""
OKX-Dog 个人量化智能交易系统 - 交易与仓位核算领域服务
模块: okx-dog-backend/services/trade_service.py
封装 AI 交易计划采纳时的科学仓位核算、2% 风险预算、杠杆与滑点风控校验代理
"""

import math
import logging
from typing import Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession

import models
from models import (
    AdoptAIPlanRequest,
    OrderSide,
    OrderType,
    PlaceOrderRequest,
    PosSide,
)
from state import (
    logger,
    paper_engine,
    rest_client,
    risk_controller,
    ws_manager,
)


async def process_adopt_ai_trade_plan(
    req: AdoptAIPlanRequest
) -> PlaceOrderRequest:
    """
    解析并核算交易员一键采纳的 AI 交易计划：
    1. 映射买卖方向与持仓方向
    2. 严格校验止盈止损方向自洽性
    3. 基于 2% 风险预算与可用保证金自适应推算名义开仓金额与张数
    4. 构造 PlaceOrderRequest
    """
    # 映射买卖方向与持仓方向
    action_str = str(req.action.value if hasattr(req.action, "value") else req.action).upper() if req.action else ""
    is_short = "SELL" in action_str or "SHORT" in action_str or (hasattr(req, "side") and req.side == OrderSide.SELL)
    side = OrderSide.SELL if is_short else OrderSide.BUY
    pos_side = PosSide.SHORT if is_short else PosSide.LONG

    # 估算委托数量与入场参考价
    cur_tk = ws_manager.get_ticker_snapshot(req.symbol)
    ref_price = cur_tk.last_price if cur_tk and cur_tk.last_price > 0 else paper_engine.last_price_map.get(
        req.symbol, 63428.0 if "BTC" in req.symbol else 2680.0 if "ETH" in req.symbol else 188.0 if "SOL" in req.symbol else 0.155
    )
    entry_px = req.selected_entry_price or req.entry_price or ref_price

    # 严格校验止盈止损方向自洽性
    raw_tp = float(req.take_profit_target_1 or 0.0)
    raw_sl = float(req.stop_loss_price or 0.0)
    valid_tp: Optional[float] = None
    valid_sl: Optional[float] = None

    if pos_side == PosSide.LONG:
        if raw_tp > entry_px:
            valid_tp = raw_tp
        if 0 < raw_sl < entry_px:
            valid_sl = raw_sl
    else:
        if 0 < raw_tp < entry_px:
            valid_tp = raw_tp
        if raw_sl > entry_px:
            valid_sl = raw_sl

    # 动态科学仓位推算 (严格基于实盘/模拟当前剩余可用资金与杠杆自适应计算)
    is_live = rest_client.is_live_mode and bool(rest_client.api_key)
    if is_live:
        try:
            live_bal = await rest_client.get_account_balance()
            avail_margin = live_bal.available_margin_usdt if live_bal.available_margin_usdt > 0 else live_bal.total_equity_usdt
            total_equity = live_bal.total_equity_usdt
        except Exception as e:
            logger.warning(f"获取实盘可用资产异常，使用安全兜底: {e}")
            avail_margin = 100.0
            total_equity = 100.0
    else:
        paper_bal = paper_engine.get_account_balance()
        avail_margin = paper_bal.available_margin_usdt if paper_bal.available_margin_usdt > 0 else paper_bal.total_equity_usdt
        total_equity = paper_bal.total_equity_usdt

    max_lev = risk_controller.get_effective_max_leverage() if risk_controller else 20
    lev_to_use = min(req.leverage or req.suggested_leverage or 3, max_lev)

    # 基于当前可用资金允许的最大名义开仓上限 (预留 5% 缓冲防止极端行情保证金不足)
    max_affordable_notional = max(10.0, avail_margin * lev_to_use * 0.95)
    max_cap = min(
        risk_controller.rules.max_order_usdt if (risk_controller and risk_controller.rules) else 50000.0,
        max_affordable_notional
    )

    notional_to_use = req.notional_usdt
    if not notional_to_use or notional_to_use <= 0:
        # 单笔 2% 风险预算 (基于可用资金与总权益中较小者进行保守风险核算)
        risk_budget = max(5.0, min(total_equity, avail_margin) * 0.02)
        stop_dist = abs(entry_px - valid_sl) if valid_sl else entry_px * 0.015
        calc_coins = risk_budget / max(stop_dist, entry_px * 0.005)
        raw_notional = calc_coins * entry_px
        notional_to_use = min(raw_notional, max_cap)
    else:
        # 若传入的名义价值高于当前实盘可用保证金上限或风控上限，自动裁切至安全上限以内
        notional_to_use = min(notional_to_use, max_cap)

    ct_val, _, lot_sz, min_sz = rest_client.get_instrument_spec(req.symbol)
    min_coin_sz = max(1e-6, ct_val * min_sz) if ("SWAP" in req.symbol or "FUTURES" in req.symbol) else max(1e-6, min_sz)

    sz_val = max(min_coin_sz, math.floor((notional_to_use / entry_px) * 100000) / 100000.0) if entry_px > 0 else min_coin_sz
    # 终验：保证 sz_val * entry_px 严格在安全上限以内
    if sz_val * entry_px > max_cap and entry_px > 0:
        sz_val = max(min_coin_sz, math.floor((max_cap / entry_px) * 100000) / 100000.0)

    is_mkt = req.order_type == OrderType.MARKET or (isinstance(req.order_type, str) and req.order_type.lower() == "market")

    return PlaceOrderRequest(
        symbol=req.symbol,
        inst_type=models.InstType.SWAP,
        side=side,
        pos_side=pos_side,
        order_type=OrderType.MARKET if is_mkt else OrderType.LIMIT,
        price=None if is_mkt else (req.selected_entry_price or req.entry_price or ref_price),
        sz=sz_val,
        leverage=lev_to_use,
        tp_trigger_price=valid_tp,
        sl_trigger_price=valid_sl,
        copilot_mode=models.CopilotMode.FULL_AUTO if (getattr(req, "copilot_mode", None) in [models.CopilotMode.FULL_AUTO, "FULL_AUTO"]) else models.CopilotMode.MANUAL,
        ai_analysis_id=str(req.analysis_id or req.ai_analysis_id) if (req.analysis_id or req.ai_analysis_id) else None
    )
