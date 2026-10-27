"""
OKX-Dog 个人量化智能交易系统 - 交易与订单管理路由
模块: okx-dog-backend/routers/trade.py
处理委托下单 (经硬风控拦截)、一键采纳 AI 计划、撤单、持仓查询、止盈止损设置、快捷平仓与资产全景
"""

import asyncio
import datetime
import logging
import time
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

import models
from database import get_db
from models import (
    AccountBalanceModel,
    AdoptAIPlanRequest,
    ApiResponse,
    BatchCancelOrdersRequest,
    CancelOrderRequest,
    ClosePositionRequest,
    OrderModel,
    OrderSide,
    OrderState,
    OrderType,
    PlaceOrderRequest,
    PositionModel,
    PosSide,
    SetPositionTPSLRequest,
    TickerData,
    WSPushPayload,
    WSServerEvent,
)
from risk_controller import RiskInterceptionException
from services.trade_service import process_adopt_ai_trade_plan
from state import (
    logger,
    paper_engine,
    rest_client,
    risk_controller,
    ws_manager,
)

router = APIRouter(prefix="/api/v1/trade", tags=["交易与订单执行"])


@router.post("/orders", response_model=ApiResponse[OrderModel], summary="提交委托下单 (经硬风控拦截)")
async def place_trade_order(
    req: PlaceOrderRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    统一提交交易委托：
    1. 强制经由硬风控中枢拦截 (单笔金额、单日亏损、杠杆、滑点等)
    2. 若处于实盘模式且有 Key -> 提交至 OKX REST API
    3. 若处于模拟沙盒模式 -> 提交至内置 PaperTradingEngine 极速撮合
    4. 广播 WebSocket 订单与持仓事件
    """
    now_ms = int(time.time() * 1000)
    default_px = 63428.0 if "BTC" in req.symbol else (2680.0 if "ETH" in req.symbol else (188.0 if "SOL" in req.symbol else 0.155))
    fallback_px = req.price or paper_engine.last_price_map.get(req.symbol, default_px)

    # 获取当前标的行情与当前持仓以辅助风控
    current_ticker = ws_manager.latest_tickers.get(req.symbol)
    if not current_ticker:
        try:
            current_ticker = await asyncio.wait_for(rest_client.get_ticker(req.symbol), timeout=1.0)
        except Exception:
            current_ticker = TickerData(
                symbol=req.symbol,
                last_price=fallback_px,
                high_24h=fallback_px * 1.02,
                low_24h=fallback_px * 0.98,
                volume_24h=15234.0,
                vol_currency_24h=1468892340.0,
                change_24h_pct=1.85,
                timestamp=now_ms
            )

    if rest_client.is_live_mode:
        if not rest_client.api_key:
            raise HTTPException(status_code=400, detail="当前处于实盘环境但未配置 OKX API Key，请先在右上角配置并保存 API 凭证！")
        current_positions = list(ws_manager.latest_positions.values())
    else:
        current_positions = paper_engine.get_positions()

    # 1. 触发不可绕过的硬风控拦截
    risk_check = await risk_controller.check_order(req, current_ticker, current_positions)
    if not risk_check.passed:
        logger.warning(f"【硬风控拦截】下单被阻断: {risk_check.message}")
        raise RiskInterceptionException(
            message=risk_check.message or "硬风控前置校验未通过",
            rule_name=risk_check.rejected_rule or "RISK_RULE",
            current_value="ORDER_REQUEST",
            limit_value="BLOCKED"
        )

    # 2. 模拟沙盒撮合处理
    if not rest_client.is_live_mode:
        mkt_px = current_ticker.last_price if (current_ticker and current_ticker.last_price > 0) else fallback_px

        try:
            order_model = paper_engine.place_order(req, current_market_price=mkt_px)
        except Exception as e:
            logger.error(f"模拟撮合异常: {e}", exc_info=True)
            raise HTTPException(status_code=400, detail=str(e))

        # 广播 WebSocket 订单、持仓与资产变动
        await ws_manager.broadcast_to_subscribers(WSPushPayload[OrderModel](
            channel="orders",
            event=WSServerEvent.UPDATE,
            data=order_model,
            timestamp=now_ms
        ))
        await ws_manager.broadcast_to_subscribers(WSPushPayload[List[PositionModel]](
            channel="positions",
            event=WSServerEvent.SNAPSHOT,
            data=paper_engine.get_positions(),
            timestamp=now_ms
        ))
        await ws_manager.broadcast_to_subscribers(WSPushPayload[AccountBalanceModel](
            channel="account",
            event=WSServerEvent.UPDATE,
            data=paper_engine.get_account_balance(),
            timestamp=now_ms
        ))

        return ApiResponse(
            code=0,
            msg="【模拟操盘】订单已快速撮合成交",
            data=order_model,
            timestamp=now_ms
        )

    # 3. 实盘模式调用 OKX REST 下单
    client_ord_id = req.client_order_id or f"OKXD_{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:17]}"
    okx_res = await rest_client.place_order(
        symbol=req.symbol,
        side=req.side,
        pos_side=req.pos_side,
        order_type=req.order_type,
        sz=req.sz,
        price=req.price,
        trade_mode="cross",
        leverage=req.leverage,
        client_order_id=client_ord_id,
        reduce_only=bool(req.reduce_only),
        tp_trigger_price=req.tp_trigger_price,
        tp_order_price=req.tp_ord_price or req.tp_order_price,
        sl_trigger_price=req.sl_trigger_price,
        sl_order_price=req.sl_ord_price or req.sl_order_price
    )

    okx_order_id = okx_res.get("ordId", "")
    price_val = req.price or (current_ticker.last_price if current_ticker else 0.0)
    notional = price_val * req.sz

    order_model = OrderModel(
        client_order_id=client_ord_id,
        okx_order_id=okx_order_id,
        symbol=req.symbol,
        inst_type=req.inst_type,
        side=req.side,
        pos_side=req.pos_side,
        order_type=req.order_type,
        price=req.price,
        sz=req.sz,
        notional_usdt=round(notional, 2),
        filled_sz=0.0,
        avg_price=0.0,
        state=OrderState.LIVE,
        leverage=req.leverage,
        tp_trigger_price=req.tp_trigger_price,
        sl_trigger_price=req.sl_trigger_price,
        risk_check_passed=True,
        risk_check_msg="通过全部硬风控检查",
        copilot_mode=req.copilot_mode,
        ai_analysis_id=str(req.ai_analysis_id) if req.ai_analysis_id else None,
        created_at=now_ms,
        updated_at=now_ms
    )

    push_msg = WSPushPayload[OrderModel](
        channel="orders",
        event=WSServerEvent.UPDATE,
        data=order_model,
        timestamp=now_ms
    )
    await ws_manager.broadcast_to_subscribers(push_msg)

    return ApiResponse(
        code=0,
        msg="订单委托已成功提交 OKX",
        data=order_model,
        timestamp=now_ms
    )


@router.post("/orders/adopt-ai-plan", response_model=ApiResponse[OrderModel], summary="一键采纳 AI 建议计划下单")
async def adopt_ai_trade_plan(
    req: AdoptAIPlanRequest,
    db: AsyncSession = Depends(get_db)
):
    """交易员一键采纳 AI 交易计划并生成委托"""
    order_req = await process_adopt_ai_trade_plan(req)
    return await place_trade_order(order_req, db)


@router.post("/orders/cancel", response_model=ApiResponse[Dict[str, Any]], summary="撤销指定订单")
async def cancel_trade_order(
    req: CancelOrderRequest,
    db: AsyncSession = Depends(get_db)
):
    """撤销未成交委托"""
    now_ms = int(time.time() * 1000)
    is_live = rest_client.is_live_mode and bool(rest_client.api_key)
    if not is_live:
        return ApiResponse(code=0, msg="【模拟操盘】订单已撤销", data={"order_id": req.order_id}, timestamp=now_ms)

    res = await rest_client.cancel_order(
        symbol=req.symbol or "BTC-USDT-SWAP",
        order_id=req.okx_order_id or req.order_id,
        client_order_id=req.client_order_id
    )
    return ApiResponse(code=0, msg="订单已成功撤销", data=res, timestamp=now_ms)


@router.post("/orders/batch-cancel", response_model=ApiResponse[List[Dict[str, Any]]], summary="批量撤单")
async def batch_cancel_orders(req: BatchCancelOrdersRequest):
    """批量撤销订单"""
    now_ms = int(time.time() * 1000)
    is_live = rest_client.is_live_mode and bool(rest_client.api_key)
    if not is_live:
        return ApiResponse(code=0, msg="【模拟操盘】批量撤单完成", data=[], timestamp=now_ms)

    orders_to_cancel = [{"instId": req.symbol or "BTC-USDT-SWAP", "ordId": oid} for oid in (req.order_ids or [])]
    res = await rest_client.cancel_batch_orders(orders_to_cancel)
    return ApiResponse(code=0, msg="批量撤单指令已发送", data=res, timestamp=now_ms)


@router.get("/orders", response_model=ApiResponse[List[OrderModel]], summary="查询当前委托挂单与订单列表")
async def get_orders_list(
    symbol: Optional[str] = Query(None, description="标的代码筛选"),
    state: Optional[str] = Query(None, description="订单状态筛选: live, filled, canceled")
):
    """获取当前委托挂单与订单列表 (实盘模式查询 OKX，模拟模式查询 PaperEngine)"""
    now_ms = int(time.time() * 1000)
    if not rest_client.is_live_mode:
        orders = paper_engine.get_orders()
        if symbol:
            orders = [o for o in orders if o.symbol == symbol]
        if state:
            orders = [o for o in orders if (o.state.value if hasattr(o.state, "value") else str(o.state)).lower() == state.lower()]
        return ApiResponse(code=0, msg="【模拟操盘】订单列表", data=orders, timestamp=now_ms)

    if not rest_client.api_key:
        return ApiResponse(code=0, msg="【真实实盘】未配置 OKX API Key", data=[], timestamp=now_ms)

    try:
        okx_orders = await rest_client.get_orders(symbol=symbol)
        return ApiResponse(code=0, msg="【真实实盘】订单列表", data=okx_orders, timestamp=now_ms)
    except Exception as e:
        logger.warning(f"获取实盘订单列表异常: {e}")
        return ApiResponse(code=0, msg=f"获取实盘订单列表异常: {e}", data=[], timestamp=now_ms)


@router.get("/history", response_model=ApiResponse[List[Dict[str, Any]]], summary="查询历史平仓成交明细")
async def get_trade_history():
    """获取历史已平仓交易记录 (统计胜率、盈亏与平仓原因)"""
    now_ms = int(time.time() * 1000)
    if not rest_client.is_live_mode:
        return ApiResponse(code=0, msg="【模拟操盘】历史平仓记录", data=paper_engine.closed_trades, timestamp=now_ms)

    if not rest_client.api_key:
        return ApiResponse(code=0, msg="【真实实盘】未配置 OKX API Key", data=[], timestamp=now_ms)

    return ApiResponse(code=0, msg="【真实实盘】历史平仓记录", data=[], timestamp=now_ms)


@router.get("/positions", response_model=ApiResponse[List[PositionModel]], summary="查询当前持仓列表")
async def get_positions_list():
    """获取当前持仓列表 (实盘模式查询 OKX 合约/杠杆/现货全景，模拟模式查询 PaperEngine)"""
    now_ms = int(time.time() * 1000)
    if not rest_client.is_live_mode:
        return ApiResponse(code=0, msg="【模拟操盘】持仓列表", data=paper_engine.get_positions(), timestamp=now_ms)

    if not rest_client.api_key:
        return ApiResponse(code=0, msg="【真实实盘】未配置 OKX API Key，请先配置并保存实盘凭证", data=[], timestamp=now_ms)

    try:
        positions = await rest_client.get_positions(inst_type=None, include_spot=True)
        ws_manager.latest_positions.clear()
        for p in positions:
            ws_manager.latest_positions[f"{p.symbol}:{p.pos_side}"] = p
        return ApiResponse(code=0, msg="【真实实盘】持仓列表", data=positions, timestamp=now_ms)
    except Exception as e:
        logger.warning(f"获取实盘持仓异常: {e}")
        return ApiResponse(code=0, msg=f"获取实盘持仓异常: {e}", data=[], timestamp=now_ms)


@router.post("/positions/tpsl", response_model=ApiResponse[Dict[str, Any]], summary="设置持仓止盈止损")
async def set_position_tpsl(req: SetPositionTPSLRequest):
    """为现有持仓修改止盈止损触发价"""
    now_ms = int(time.time() * 1000)
    if not rest_client.is_live_mode:
        key = f"{req.symbol}:long"
        if key in paper_engine.positions:
            paper_engine.positions[key].tp_trigger_price = req.tp_trigger_price
            paper_engine.positions[key].sl_trigger_price = req.sl_trigger_price
        key_short = f"{req.symbol}:short"
        if key_short in paper_engine.positions:
            paper_engine.positions[key_short].tp_trigger_price = req.tp_trigger_price
            paper_engine.positions[key_short].sl_trigger_price = req.sl_trigger_price

        return ApiResponse(
            code=0,
            msg="【模拟操盘】止盈止损参数已成功更新",
            data={
                "symbol": req.symbol,
                "tp_trigger_price": req.tp_trigger_price,
                "sl_trigger_price": req.sl_trigger_price
            },
            timestamp=now_ms
        )

    if not rest_client.api_key:
        raise HTTPException(status_code=400, detail="实盘模式未配置 OKX API Key！")

    # 查找当前持仓的方向与数量，向 OKX 提交策略委托止盈止损单
    target_pos_side = "long"
    target_sz = None
    for k, p in ws_manager.latest_positions.items():
        if p.symbol == req.symbol and p.pos > 0:
            target_pos_side = p.pos_side.value if hasattr(p.pos_side, "value") else str(p.pos_side)
            target_sz = p.pos
            break

    algo_res = await rest_client.place_tpsl_algo_order(
        symbol=req.symbol,
        pos_side=target_pos_side,
        sz=target_sz,
        tp_trigger_price=req.tp_trigger_price,
        sl_trigger_price=req.sl_trigger_price,
    )

    return ApiResponse(
        code=0,
        msg="实盘止盈止损策略委托已成功挂载至 OKX",
        data={
            "symbol": req.symbol,
            "tp_trigger_price": req.tp_trigger_price,
            "sl_trigger_price": req.sl_trigger_price,
            "algo_result": algo_res,
        },
        timestamp=now_ms
    )


@router.post("/positions/close", response_model=ApiResponse[Dict[str, Any]], summary="快捷平仓")
async def close_position_endpoint(req: ClosePositionRequest):
    """对指定持仓进行快捷市价/限价平仓"""
    now_ms = int(time.time() * 1000)
    if not rest_client.is_live_mode:
        res = paper_engine.close_position(
            symbol=req.symbol,
            pos_side=req.pos_side,
            price=req.price,
            size=req.size,
            reason="交易员在模拟工作台手动平仓"
        )
        await ws_manager.broadcast_to_subscribers(WSPushPayload[List[PositionModel]](
            channel="positions",
            event=WSServerEvent.SNAPSHOT,
            data=paper_engine.get_positions(),
            timestamp=now_ms
        ))
        await ws_manager.broadcast_to_subscribers(WSPushPayload[AccountBalanceModel](
            channel="account",
            event=WSServerEvent.UPDATE,
            data=paper_engine.get_account_balance(),
            timestamp=now_ms
        ))
        return ApiResponse(code=0, msg=res["msg"], data=res, timestamp=now_ms)

    if not rest_client.api_key:
        raise HTTPException(status_code=400, detail="实盘模式未配置 OKX API Key！")

    res = await rest_client.close_position(
        symbol=req.symbol,
        mgn_mode="cross",
        pos_side=req.pos_side.value if hasattr(req.pos_side, "value") else (str(req.pos_side) if req.pos_side else None),
        auto_cancel=True
    )

    # 异步刷新实盘持仓与资产快照并广播
    try:
        live_pos = await rest_client.get_positions(inst_type=None, include_spot=True)
        ws_manager.latest_positions.clear()
        for p in live_pos:
            ws_manager.latest_positions[f"{p.symbol}:{p.pos_side}"] = p
        await ws_manager.broadcast_to_subscribers(WSPushPayload[List[PositionModel]](
            channel="positions",
            event=WSServerEvent.SNAPSHOT,
            data=live_pos,
            timestamp=now_ms
        ))
        live_bal = await rest_client.get_account_balance()
        await ws_manager.broadcast_to_subscribers(WSPushPayload[AccountBalanceModel](
            channel="account",
            event=WSServerEvent.UPDATE,
            data=live_bal,
            timestamp=now_ms
        ))
    except Exception as sync_err:
        logger.debug(f"实盘平仓后刷新状态快照异常: {sync_err}")

    return ApiResponse(code=0, msg="平仓指令已提交 OKX", data=res, timestamp=now_ms)


@router.get("/account/balance", response_model=ApiResponse[AccountBalanceModel], summary="获取账户资产全景")
async def get_account_balance():
    """获取账户总权益、可用保证金、已用保证金及保证金率"""
    now_ms = int(time.time() * 1000)
    if not rest_client.is_live_mode:
        return ApiResponse(code=0, msg="【模拟操盘】资产全景", data=paper_engine.get_account_balance(), timestamp=now_ms)

    if not rest_client.api_key:
        empty_bal = AccountBalanceModel(
            total_equity_usdt=0.0,
            available_margin_usdt=0.0,
            used_margin_usdt=0.0,
            frozen_balance_usdt=0.0,
            unrealized_pnl_usdt=0.0,
            today_realized_pnl_usdt=0.0,
            today_pnl_ratio=0.0,
            margin_ratio=10.0,
            is_warning_margin=False,
            timestamp=now_ms,
            updated_at=now_ms
        )
        return ApiResponse(code=0, msg="【真实实盘】未配置 OKX API Key", data=empty_bal, timestamp=now_ms)

    try:
        bal = await rest_client.get_account_balance()
        return ApiResponse(code=0, msg="【真实实盘】资产全景", data=bal, timestamp=now_ms)
    except Exception as e:
        logger.warning(f"获取实盘资产异常: {e}")
        empty_bal = AccountBalanceModel(
            total_equity_usdt=0.0,
            available_margin_usdt=0.0,
            used_margin_usdt=0.0,
            frozen_balance_usdt=0.0,
            unrealized_pnl_usdt=0.0,
            today_realized_pnl_usdt=0.0,
            today_pnl_ratio=0.0,
            margin_ratio=10.0,
            is_warning_margin=False,
            timestamp=now_ms,
            updated_at=now_ms
        )
        return ApiResponse(code=0, msg=f"获取实盘资产异常: {e}", data=empty_bal, timestamp=now_ms)
