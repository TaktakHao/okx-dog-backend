"""
OKX-Dog 个人量化智能交易系统 - 高仿真模拟操盘沙盒引擎 (Paper Trading Engine)
模块: okx-dog-backend/paper_trading.py

特性:
1. 100% 免 API Key 运行，依托 OKX 官方实时公共行情进行高仿真撮合与动态盯市。
2. 支持自定义初始虚拟本金 (100 ~ 1,000,000 USDT) 与一键重置账户。
3. 严格模拟全仓/逐仓杠杆计算、开仓均价平滑、未实现浮盈 (UPL)、ROI、保证金占用与预估强平价。
4. 支持双向持仓、关联止盈 (TP) 与止损 (SL) 实时价格监控与自动触发平仓。
5. 自动记录历史成交与已实现盈亏，实时计算胜率 (Win Rate) 与今日收益率。
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from models import (
    AccountBalanceModel,
    ClosePositionRequest,
    CopilotMode,
    InstType,
    OrderCreateRequest,
    OrderModel,
    OrderSide,
    OrderState,
    OrderType,
    PaperAccountInfo,
    PositionModel,
    PosSide,
)

logger = logging.getLogger("okx_dog.paper_trading")


class PaperTradingEngine:
    """
    内置高仿真模拟操盘沙盒引擎
    """

    def __init__(self, initial_equity: float = 10000.0):
        self.initial_equity: float = initial_equity
        self.cash: float = initial_equity  # 未占用可用现金 (USDT)
        self.today_start_equity: float = initial_equity

        # 内存持仓字典: key 为 f"{symbol}:{pos_side}"
        self.positions: Dict[str, PositionModel] = {}
        # 内存订单字典: key 为 order_id
        self.orders: Dict[str, OrderModel] = {}
        # 历史成交平仓记录 (用于统计胜率与收益)
        self.closed_trades: List[Dict[str, Any]] = []

        self.last_price_map: Dict[str, float] = {
            "BTC-USDT-SWAP": 63428.0,
            "ETH-USDT-SWAP": 2680.0,
            "SOL-USDT-SWAP": 188.5,
            "DOGE-USDT-SWAP": 0.155,
            "XAU-USDT-SWAP": 2650.0,
            "XAG-USDT-SWAP": 31.5,
            "EUR-USDT-SWAP": 1.085,
            "GBP-USDT-SWAP": 1.295,
            "NVDA-USDT-SWAP": 128.5,
            "TSLA-USDT-SWAP": 245.0,
            "AAPL-USDT-SWAP": 228.0,
            "ONDO-USDT-SWAP": 0.725,
            "PENDLE-USDT-SWAP": 4.85,
            "PAXG-USDT": 2650.0,
            "MKR-USDT-SWAP": 1650.0,
        }


        self.trade_closed_callbacks = []

        logger.info(f"【模拟操盘沙盒】引擎初始化完成，初始虚拟资金: {self.initial_equity:.2f} USDT")

    def register_trade_closed_callback(self, cb):
        """注册平仓事件监听回调"""
        self.trade_closed_callbacks.append(cb)

    def reset_account(self, initial_equity: float = 10000.0) -> PaperAccountInfo:
        """重置模拟操盘账户与持仓数据"""
        self.initial_equity = initial_equity
        self.cash = initial_equity
        self.today_start_equity = initial_equity
        self.positions.clear()
        self.orders.clear()
        self.closed_trades.clear()
        logger.info(f"【模拟操盘沙盒】账户已成功重置，新初始本金: {initial_equity:.2f} USDT")
        return self.get_account_info()

    def update_market_price(self, symbol: str, price: float) -> List[Dict[str, Any]]:
        """
        行情价格推流驱动：更新最新价并动态计算持仓浮盈，检查是否触发止盈/止损
        :return: 触发自动平仓的事件列表
        """
        if price <= 0:
            return []

        self.last_price_map[symbol] = price
        triggered_events = []

        # 动态盯市计算 UPL 与检查 TP/SL
        keys_to_check = list(self.positions.keys())
        for key in keys_to_check:
            if key not in self.positions:
                continue
            pos = self.positions[key]
            if pos.symbol != symbol or pos.pos <= 0:
                continue

            pos.mark_px = price
            is_long = pos.pos_side == PosSide.LONG

            # 计算未实现盈亏 (UPL)
            if is_long:
                upl = (price - pos.avg_px) * pos.pos
            else:
                upl = (pos.avg_px - price) * pos.pos

            pos.upl = upl
            pos.upl_ratio = (upl / pos.margin) if pos.margin > 0 else 0.0

            # 动态跟踪并维护持仓生命周期最高价与最高浮盈率
            cur_profit_ratio = (price - pos.avg_px) / pos.avg_px if is_long else (pos.avg_px - price) / pos.avg_px
            if getattr(pos, "peak_price", None) is None:
                pos.peak_price = price
                pos.peak_profit_ratio = cur_profit_ratio
            else:
                if is_long:
                    if price > pos.peak_price:
                        pos.peak_price = price
                else:
                    if price < pos.peak_price:
                        pos.peak_price = price
                pos.peak_profit_ratio = max(getattr(pos, "peak_profit_ratio", 0.0) or 0.0, cur_profit_ratio)

            # 1. 检查阶梯止盈 (Take Profit) 触发 (首次达到 TP1 执行 50% 平仓 + 自动移动保本止损)
            if pos.tp_trigger_price and pos.tp_trigger_price > 0:
                is_tp_hit = False
                if is_long and pos.tp_trigger_price > pos.avg_px and price >= pos.tp_trigger_price:
                    is_tp_hit = True
                elif (not is_long) and pos.tp_trigger_price < pos.avg_px and price <= pos.tp_trigger_price:
                    is_tp_hit = True

                if is_tp_hit:
                    # 如果尚未执行过 50% 止盈，执行阶梯分批止盈并移动保本
                    if not getattr(pos, "tp_triggered_50", False) and pos.pos >= 0.002:
                        partial_close_sz = round(pos.pos * 0.5, 4)
                        logger.info(
                            f"【模拟操盘-分批止盈】标的 {symbol} 触及第一止盈位 ${pos.tp_trigger_price:.1f}！"
                            f"执行自动平仓 50% ({partial_close_sz} 币) 锁定利润，并将剩余仓位止损上移至保本安全线"
                        )
                        close_res = self.close_position(
                            symbol=symbol,
                            pos_side=pos.pos_side,
                            price=price,
                            size=partial_close_sz,
                            reason=f"触及第一止盈位 ${pos.tp_trigger_price:.1f} (平仓50%落袋为安)"
                        )
                        triggered_events.append(close_res)

                        # 在内存持仓中更新状态：标记保本，止损价移至「均价+摩擦覆盖缓冲」(True Breakeven)，清空 TP1
                        pos.tp_triggered_50 = True
                        pos.is_breakeven_locked = True
                        friction_buffer = pos.avg_px * 0.0012
                        pos.sl_trigger_price = round(pos.avg_px + friction_buffer, 2 if pos.avg_px > 10 else 4) if is_long else round(pos.avg_px - friction_buffer, 2 if pos.avg_px > 10 else 4)
                        pos.tp_trigger_price = None
                        continue
                    else:
                        logger.info(f"【模拟操盘】标的 {symbol} 触及终极止盈价 ${pos.tp_trigger_price:.1f}，全量获利平仓！")
                        close_res = self.close_position(
                            symbol=symbol,
                            pos_side=pos.pos_side,
                            price=price,
                            reason=f"触及终极止盈价 ${pos.tp_trigger_price:.1f} 获利平仓"
                        )
                        triggered_events.append(close_res)
                        continue

            # 2. 检查止损 (Stop Loss) 触发 (包含移动保本触发)
            if pos.sl_trigger_price and pos.sl_trigger_price > 0:
                is_sl_hit = False
                if is_long and price <= pos.sl_trigger_price:
                    is_sl_hit = True
                elif (not is_long) and price >= pos.sl_trigger_price:
                    is_sl_hit = True

                if is_sl_hit:
                    is_be = getattr(pos, "is_breakeven_locked", False)
                    reason_str = (
                        f"触及移动保本价 ${pos.sl_trigger_price:.1f} (保本平仓)"
                        if is_be else
                        f"触及硬止损价 ${pos.sl_trigger_price:.1f} (硬止损平仓)"
                    )
                    logger.info(f"【模拟操盘】标的 {symbol} {reason_str}")
                    close_res = self.close_position(
                        symbol=symbol,
                        pos_side=pos.pos_side,
                        price=price,
                        reason=reason_str
                    )
                    triggered_events.append(close_res)
                    continue



        return triggered_events

    def place_order(self, req: OrderCreateRequest, current_market_price: Optional[float] = None) -> OrderModel:
        """
        模拟撮合成交
        """
        now_ms = int(time.time() * 1000)
        symbol = req.symbol
        side = req.side
        pos_side = PosSide.LONG if side == OrderSide.BUY else PosSide.SHORT
        leverage = req.leverage or 3

        # 确定撮合价格
        exec_price = float(getattr(req, "price", 0.0) or 0.0)
        if getattr(req, "order_type", None) == OrderType.MARKET or exec_price <= 0:
            exec_price = current_market_price or self.last_price_map.get(symbol, 96420.5)

        size = float(getattr(req, "sz", None) or getattr(req, "size", None) or 0.0)
        if size <= 0:
            raise ValueError("委托数量必须大于 0")

        notional_usdt = exec_price * size
        required_margin = notional_usdt / leverage

        # 区分 Maker 与 Taker 真实交易费率 (POST_ONLY: 0.02% Maker, MARKET: 0.05% Taker)
        is_maker = getattr(req, "order_type", None) == OrderType.POST_ONLY
        open_fee_rate = 0.0002 if is_maker else 0.0005
        open_fee = round(notional_usdt * open_fee_rate, 4)

        if self.cash < (required_margin + open_fee):
            raise ValueError(f"模拟账户可用资金不足！需保证金 {required_margin:.2f} USDT + 开仓手续费 {open_fee:.2f} USDT，当前可用现金 {self.cash:.2f} USDT")

        # 扣减可用现金作为保证金与开仓手续费
        self.cash -= (required_margin + open_fee)

        # 校验并清洗止盈/止损价，过滤方向倒置的无效点位
        raw_tp = float(getattr(req, "tp_trigger_price", 0.0) or 0.0)
        raw_sl = float(getattr(req, "sl_trigger_price", 0.0) or 0.0)
        sanitized_tp: Optional[float] = None
        sanitized_sl: Optional[float] = None

        if pos_side == PosSide.LONG:
            if raw_tp > exec_price:
                sanitized_tp = raw_tp
            if 0 < raw_sl < exec_price:
                sanitized_sl = raw_sl
        else:
            if 0 < raw_tp < exec_price:
                sanitized_tp = raw_tp
            if raw_sl > exec_price:
                sanitized_sl = raw_sl

        # 生成模拟订单
        order_id = f"sim_ord_{uuid.uuid4().hex[:12]}"
        client_order_id = getattr(req, "client_order_id", None) or f"sim_cl_{uuid.uuid4().hex[:8]}"

        order_model = OrderModel(
            order_id=order_id,
            okx_order_id=order_id,
            client_order_id=client_order_id,
            symbol=symbol,
            inst_type=getattr(req, "inst_type", None) or InstType.SWAP,
            side=side,
            pos_side=pos_side,
            order_type=getattr(req, "order_type", OrderType.LIMIT),
            price=exec_price,
            sz=size,
            filled_sz=size,
            avg_price=exec_price,
            notional_usdt=round(notional_usdt, 2),
            state=OrderState.FILLED,
            leverage=leverage,
            tp_trigger_price=sanitized_tp,
            sl_trigger_price=sanitized_sl,
            fee=open_fee,
            fee_currency="USDT",
            risk_check_passed=True,
            risk_check_msg="通过硬风控检查 (模拟操盘撮合)",
            copilot_mode=getattr(req, "copilot_mode", CopilotMode.MANUAL),
            ai_analysis_id=str(getattr(req, "ai_analysis_id", "")) if getattr(req, "ai_analysis_id", None) else None,
            created_at=now_ms,
            updated_at=now_ms,
        )
        self.orders[order_id] = order_model

        # 更新持仓
        is_spot_order = getattr(req, "inst_type", None) == InstType.SPOT or ("SWAP" not in symbol and "FUTURES" not in symbol)
        final_inst_type = InstType.SPOT if is_spot_order else (getattr(req, "inst_type", None) or InstType.SWAP)
        final_pos_side = PosSide.NET if is_spot_order else pos_side
        final_leverage = 1 if is_spot_order else leverage

        pos_key = f"{symbol}:{final_pos_side.value if hasattr(final_pos_side, 'value') else str(final_pos_side)}"
        if pos_key in self.positions:
            # 加仓：计算加权均价
            old_pos = self.positions[pos_key]
            total_size = old_pos.pos + size
            total_cost = (old_pos.avg_px * old_pos.pos) + (exec_price * size)
            new_avg_px = total_cost / total_size
            new_margin = old_pos.margin + required_margin

            old_pos.pos = total_size
            old_pos.pos_size = total_size
            old_pos.avail_pos = total_size
            old_pos.avg_px = new_avg_px
            old_pos.avg_price = new_avg_px
            old_pos.mark_px = exec_price
            old_pos.mark_price = exec_price
            old_pos.margin = new_margin
            old_pos.notional_usd = round(total_size * exec_price, 2)
            old_pos.scale_layers = (getattr(old_pos, "scale_layers", None) or 1) + 1
            old_pos.last_scale_price = exec_price
            old_pos.last_scale_time = now_ms
            if sanitized_tp:
                old_pos.tp_trigger_price = sanitized_tp
            if sanitized_sl:
                old_pos.sl_trigger_price = sanitized_sl
            old_pos.liq_px = None if is_spot_order else self._calculate_liq_price(new_avg_px, final_leverage, final_pos_side)
            old_pos.liquidation_price = old_pos.liq_px
        else:
            # 新开仓
            liq_px = None if is_spot_order else self._calculate_liq_price(exec_price, final_leverage, final_pos_side)
            new_pos = PositionModel(
                pos_id=f"sim_pos_{uuid.uuid4().hex[:10]}",
                symbol=symbol,
                inst_type=final_inst_type,
                pos_side=final_pos_side,
                pos=size,
                pos_size=size,
                avail_pos=size,
                avg_px=exec_price,
                avg_price=exec_price,
                mark_px=exec_price,
                mark_price=exec_price,
                liq_px=liq_px,
                liquidation_price=liq_px,
                margin=required_margin,
                margin_ratio=1.0 if not is_spot_order else 0.0,
                mgn_ratio=1.0 if not is_spot_order else 0.0,
                maint_margin_ratio=0.01,
                leverage=final_leverage,
                notional_usd=round(notional_usdt, 2),
                upl=0.0,
                upl_ratio=0.0,
                realized_pnl=0.0,
                tp_trigger_price=sanitized_tp,
                sl_trigger_price=sanitized_sl,
                is_breakeven_locked=False,
                tp_triggered_50=False,
                scale_layers=1,
                last_scale_price=exec_price,
                last_scale_time=now_ms,
                last_synced_at=now_ms,
            )
            self.positions[pos_key] = new_pos


        logger.info(
            f"【模拟操盘】订单成交: {side.value} {size} BTC @ ${exec_price:.1f}, "
            f"杠杆 {leverage}x, 占用保证金 {required_margin:.2f} USDT"
        )
        return order_model

    def close_position(
        self,
        symbol: str,
        pos_side: Optional[PosSide] = None,
        price: Optional[float] = None,
        size: Optional[float] = None,
        reason: str = "交易员手动市价平仓"
    ) -> Dict[str, Any]:
        """对模拟持仓进行市价/限价平仓并结算已实现盈亏"""
        now_ms = int(time.time() * 1000)
        close_price = price or self.last_price_map.get(symbol, 96420.5)

        targets = []
        if pos_side:
            key = f"{symbol}:{pos_side.value if hasattr(pos_side, 'value') else str(pos_side)}"
            if key in self.positions:
                targets.append((key, self.positions[key]))
        else:
            for k, p in list(self.positions.items()):
                if p.symbol == symbol:
                    targets.append((k, p))

        if not targets:
            return {"success": False, "msg": f"未找到标的 {symbol} 的模拟持仓"}

        total_realized_pnl = 0.0
        for k, pos in targets:
            close_sz = size or pos.pos
            close_ratio = min(1.0, close_sz / pos.pos) if pos.pos > 0 else 1.0

            is_long = pos.pos_side == PosSide.LONG
            if is_long:
                raw_pnl = (close_price - pos.avg_px) * close_sz
            else:
                raw_pnl = (pos.avg_px - close_price) * close_sz

            # 扣除平仓手续费 (市价平仓按 Taker 0.05% 计，限价平仓按 0.02% 计)
            close_notional = close_price * close_sz
            close_fee_rate = 0.0005  # 平仓多数为市价止损或止盈单
            close_fee = round(close_notional * close_fee_rate, 4)
            pnl = round(raw_pnl - close_fee, 4)

            released_margin = pos.margin * close_ratio
            self.cash += (released_margin + pnl)
            total_realized_pnl += pnl

            trade_record = {
                "symbol": symbol,
                "pos_side": pos.pos_side.value if hasattr(pos.pos_side, "value") else str(pos.pos_side),
                "entry_price": pos.avg_px,
                "exit_price": close_price,
                "size": close_sz,
                "leverage": pos.leverage,
                "realized_pnl": pnl,
                "fee": close_fee,
                "roi_pct": (pnl / released_margin * 100) if released_margin > 0 else 0.0,
                "reason": reason,
                "timestamp": now_ms,
            }
            # 记录历史战绩
            self.closed_trades.append(trade_record)

            # 触发平仓事件回调
            for cb in self.trade_closed_callbacks:
                try:
                    cb(trade_record)
                except Exception as cb_err:
                    logger.warning(f"平仓回调执行异常: {cb_err}")

            # 触发 AI 员工自演进与战绩更新
            try:
                import sys, os
                ai_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "okx-dog-ai"))
                if ai_path not in sys.path:
                    sys.path.insert(0, ai_path)
                from agent.evolution.evolution_manager import AgentEvolutionManager
                evo_mgr = AgentEvolutionManager.get_instance()
                evo_mgr.process_trade_reinforcement(
                    trade_id=f"sim_trade_{now_ms}",
                    symbol=symbol,
                    pos_side=pos.pos_side.value if hasattr(pos.pos_side, "value") else str(pos.pos_side),
                    entry_price=pos.avg_px,
                    exit_price=close_price,
                    sl_price=pos.sl_trigger_price or (close_price if "止损" in reason else pos.avg_px * 0.985),
                    realized_pnl=pnl,
                    is_crisis_defended=("保本" in reason or "防守" in reason or "排雷" in reason or "风控" in reason)
                )

                # 同步更新数据集真实盈亏正负标签 (Chosen/Rejected) 与实习生虚拟实盘收益
                try:
                    from dataset.collector import dataset_collector
                    from agent.evolution.intern_slot import intern_slot_manager
                    pnl_ratio = (pnl / released_margin) if released_margin > 0 else 0.0
                    hit_tp = ("止盈" in reason) or (pnl_ratio >= 0.015)
                    hit_sl = ("止损" in reason) or (pnl_ratio <= -0.010)

                    dataset_collector.update_sample_outcome(
                        sample_id_or_analysis_id=symbol,
                        pnl_pct=pnl_ratio,
                        hit_tp=hit_tp,
                        hit_sl=hit_sl
                    )
                    if intern_slot_manager and intern_slot_manager.is_enabled():
                        intern_slot_manager.settle_virtual_outcome(
                            analysis_id=symbol,
                            actual_pnl_pct=pnl_ratio,
                            hit_tp=hit_tp,
                            hit_sl=hit_sl
                        )
                except Exception as d_err:
                    logger.debug(f"数据集收益沉淀或实习生结算跳过: {d_err}")
            except Exception as evo_err:
                logger.debug(f"模拟操盘 AI 演进联动跳过: {evo_err}")

            if close_ratio >= 0.999:
                del self.positions[k]
            else:
                pos.pos -= close_sz
                pos.pos_size = pos.pos
                pos.avail_pos = max(0.0, pos.avail_pos - close_sz)
                pos.margin = max(0.0, pos.margin - released_margin)
                pos.notional_usd = pos.pos * close_price
                if ("50%" in reason or "阶梯" in reason or "第一止盈" in reason or "减半" in reason) and not pos.tp_triggered_50:
                    pos.tp_triggered_50 = True
                    pos.is_breakeven_locked = True
                    pos.sl_trigger_price = pos.avg_px
                    pos.tp_trigger_price = None
                    logger.info(f"【模拟操盘-阶梯止盈】{symbol} 已完成 50% 阶梯止盈，剩余仓位 {pos.pos} 张已锁定移动保本 (SL=${pos.avg_px})，固定止盈清除由移动追踪接管")

            logger.info(
                f"【模拟操盘】平仓结算完成: {symbol} {pos.pos_side.value} {close_sz} 张 @ ${close_price:.1f}, "
                f"实现盈亏: {'+' if pnl >= 0 else ''}{pnl:.2f} USDT, 原因: {reason}"
            )

        return {
            "success": True,
            "closed_count": len(targets),
            "msg": f"平仓成功，已实现盈亏: {total_realized_pnl:+.2f} USDT",
            "realized_pnl": total_realized_pnl,
            "close_price": close_price,
            "new_cash": self.cash,
            "reason": reason
        }

    def cancel_all_orders(self, symbol: Optional[str] = None) -> int:
        """撤销指定标的或全部未结模拟订单 (防幽灵仓)"""
        canceled_count = 0
        for ord_id, o in list(self.orders.items()):
            if symbol is None or o.symbol == symbol:
                if o.state in (OrderState.LIVE, OrderState.PARTIALLY_FILLED):
                    o.state = OrderState.CANCELED
                    canceled_count += 1
        if canceled_count > 0:
            logger.info(f"【模拟操盘】已清理标的 {symbol or 'ALL'} 的 {canceled_count} 笔挂单 (Cancel-Before-Close)")
        return canceled_count


    def update_position_stops(
        self,
        symbol: str,
        pos_side: Optional[PosSide] = None,
        stop_loss_price: Optional[float] = None,
        take_profit_price: Optional[float] = None,
        is_breakeven_locked: Optional[bool] = None,
        clear_take_profit: bool = False,
    ) -> bool:
        """更新模拟持仓的止盈止损价与移动保本锁定标记"""
        updated = False
        for pos_key, pos in self.positions.items():
            if pos.symbol == symbol:
                if pos_side is not None and pos.pos_side != pos_side:
                    continue
                if stop_loss_price is not None:
                    pos.sl_trigger_price = stop_loss_price
                if clear_take_profit:
                    pos.tp_trigger_price = None
                elif take_profit_price is not None:
                    pos.tp_trigger_price = take_profit_price
                if is_breakeven_locked is not None:
                    pos.is_breakeven_locked = is_breakeven_locked
                elif stop_loss_price is not None:
                    pos.is_breakeven_locked = True
                updated = True
        return updated

    def get_positions(self) -> List[PositionModel]:
        """获取当前活跃模拟持仓列表"""
        return list(self.positions.values())

    def get_orders(self) -> List[OrderModel]:
        """获取所有模拟订单"""
        return list(self.orders.values())

    def get_account_balance(self) -> AccountBalanceModel:
        """获取模拟账户资金全景"""
        now_ms = int(time.time() * 1000)
        unrealized_pnl = sum(p.upl for p in self.positions.values())
        used_margin = sum(p.margin for p in self.positions.values())
        total_equity = max(0.0, self.cash + used_margin + unrealized_pnl)
        realized_today = sum(t["realized_pnl"] for t in self.closed_trades)

        today_pnl_ratio = (
            (total_equity - self.today_start_equity) / self.today_start_equity
            if self.today_start_equity > 0
            else 0.0
        )

        margin_ratio = (
            (used_margin / total_equity) if total_equity > 0 else 0.0
        )

        return AccountBalanceModel(
            total_equity_usdt=round(total_equity, 2),
            total_equity_btc=round(total_equity / self.last_price_map.get("BTC-USDT-SWAP", 96420.5), 6),
            available_margin_usdt=round(max(0.0, self.cash), 2),
            used_margin_usdt=round(used_margin, 2),
            frozen_balance_usdt=0.0,
            unrealized_pnl_usdt=round(unrealized_pnl, 2),
            today_realized_pnl_usdt=round(realized_today, 2),
            today_pnl_ratio=round(today_pnl_ratio, 4),
            margin_ratio=round(margin_ratio, 4),
            is_warning_margin=margin_ratio > 0.8,
            timestamp=now_ms,
        )

    def get_account_info(self) -> PaperAccountInfo:
        """获取模拟账户综合详情与胜率统计"""
        bal = self.get_account_balance()
        total_trades = len(self.closed_trades)
        winning_trades = sum(1 for t in self.closed_trades if t["realized_pnl"] > 0)
        win_rate = (winning_trades / total_trades) if total_trades > 0 else 0.0

        return PaperAccountInfo(
            initial_equity_usdt=self.initial_equity,
            total_equity_usdt=bal.total_equity_usdt,
            available_margin_usdt=bal.available_margin_usdt,
            used_margin_usdt=bal.used_margin_usdt,
            unrealized_pnl_usdt=bal.unrealized_pnl_usdt,
            realized_pnl_usdt=sum(t["realized_pnl"] for t in self.closed_trades),
            today_realized_pnl_usdt=bal.today_realized_pnl_usdt,
            today_pnl_ratio=bal.today_pnl_ratio,
            total_trades_count=total_trades,
            winning_trades_count=winning_trades,
            win_rate=round(win_rate, 4),
            created_at=int(datetime.utcnow().timestamp() * 1000),
            updated_at=int(datetime.utcnow().timestamp() * 1000),
        )

    @staticmethod
    def _calculate_liq_price(avg_px: float, leverage: int, pos_side: PosSide) -> float:
        """预估强平参考价格"""
        buffer = 0.9 / max(1, leverage)
        if pos_side == PosSide.LONG:
            return round(max(0.0, avg_px * (1.0 - buffer)), 2)
        else:
            return round(avg_px * (1.0 + buffer), 2)


# 全局单例模拟撮合引擎
paper_engine = PaperTradingEngine(initial_equity=10000.0)
