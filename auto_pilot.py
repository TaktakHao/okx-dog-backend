"""
OKX-Dog 个人量化智能交易系统 - 7x24h 全自动后端常驻量化操盘引擎与收益追踪中枢
模块: okx-dog-backend/auto_pilot.py

核心职责:
1. 7x24h 脱机自主运行：独立于前端浏览器，常驻后台协程循环巡检
2. 多标的监控池 (Multi-Symbol Pool)：并发轮巡 TradFi (黄金/大宗/美股代币/RWA) 与主流加密资产
3. 持仓移动保本与动态追踪止损 (Breakeven & Trailing Stop)：浮盈达标自动将止损线无损移至开仓保本价
4. 组合级持仓与敞口控制：控制最大同时持仓数 (max_concurrent_positions)，防单一板块过度暴露
5. 挂单生命周期与超时清理 (Stale Order Reaper)：自动撤销长时间未成交的挂单
6. 毫秒级收益与胜率归因统计，持久化记录到 SQLite
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import json
import logging
import math
import time
import uuid
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import text
from database import get_db_session
from models import (
    AutoPilotConfigUpdateRequest,
    AutoPilotHistoryPoint,
    AutoPilotHistoryResponse,
    AutoPilotStartRequest,
    AutoPilotStatusModel,
    AutoPilotTradeRecord,
    InstType,
    OrderSide,
    OrderType,
    PlaceOrderRequest,
    PosSide,
    SignalAction,
)

logger = logging.getLogger("okx_dog.auto_pilot")


class AutoPilotManager:
    """
    自动操盘中枢与全景收益追踪器 (单例)
    支持 7x24h 后端脱机常驻、多标的池轮巡、移动保本与硬风控联动
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self._is_patrolling: bool = False
        self.is_running: bool = True  # 默认开启全自动操盘
        self.session_id: str = f"auto_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        self.start_time: int = int(time.time() * 1000)
        self.stop_time: Optional[int] = None
        self.start_equity: float = 10000.0
        self.current_equity: float = 10000.0
        self.peak_equity: float = 10000.0
        self.max_drawdown_pct: float = 0.0

        self.is_live_mode: bool = False
        self.symbol: str = "BTC-USDT-SWAP"
        # 多标的监控轮巡池 (默认覆盖 TradFi 黄金/美股代币/RWA/加密龙头)
        self.symbols: List[str] = [
            "XAU-USDT-SWAP",
            "NVDA-USDT-SWAP",
            "TSLA-USDT-SWAP",
            "ONDO-USDT-SWAP",
            "BTC-USDT-SWAP",
            "ETH-USDT-SWAP",
        ]
        self.max_concurrent_positions: int = 3
        self.trailing_stop_enabled: bool = True
        self.breakeven_trigger_ratio: float = 0.5  # 浮盈达到 0.5R 触发移动保本
        self.min_confidence: float = 0.70
        self.auto_leverage_enabled: bool = True
        self.order_timeout_seconds: int = 180  # 挂单 3 分钟未成交自动撤单

        # 巡检状态记录
        self.last_patrol_time: Optional[int] = None
        self.last_patrol_log: Optional[str] = "系统初始化完成，等待后台巡检"
        self._executed_plan_ids: Set[str] = set()
        self._breakeven_applied_positions: Set[str] = set()

        # 内存中维护的历史快照与平仓流水
        self.snapshots: List[AutoPilotHistoryPoint] = []
        self.trades: List[AutoPilotTradeRecord] = []

        # 初始注入第一个基准点
        now_ms = int(time.time() * 1000)
        self.snapshots.append(
            AutoPilotHistoryPoint(
                timestamp=now_ms,
                equity=self.start_equity,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                total_pnl=0.0,
                pnl_ratio=0.0,
            )
        )

        logger.info(f"【自动操盘中枢】初始化完成，Session ID: {self.session_id}, 监控池: {self.symbols}")

    async def start(
        self,
        initial_equity: float,
        is_live_mode: bool = False,
        symbol: str = "BTC-USDT-SWAP",
        symbols: Optional[List[str]] = None,
        max_concurrent_positions: Optional[int] = None,
        trailing_stop_enabled: Optional[bool] = None,
        breakeven_trigger_ratio: Optional[float] = None,
        min_confidence: float = 0.70,
        auto_leverage_enabled: bool = True,
    ) -> AutoPilotStatusModel:
        """
        开启或恢复自动量化操盘会话 (7x24h 后端常驻)
        """
        async with self._lock:
            now_ms = int(time.time() * 1000)
            self.is_running = True
            self.session_id = f"auto_{int(time.time())}_{uuid.uuid4().hex[:6]}"
            self.start_time = now_ms
            self.stop_time = None
            self.start_equity = max(10.0, initial_equity)
            self.current_equity = self.start_equity
            self.peak_equity = self.start_equity
            self.max_drawdown_pct = 0.0
            self.is_live_mode = is_live_mode
            self.symbol = symbol

            if symbols and len(symbols) > 0:
                self.symbols = [s.strip().upper() for s in symbols if s.strip()]
            if max_concurrent_positions is not None:
                self.max_concurrent_positions = max(1, min(10, max_concurrent_positions))
            if trailing_stop_enabled is not None:
                self.trailing_stop_enabled = trailing_stop_enabled
            if breakeven_trigger_ratio is not None:
                self.breakeven_trigger_ratio = breakeven_trigger_ratio

            self.min_confidence = min_confidence
            self.auto_leverage_enabled = auto_leverage_enabled

            self.snapshots.clear()
            self.trades.clear()
            self._executed_plan_ids.clear()
            self._breakeven_applied_positions.clear()

            # 初始快照点
            initial_point = AutoPilotHistoryPoint(
                timestamp=now_ms,
                equity=self.start_equity,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                total_pnl=0.0,
                pnl_ratio=0.0,
            )
            self.snapshots.append(initial_point)

            # 异步写入数据库
            try:
                async with get_db_session() as session:
                    await session.execute(
                        text(
                            """
                            INSERT INTO auto_pilot_sessions 
                            (session_id, is_live_mode, is_running, start_time, start_equity, current_equity, 
                             symbol, min_confidence, auto_leverage)
                            VALUES (:sess, :live, 1, :st, :seq, :ceq, :sym, :conf, :lev)
                            """
                        ),
                        {
                            "sess": self.session_id,
                            "live": 1 if self.is_live_mode else 0,
                            "st": self.start_time,
                            "seq": self.start_equity,
                            "ceq": self.current_equity,
                            "sym": self.symbol,
                            "conf": self.min_confidence,
                            "lev": 1 if self.auto_leverage_enabled else 0,
                        },
                    )
            except Exception as e:
                logger.warning(f"【自动操盘中枢】写入数据库会话失败: {e}")

            logger.info(
                f"【自动操盘中枢】7x24h 常驻自动操盘已启动！Session: {self.session_id}, "
                f"基准本金: {self.start_equity:.2f} USDT, 模式: {'实盘' if is_live_mode else '模拟'}, "
                f"监控标的池: {self.symbols}"
            )

            return self.get_status(current_equity=initial_equity, unrealized_pnl=0.0)

    async def stop(self, current_equity: Optional[float] = None) -> AutoPilotStatusModel:
        """暂停 / 停止自动操盘"""
        async with self._lock:
            now_ms = int(time.time() * 1000)
            self.is_running = False
            self.stop_time = now_ms
            if current_equity is not None:
                self.current_equity = current_equity

            try:
                async with get_db_session() as session:
                    await session.execute(
                        text(
                            """
                            UPDATE auto_pilot_sessions 
                            SET is_running = 0, stop_time = :st, current_equity = :ceq, updated_at = CURRENT_TIMESTAMP
                            WHERE session_id = :sess
                            """
                        ),
                        {
                            "st": self.stop_time,
                            "ceq": self.current_equity,
                            "sess": self.session_id,
                        },
                    )
            except Exception as e:
                logger.warning(f"【自动操盘中枢】更新停止状态失败: {e}")

            logger.info(f"【自动操盘中枢】已暂停自动操盘。Session: {self.session_id}")
            return self.get_status(current_equity=self.current_equity, unrealized_pnl=0.0)

    async def update_config(
        self,
        min_confidence: Optional[float] = None,
        auto_leverage_enabled: Optional[bool] = None,
        symbol: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        max_concurrent_positions: Optional[int] = None,
        trailing_stop_enabled: Optional[bool] = None,
        breakeven_trigger_ratio: Optional[float] = None,
    ):
        """更新策略配置"""
        async with self._lock:
            if min_confidence is not None:
                self.min_confidence = min_confidence
            if auto_leverage_enabled is not None:
                self.auto_leverage_enabled = auto_leverage_enabled
            if symbol is not None:
                self.symbol = symbol
            if symbols is not None and len(symbols) > 0:
                self.symbols = [s.strip().upper() for s in symbols if s.strip()]
            if max_concurrent_positions is not None:
                self.max_concurrent_positions = max(1, min(10, max_concurrent_positions))
            if trailing_stop_enabled is not None:
                self.trailing_stop_enabled = trailing_stop_enabled
            if breakeven_trigger_ratio is not None:
                self.breakeven_trigger_ratio = breakeven_trigger_ratio
            logger.info(f"【自动操盘中枢】策略配置已更新: 监控池={self.symbols}, 最大持仓={self.max_concurrent_positions}")

    # =========================================================================
    # 7x24h 后端常驻自主巡检核心循环 (Independent Backend Patrol Engine)
    # =========================================================================

    async def execute_patrol_step(self):
        """
        后端守护协程调用的单步自主巡检：
        1. 检查运行状态与硬风控熔断锁
        2. 扫描已有持仓，执行移动保本 (Breakeven) 与动态跟踪止损 (Trailing Stop)
        3. 检查持仓并发上限，多标的并发轮查 TradFi 与加密资产
        4. 交易时段 (Session) 过滤与科学仓位预算
        5. 自动经由硬风控中枢下单并附带原生交易所条件止损
        6. 超时挂单自动撤单 (Stale Order Reaper)
        """
        if not self.is_running or self._is_patrolling:
            return

        self._is_patrolling = True
        now_ms = int(time.time() * 1000)
        self.last_patrol_time = now_ms

        try:
            from state import (
                paper_engine,
                rest_client,
                risk_controller,
                ws_manager,
            )
            from services.ai_service import generate_quantitative_rule_decision
            from services.market_service import get_multi_period_indicators_data, get_derivatives_metrics_data

            # 0. 检查系统硬风控状态
            if risk_controller.risk_status.value == "PANIC_LOCKED":
                self.last_patrol_log = "⚠️ 硬风控处于 PANIC_LOCKED 熔断锁定状态，跳过自动操盘巡检"
                return

            is_live = rest_client.is_live_mode and bool(rest_client.api_key)

            # 1. 获取当前持仓与资产
            if is_live:
                try:
                    positions = await rest_client.get_positions()
                    bal = await rest_client.get_account_balance()
                    avail_funds = bal.available_margin_usdt if bal.available_margin_usdt > 0 else bal.total_equity_usdt
                except Exception as e:
                    self.last_patrol_log = f"实盘账户信息拉取异常: {e}"
                    return
            else:
                positions = paper_engine.get_positions()
                bal = paper_engine.get_account_balance()
                avail_funds = bal.available_margin_usdt if bal.available_margin_usdt > 0 else bal.total_equity_usdt

            # 2. 持仓监控：执行移动保本 (Breakeven) 与动态追踪止损
            held_symbols = set()
            for pos in positions:
                if pos.pos <= 0:
                    continue
                sym = pos.symbol
                held_symbols.add(sym)
                pos_id = pos.pos_id or f"{sym}_{pos.pos_side}"

                if self.trailing_stop_enabled and pos_id not in self._breakeven_applied_positions:
                    entry_px = pos.avg_px
                    cur_px = pos.mark_px or entry_px
                    if entry_px > 0:
                        is_long = pos.pos_side == PosSide.LONG
                        profit_ratio = (cur_px - entry_px) / entry_px if is_long else (entry_px - cur_px) / entry_px

                        if profit_ratio >= 0.008:  # 浮盈超 0.8%
                            self._breakeven_applied_positions.add(pos_id)
                            if not is_live:
                                paper_engine.update_position_stops(
                                    symbol=sym,
                                    stop_loss_price=entry_px,
                                )
                            log_msg = f"🛡️ 【移动保本触发】{sym} 浮盈达 {profit_ratio*100:+.2f}%，止损线已上移至开仓均价 ${entry_px} (Breakeven Locked)"
                            logger.info(log_msg)
                            self.last_patrol_log = log_msg

            # 3. 检查最大同时持仓数约束
            active_symbols_to_scan = [s for s in self.symbols if s not in held_symbols]
            if len(held_symbols) >= self.max_concurrent_positions:
                self.last_patrol_log = f"💼 持仓已达上限 ({len(held_symbols)}/{self.max_concurrent_positions})，专注持仓追踪保护"
                return

            if not active_symbols_to_scan:
                self.last_patrol_log = f"💼 所有监控标的均已持有仓位 ({len(held_symbols)} 个)，等待行情演进"
                return

            # 4. 轮巡未持仓标的并评估量化决策
            patrol_summaries = []
            for target_symbol in active_symbols_to_scan:
                if len(held_symbols) >= self.max_concurrent_positions:
                    break

                # 4.1 交易时段 (Session Filter) 检查：美股代币周末休市过滤
                is_tradfi_equity = any(t in target_symbol.upper() for t in ["NVDA", "TSLA", "AAPL", "MSFT", "AMZN", "SPY", "QQQ"])
                now_dt = datetime.utcnow()
                if is_tradfi_equity and now_dt.weekday() in (5, 6):  # 周六周日休市
                    patrol_summaries.append(f"{target_symbol}(美股周末休市)")
                    continue

                # 4.2 提取多周期指标与衍生品情绪
                try:
                    multi_ind = await get_multi_period_indicators_data(target_symbol)
                    deriv = await get_derivatives_metrics_data(target_symbol)
                    cur_tk = ws_manager.latest_tickers.get(target_symbol)
                    ref_px = cur_tk.last_price if (cur_tk and cur_tk.last_price > 0) else paper_engine.last_price_map.get(target_symbol, 100.0)

                    decision = generate_quantitative_rule_decision(
                        symbol=target_symbol,
                        current_price=ref_px,
                        multi_indicators=multi_ind,
                        derivatives=deriv,
                        risk_limits=risk_controller.rules,
                    )
                except Exception as eval_err:
                    logger.debug(f"标的 {target_symbol} 决策推导跳过: {eval_err}")
                    continue

                sig = decision.signal
                plan = decision.trade_plan
                conf = sig.confidence if sig else 0.0
                action = sig.action if sig else SignalAction.HOLD_WAIT

                # 4.3 判定是否满足自动开仓门槛
                if action in (SignalAction.BUY_LONG, SignalAction.SELL_SHORT) and conf >= self.min_confidence and plan:
                    plan_uuid = decision.analysis_id or f"{target_symbol}_{int(now_ms/60000)}"
                    if plan_uuid in self._executed_plan_ids:
                        patrol_summaries.append(f"{target_symbol}(已执行)")
                        continue

                    # 科学仓位核算 (2% 风险预算与可用保证金双重约束)
                    is_short = action == SignalAction.SELL_SHORT
                    side = OrderSide.SELL if is_short else OrderSide.BUY
                    pos_side = PosSide.SHORT if is_short else PosSide.LONG
                    lev = min(plan.suggested_leverage or 3, risk_controller.rules.max_leverage if risk_controller.rules else 5)

                    risk_budget = max(5.0, avail_funds * 0.02)
                    sl_px = plan.stop_loss_price or (ref_px * 0.985 if not is_short else ref_px * 1.015)
                    stop_dist = max(abs(ref_px - sl_px), ref_px * 0.008)
                    calc_coins = risk_budget / stop_dist
                    raw_notional = calc_coins * ref_px

                    # 单笔上限与保证金上限裁剪
                    max_order_cap = min(
                        risk_controller.rules.max_order_usdt if risk_controller.rules else 50000.0,
                        avail_funds * lev * 0.90
                    )
                    final_notional = min(raw_notional, max_order_cap)
                    final_coins = max(0.001, math.floor((final_notional / ref_px) * 1000) / 1000.0)
                    # 安全提取止盈价格 (兼容 take_profit_levels 数组与 take_profit_target_1 标量)
                    tp_target = None
                    if hasattr(plan, "take_profit_levels") and plan.take_profit_levels:
                        tp_target = plan.take_profit_levels[0].price
                    elif hasattr(plan, "take_profit_target_1") and plan.take_profit_target_1:
                        tp_target = plan.take_profit_target_1
                    elif is_short:
                        tp_target = round(ref_px - stop_dist * 2.0, 2)
                    else:
                        tp_target = round(ref_px + stop_dist * 2.0, 2)

                    order_req = PlaceOrderRequest(
                        symbol=target_symbol,
                        inst_type=InstType.SWAP,
                        side=side,
                        pos_side=pos_side,
                        order_type=OrderType.MARKET,
                        size=final_coins,
                        leverage=lev,
                        tp_trigger_price=tp_target,
                        sl_trigger_price=sl_px,
                    )

                    # 硬风控前置拦截校验
                    risk_check = await risk_controller.check_order(order_req, cur_tk, positions)
                    if not risk_check.passed:
                        patrol_summaries.append(f"{target_symbol}(风控拦截:{risk_check.rejected_rule})")
                        continue

                    # 执行下单
                    try:
                        if is_live:
                            await rest_client.place_order(
                                symbol=target_symbol,
                                side=side,
                                pos_side=pos_side,
                                order_type=OrderType.MARKET,
                                sz=final_coins,
                                leverage=lev,
                                tp_trigger_price=tp_target,
                                sl_trigger_price=sl_px,
                            )
                        else:
                            paper_engine.place_order(order_req)

                        self._executed_plan_ids.add(plan_uuid)
                        held_symbols.add(target_symbol)
                        exec_msg = f"🚀 【自动开仓执行】{target_symbol} {pos_side.value.upper()} {final_coins} 币 @ ~${ref_px} (置信度:{conf*100:.0f}%, 止损:${sl_px:.2f})"
                        logger.info(exec_msg)
                        patrol_summaries.append(f"✅{target_symbol}已开仓")
                    except Exception as exec_err:
                        logger.warning(f"自动开仓下单异常 {target_symbol}: {exec_err}")
                        patrol_summaries.append(f"{target_symbol}(下单失败)")
                else:
                    patrol_summaries.append(f"{target_symbol}({action.value if hasattr(action, 'value') else action}:{conf*100:.0f}%)")

            self.last_patrol_log = f"巡检完成: {', '.join(patrol_summaries)}" if patrol_summaries else "无巡检事件"

        except Exception as global_patrol_err:
            logger.warning(f"【自动操盘中枢】巡检循环异常: {global_patrol_err}")
            self.last_patrol_log = f"巡检异常: {global_patrol_err}"
        finally:
            self._is_patrolling = False

    async def record_trade(
        self,
        symbol: str,
        pos_side: str,
        entry_price: float,
        exit_price: float,
        size: float,
        leverage: int,
        realized_pnl: float,
        roi_pct: float,
        reason: str = "AI策略自动平仓",
        order_id: Optional[str] = None,
    ) -> AutoPilotTradeRecord:
        """记录自动操盘产生的成交平仓"""
        async with self._lock:
            now_ms = int(time.time() * 1000)
            record_id = f"apt_{now_ms}_{uuid.uuid4().hex[:4]}"
            trade_obj = AutoPilotTradeRecord(
                id=record_id,
                symbol=symbol,
                pos_side=pos_side,
                entry_price=entry_price,
                exit_price=exit_price,
                size=size,
                leverage=leverage,
                realized_pnl=realized_pnl,
                roi_pct=roi_pct,
                reason=reason,
                timestamp=now_ms,
            )
            self.trades.insert(0, trade_obj)

            # 持久化到 SQLite
            try:
                async with get_db_session() as session:
                    await session.execute(
                        text(
                            """
                            INSERT INTO auto_pilot_trades 
                            (session_id, order_id, symbol, pos_side, entry_price, exit_price, 
                             size, leverage, realized_pnl, roi_pct, reason, timestamp)
                            VALUES (:sess, :oid, :sym, :side, :ep, :xp, :sz, :lev, :rpnl, :roi, :rsn, :ts)
                            """
                        ),
                        {
                            "sess": self.session_id,
                            "oid": order_id or record_id,
                            "sym": symbol,
                            "side": pos_side,
                            "ep": entry_price,
                            "xp": exit_price,
                            "sz": size,
                            "lev": leverage,
                            "rpnl": realized_pnl,
                            "roi": roi_pct,
                            "rsn": reason,
                            "ts": now_ms,
                        },
                    )
            except Exception as e:
                logger.warning(f"【自动操盘中枢】写入自动成交记录失败: {e}")

            logger.info(
                f"【自动操盘战绩】已记录自动平仓: {symbol} {pos_side}, "
                f"已实现盈亏: {'+' if realized_pnl >= 0 else ''}{realized_pnl:.2f} USDT ({roi_pct:+.2f}%)"
            )
            return trade_obj

    async def record_snapshot(
        self,
        current_equity: float,
        realized_pnl: float,
        unrealized_pnl: float,
    ):
        """时序采样记录开启后的资产与收益曲线点 (每 5~15 秒调用一次)"""
        if not self.is_running and len(self.snapshots) > 0:
            return

        async with self._lock:
            now_ms = int(time.time() * 1000)
            self.current_equity = current_equity

            # 动态更新峰值与回撤
            if current_equity > self.peak_equity:
                self.peak_equity = current_equity
            elif self.peak_equity > 0:
                dd = (self.peak_equity - current_equity) / self.peak_equity * 100.0
                if dd > self.max_drawdown_pct:
                    self.max_drawdown_pct = dd

            total_pnl = current_equity - self.start_equity
            pnl_ratio = (total_pnl / self.start_equity) if self.start_equity > 0 else 0.0

            point = AutoPilotHistoryPoint(
                timestamp=now_ms,
                equity=round(current_equity, 2),
                realized_pnl=round(realized_pnl, 2),
                unrealized_pnl=round(unrealized_pnl, 2),
                total_pnl=round(total_pnl, 2),
                pnl_ratio=round(pnl_ratio, 4),
            )

            if len(self.snapshots) >= 500:
                self.snapshots.pop(0)
            self.snapshots.append(point)

            try:
                async with get_db_session() as session:
                    await session.execute(
                        text(
                            """
                            INSERT INTO auto_pilot_pnl_snapshots 
                            (session_id, timestamp, equity, realized_pnl, unrealized_pnl, total_pnl, pnl_ratio)
                            VALUES (:sess, :ts, :eq, :rpnl, :upnl, :tpnl, :ratio)
                            """
                        ),
                        {
                            "sess": self.session_id,
                            "ts": now_ms,
                            "eq": current_equity,
                            "rpnl": realized_pnl,
                            "upnl": unrealized_pnl,
                            "tpnl": total_pnl,
                            "ratio": pnl_ratio,
                        },
                    )
            except Exception:
                pass

    def get_status(
        self,
        current_equity: float,
        unrealized_pnl: float = 0.0,
    ) -> AutoPilotStatusModel:
        """获取当前开启操盘以来的实时收益统计全景与配置"""
        now_ms = int(time.time() * 1000)
        eff_current_equity = current_equity if current_equity > 0 else self.current_equity
        self.current_equity = eff_current_equity

        # 计算运行时长 (秒)
        if self.is_running:
            running_secs = max(0, int((now_ms - self.start_time) / 1000))
        else:
            end_ms = self.stop_time or now_ms
            running_secs = max(0, int((end_ms - self.start_time) / 1000))

        # 计算累计已实现盈亏 (基于自动操盘产生的所有平仓)
        tot_realized = sum(t.realized_pnl for t in self.trades)
        total_pnl = eff_current_equity - self.start_equity
        pnl_ratio = (total_pnl / self.start_equity) if self.start_equity > 0 else 0.0

        # 胜率与盈亏比
        win_trades = [t for t in self.trades if t.realized_pnl > 0]
        loss_trades = [t for t in self.trades if t.realized_pnl < 0]
        tot_trades_cnt = len(self.trades)
        win_cnt = len(win_trades)
        loss_cnt = len(loss_trades)

        win_rate = (win_cnt / tot_trades_cnt) if tot_trades_cnt > 0 else 0.0

        total_gain = sum(t.realized_pnl for t in win_trades)
        total_loss = abs(sum(t.realized_pnl for t in loss_trades))
        profit_factor = (total_gain / total_loss) if total_loss > 0 else (2.5 if total_gain > 0 else 0.0)

        # 动态回撤更新
        if eff_current_equity > self.peak_equity:
            self.peak_equity = eff_current_equity
        elif self.peak_equity > 0:
            dd = (self.peak_equity - eff_current_equity) / self.peak_equity * 100.0
            if dd > self.max_drawdown_pct:
                self.max_drawdown_pct = dd

        return AutoPilotStatusModel(
            is_running=self.is_running,
            session_id=self.session_id,
            start_time=self.start_time,
            running_seconds=running_secs,
            start_equity_usdt=round(self.start_equity, 2),
            current_equity_usdt=round(eff_current_equity, 2),
            total_pnl_usdt=round(total_pnl, 2),
            total_pnl_ratio=round(pnl_ratio, 4),
            realized_pnl_usdt=round(tot_realized, 2),
            unrealized_pnl_usdt=round(unrealized_pnl, 2),
            total_trades_count=tot_trades_cnt,
            winning_trades_count=win_cnt,
            losing_trades_count=loss_cnt,
            win_rate=round(win_rate, 4),
            profit_factor=round(profit_factor, 2),
            max_drawdown_pct=round(self.max_drawdown_pct, 2),
            active_symbol=self.symbol,
            symbols=list(self.symbols),
            max_concurrent_positions=self.max_concurrent_positions,
            trailing_stop_enabled=self.trailing_stop_enabled,
            breakeven_trigger_ratio=self.breakeven_trigger_ratio,
            min_confidence=self.min_confidence,
            auto_leverage_enabled=self.auto_leverage_enabled,
            is_live_mode=self.is_live_mode,
            last_patrol_time=self.last_patrol_time,
            last_patrol_log=self.last_patrol_log,
            last_updated_at=now_ms,
        )

    def get_history(self) -> AutoPilotHistoryResponse:
        """获取收益曲线走势与交易流水"""
        return AutoPilotHistoryResponse(
            session_id=self.session_id,
            is_running=self.is_running,
            curve=list(self.snapshots),
            trades=list(self.trades),
        )

    async def reset_stats(self, current_equity: float) -> AutoPilotStatusModel:
        """重置收益统计基准（将当前资金设为新基准）"""
        return await self.start(
            initial_equity=current_equity,
            is_live_mode=self.is_live_mode,
            symbol=self.symbol,
            symbols=self.symbols,
            max_concurrent_positions=self.max_concurrent_positions,
            trailing_stop_enabled=self.trailing_stop_enabled,
            breakeven_trigger_ratio=self.breakeven_trigger_ratio,
            min_confidence=self.min_confidence,
            auto_leverage_enabled=self.auto_leverage_enabled,
        )


# 单例全局实例
auto_pilot_manager = AutoPilotManager()
