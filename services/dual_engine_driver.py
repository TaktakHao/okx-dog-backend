"""
OKX-Dog 个人量化智能交易系统 - Stage 3A 冻结参数双引擎影子盘/实盘统一驱动器
模块: okx-dog-backend/services/dual_engine_driver.py
角色: 后端架构师 / AI 与量化算法工程师

核心职责:
1. 100% 复用 Gate 2A 验真通过的同构策略类 (KeltnerTrendStrategy 4H + DualEMAChandelierStrategy 1D)，
   参数彻底冻结，杜绝运行期任何线上漂移与过拟合；
2. 严格按 K 线闭合事件驱动 (Bar-Close Gating): 仅在新一根已收盘 4H / 1D K 线到达时调用 strategy.on_bar()，
   并同步以宽幅 ATR 吊灯止损单向抬升/压低止损线，彻底消除 15 秒轮询重复发单；
3. 严守 60/30/10 PortfolioAllocator 资金槽位硬隔离与 RiskController 硬风控前置拦截；
4. 同步周期性巡检底座引擎 A (FundingCarryService) 的 72h 费率机会与熊市自动休眠状态，
   在 SEMI_AUTO 模式下通过 Telegram 推送待确认提案，绝不擅自切入实盘。
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from config import settings
from core.strategy_base import (
    BacktestContext,
    BarData,
    BaseStrategy,
    DualEMAChandelierStrategy,
    KeltnerTrendStrategy,
)
from models import (
    InstType,
    OrderSide,
    OrderType,
    PlaceOrderRequest,
    PosSide,
    SignalAction,
)
from services.position_sizer import position_sizer

logger = logging.getLogger("okx_dog.dual_engine_driver")

# Gate 2A 验真冻结参数基线 (Stage 3A 严禁运行期动态篡改)
FROZEN_4H_KELTNER_PARAMS: Dict[str, Any] = {
    "ema_n": 20,
    "band_k": 2.5,
    "sl_atr_mult": 3.0,
    "trend_ema": 200,
    "risk_pct": 0.01,
}
FROZEN_1D_DUAL_EMA_PARAMS: Dict[str, Any] = {
    "fast_n": 20,
    "slow_n": 50,
    "sl_atr_mult": 2.5,
    "risk_pct": 0.015,
}


class DualEngineShadowDriver:
    """双引擎冻结参数影子盘/实盘统一巡检驱动器"""

    def __init__(
        self,
        rest_client,
        paper_engine,
        risk_controller,
        portfolio_allocator,
        funding_carry_service,
    ):
        self.rest_client = rest_client
        self.paper_engine = paper_engine
        self.risk_controller = risk_controller
        self.portfolio_allocator = portfolio_allocator
        self.funding_carry_service = funding_carry_service

        # 每个 (sleeve, symbol) 独立的同构策略实例、已处理最新收盘 K 线时间戳与持仓峰值追踪
        self._strategies: Dict[Tuple[str, str], BaseStrategy] = {}
        self._warmed_up: Dict[Tuple[str, str], bool] = {}
        self._last_closed_ts: Dict[Tuple[str, str], int] = {}
        self._last_fetch_sec: Dict[Tuple[str, str], float] = {}
        self._trailing_peaks: Dict[str, Dict[str, float]] = {}  # symbol -> {"peak": x, "trough": y, "sl_mult": m}
        self._last_carry_scan_sec: float = 0.0
        self.last_carry_overview: Optional[Dict[str, Any]] = None
        self.last_patrol_summary: str = "双引擎影子盘就绪 (4H 凯尔特纳 + 1D 双均线 + 期现套利休眠守护)"

    def _get_or_create_strategy(self, sleeve: str, symbol: str) -> BaseStrategy:
        key = (sleeve, symbol)
        if key not in self._strategies:
            if sleeve == "ENGINE_B_TREND_4H":
                self._strategies[key] = KeltnerTrendStrategy(params=dict(FROZEN_4H_KELTNER_PARAMS))
            else:
                self._strategies[key] = DualEMAChandelierStrategy(params=dict(FROZEN_1D_DUAL_EMA_PARAMS))
        return self._strategies[key]

    async def _fetch_closed_bars(self, symbol: str, timeframe: str, limit: int = 240) -> List[BarData]:
        """从 OKX 实盘公共接口拉取已确认收盘的 4H / 1D K 线 (is_closed=True)"""
        bar_str = "4H" if timeframe == "4h" else "1Dutc"
        candles = await self.rest_client.get_candles(symbol=symbol, bar=bar_str, limit=limit)
        closed_bars: List[BarData] = []
        for c in candles:
            if getattr(c, "is_closed", True):
                closed_bars.append(BarData(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=int(c.timestamp),
                    open=float(c.open),
                    high=float(c.high),
                    low=float(c.low),
                    close=float(c.close),
                    volume=float(c.volume),
                ))
        return closed_bars

    async def run_patrol_step(
        self,
        is_live: bool,
        symbols: Optional[List[str]] = None,
        initial_equity: float = 10000.0,
    ) -> str:
        """
        执行一次双引擎冻结参数巡检：
        1. 校验 60/30/10 资金槽位与 18% 全局回撤 / 3% 日亏硬熔断；
        2. 对 Top 5 标的分别推进 4H Keltner 与 1D Dual-EMA 闭合 K 线研判与宽幅吊灯止损维护；
        3. 每 30 分钟巡检一次引擎 A 期现费率机会 (低于 10% 自动保持休眠吃 USDT 活期)。
        """
        # 严格过滤合成/代币化美股标的
        raw_syms = symbols or list(settings.CORE_TRADING_UNIVERSE)
        target_syms = [
            s.upper() for s in raw_syms
            if not any(s.upper().startswith(p) for p in settings.BLOCKED_SYNTHETIC_PREFIXES)
        ]
        if not target_syms:
            target_syms = list(settings.CORE_TRADING_UNIVERSE)

        # 获取当前账户净值与持仓
        if is_live:
            bal = await self.rest_client.get_account_balance()
            positions = await self.rest_client.get_positions(inst_type="SWAP")
        else:
            bal = self.paper_engine.get_account_balance()
            positions = self.paper_engine.get_positions(inst_type="SWAP")

        total_eq = float(bal.total_equity_usdt or 0.0)
        slots_info = self.portfolio_allocator.compute_slot_budgets(
            total_equity_usdt=total_eq,
            initial_equity_usdt=initial_equity,
            today_realized_loss_usdt=self.risk_controller.today_realized_loss,
        )
        if slots_info["is_global_stop_triggered"]:
            self.last_patrol_summary = f"🛑 触发全局 18% 回撤停机底线 (${slots_info['equity_floor_usdt']})"
            return self.last_patrol_summary
        if slots_info["is_daily_breaker_triggered"]:
            self.last_patrol_summary = f"🛑 触发单日 3% 亏损硬熔断 (${slots_info['daily_loss_limit_usdt']})"
            return self.last_patrol_summary

        held_map = {p.symbol.upper(): p for p in positions if p.pos > 0}
        now_sec = time.time()
        events: List[str] = []

        # 按周期遍历双趋势子臂 (4H 凯尔特纳 + 1D 双均线)
        sleeves = [
            ("ENGINE_B_TREND_4H", "4h", 60.0, 0.01),
            ("ENGINE_B_TREND_1D", "1d", 300.0, 0.015),
        ]

        for sleeve_id, tf, min_poll_sec, port_risk_pct in sleeves:
            slot_budget = slots_info["slots"][sleeve_id]["allocated_usdt"]
            for sym in target_syms:
                key = (sleeve_id, sym)
                if (now_sec - self._last_fetch_sec.get(key, 0.0)) < min_poll_sec and self._warmed_up.get(key, False):
                    continue

                try:
                    bars = await self._fetch_closed_bars(sym, tf, limit=240 if not self._warmed_up.get(key) else 5)
                    self._last_fetch_sec[key] = now_sec
                    if not bars:
                        continue
                except Exception as fe:
                    logger.debug(f"[DualEngine] 拉取 {sym} [{tf}] 闭合 K 线跳过: {fe}")
                    continue

                strat = self._get_or_create_strategy(sleeve_id, sym)
                ctx = BacktestContext(symbol=sym, timeframe=tf, initial_equity=slot_budget, leverage=3)

                # 首次冷启动：喂入历史闭合 K 线完成内部 EMA/ATR 预热，不发历史旧单
                if not self._warmed_up.get(key, False):
                    for b in bars:
                        ctx.current_pos_side = None
                        strat.on_bar(b, ctx)
                    self._last_closed_ts[key] = bars[-1].timestamp
                    self._warmed_up[key] = True
                    continue

                # 增量检测新闭合 K 线 (零重复触发)
                last_ts = self._last_closed_ts.get(key, 0)
                new_bars = [b for b in bars if b.timestamp > last_ts]
                if not new_bars:
                    continue

                for nb in new_bars:
                    self._last_closed_ts[key] = nb.timestamp
                    cur_pos = held_map.get(sym)
                    cur_side_str = (
                        cur_pos.pos_side.value.lower()
                        if (cur_pos and hasattr(cur_pos.pos_side, "value"))
                        else (str(cur_pos.pos_side).lower() if cur_pos else None)
                    )
                    ctx.current_pos_side = cur_side_str
                    decision = strat.on_bar(nb, ctx)

                    # 维护现有持仓的宽幅 ATR 吊灯移动止损 (仅向有利方向单向推进)
                    atr_val = getattr(strat, "_atr14", None) or (nb.close * 0.02)
                    if cur_pos is not None and sym in self._trailing_peaks:
                        tpk = self._trailing_peaks[sym]
                        sl_mult = tpk.get("sl_mult", 3.0)
                        if cur_side_str == "long":
                            tpk["peak"] = max(tpk.get("peak", nb.high), nb.high)
                            cand_sl = round(tpk["peak"] - atr_val * sl_mult, 4)
                            if cur_pos.sl_trigger_price is None or cand_sl > cur_pos.sl_trigger_price:
                                if is_live:
                                    await self.rest_client.replace_position_stop_loss_safely(
                                        symbol=sym, pos_side=PosSide.LONG, sz=cur_pos.pos, sl_trigger_price=cand_sl
                                    )
                                else:
                                    self.paper_engine.update_position_stops(symbol=sym, pos_side=PosSide.LONG, stop_loss_price=cand_sl)
                                events.append(f"🛡️{sym}吊灯移损→${cand_sl}")
                        elif cur_side_str == "short":
                            tpk["trough"] = min(tpk.get("trough", nb.low), nb.low)
                            cand_sl = round(tpk["trough"] + atr_val * sl_mult, 4)
                            if cur_pos.sl_trigger_price is None or cand_sl < cur_pos.sl_trigger_price:
                                if is_live:
                                    await self.rest_client.replace_position_stop_loss_safely(
                                        symbol=sym, pos_side=PosSide.SHORT, sz=cur_pos.pos, sl_trigger_price=cand_sl
                                    )
                                else:
                                    self.paper_engine.update_position_stops(symbol=sym, pos_side=PosSide.SHORT, stop_loss_price=cand_sl)
                                events.append(f"🛡️{sym}吊灯移损→${cand_sl}")

                    if decision is None:
                        continue

                    # A. 趋势结束主动平仓
                    if decision.action == SignalAction.CLOSE_POSITION and cur_pos is not None:
                        if is_live:
                            await self.rest_client.close_position(symbol=sym, pos_side=cur_pos.pos_side)
                        else:
                            self.paper_engine.close_position(symbol=sym, pos_side=cur_pos.pos_side, price=nb.close, reason=decision.reason)
                        self._trailing_peaks.pop(sym, None)
                        held_map.pop(sym, None)
                        events.append(f"🏁{sym}[{tf}]信号平仓")
                        continue

                    # B. 空仓顺势突破入场 (严格前置校验槽位隔离 + 硬风控)
                    if cur_pos is None and decision.action in (SignalAction.BUY_LONG, SignalAction.SELL_SHORT):
                        is_short = (decision.action == SignalAction.SELL_SHORT)
                        sizer = position_sizer.calculate_adaptive_size(
                            symbol=sym,
                            equity=slot_budget,
                            entry_price=nb.close,
                            is_short=is_short,
                            stop_loss_price=decision.stop_loss_price,
                            atr=atr_val,
                            atr_multiplier=decision.trailing_atr_mult or 3.0,
                            risk_pct=port_risk_pct,
                            max_leverage=3,
                            max_order_usdt=min(slot_budget * 0.9, self.risk_controller.rules.max_order_usdt),
                            timeframe=tf,
                            silent=True,
                        )
                        if sizer.calculated_size <= 0 or sizer.notional_value_usdt < 10.0:
                            continue

                        alloc_chk = self.portfolio_allocator.can_engine_open(
                            engine_id=sleeve_id,
                            required_margin_usdt=sizer.required_margin_usdt,
                            total_equity_usdt=total_eq,
                            initial_equity_usdt=initial_equity,
                            today_realized_loss_usdt=self.risk_controller.today_realized_loss,
                        )
                        if not alloc_chk["allowed"]:
                            events.append(f"⛔{sym}[{tf}]槽位受限")
                            continue

                        req = PlaceOrderRequest(
                            symbol=sym,
                            inst_type=InstType.SWAP,
                            side=OrderSide.SELL if is_short else OrderSide.BUY,
                            pos_side=PosSide.SHORT if is_short else PosSide.LONG,
                            order_type=OrderType.MARKET if not is_live else OrderType.POST_ONLY,
                            sz=sizer.calculated_size,
                            leverage=3,
                            sl_trigger_price=sizer.stop_loss_price,
                            tp_trigger_price=None,
                        )
                        risk_res = await self.risk_controller.check_order(req, current_positions=positions)
                        if not risk_res.passed:
                            events.append(f"🛡️{sym}[{tf}]风控拦截({risk_res.rejected_rule})")
                            continue

                        if is_live:
                            await self.rest_client.place_order(
                                symbol=sym,
                                side=req.side,
                                pos_side=req.pos_side,
                                order_type=OrderType.MARKET,
                                sz=req.sz,
                                leverage=3,
                                sl_trigger_price=req.sl_trigger_price,
                            )
                        else:
                            self.paper_engine.place_order(req, current_market_price=nb.close)

                        self._trailing_peaks[sym] = {
                            "peak": nb.close,
                            "trough": nb.close,
                            "sl_mult": decision.trailing_atr_mult or 3.0,
                        }
                        events.append(f"🚀{sym}[{tf}]{'开空' if is_short else '开多'}@{nb.close:.2f}")

        # 同步各趋势子臂保证金占用至组合分配器
        total_trend_margin = sum(float(p.margin or 0.0) for p in held_map.values())
        self.portfolio_allocator.update_engine_used_margin("ENGINE_B_TREND_4H", total_trend_margin * 0.5)
        self.portfolio_allocator.update_engine_used_margin("ENGINE_B_TREND_1D", total_trend_margin * 0.5)

        # 每 30 分钟巡检一次底座引擎 A (期现费率套利)
        if (now_sec - self._last_carry_scan_sec) >= 1800.0:
            self._last_carry_scan_sec = now_sec
            try:
                self.last_carry_overview = await self.funding_carry_service.scan_live_opportunities(target_syms)
                if self.last_carry_overview.get("overall_state") == "ACTIVE_OPPORTUNITY":
                    for op in self.last_carry_overview.get("opportunities", []):
                        if op["recommended_action"] == "ENTER_CARRY" and not self.funding_carry_service.pending_proposals:
                            carry_budget = slots_info["slots"]["ENGINE_A_CARRY"]["available_usdt"]
                            if carry_budget >= 500.0:
                                await self.funding_carry_service.create_or_execute_proposal(
                                    swap_symbol=op["swap_symbol"],
                                    action="OPEN_CARRY",
                                    notional_usdt=min(carry_budget, 1500.0),
                                    total_equity_usdt=total_eq,
                                    is_paper=(not is_live),
                                    reason=f"72h 年化费率达 {op['rolling_72h_ann_pct']}%，基差 {op['basis_bps']}bps",
                                )
                                events.append(f"💼引擎A套利提案({op['swap_symbol']})")
                                break
            except Exception as ce:
                logger.debug(f"[DualEngine] 引擎 A 费率巡检跳过: {ce}")

        carry_state = (self.last_carry_overview or {}).get("overall_state", "HIBERNATING_USDT_YIELD")
        if events:
            self.last_patrol_summary = f"❄️双引擎冻结运行 | {' | '.join(events[:4])} | 套利={carry_state}"
        else:
            self.last_patrol_summary = (
                f"❄️双引擎影子盘巡检完成 (Top5 · 4H凯尔特纳+1D双均线 · 持仓{len(held_map)}笔 · 套利={carry_state})"
            )
        return self.last_patrol_summary
