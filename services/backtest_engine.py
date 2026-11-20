"""
OKX-Dog 个人量化智能交易系统 - 高保真事件驱动回测、组合回测与 Walk-Forward 样本外验证中枢
模块: okx-dog-backend/services/backtest_engine.py
角色: 后端架构师 / AI 与量化算法工程师

核心能力:
1. 三位一体同构驱动: 直接调用 core/strategy_base.py 注册的 BaseStrategy 子类，确保回测与实盘 100% 逻辑一致；
2. 零前瞻高保真撮合: Next-Bar 开盘成交、单根 Bar 阴阳子路径止损/止盈推导、宽幅 ATR 吊灯移动止损、真实历史 8h 资金费率结算；
3. 零误差全口径记账: 每笔净盈亏完整扣除开仓费、平仓费、滑点与已结算资金费，账户权益变动与逐笔净盈亏严格相等；
4. 机构级评价体系: 日频重采样年化夏普 (sqrt(365))、CAGR、最大回撤、Calmar、Top-3 剔除保留率、手续费/毛利比、40bps 滑点压测与 ±20% 参数平原校验；
5. 多标的组合回测与滚动样本外 Walk-Forward 门禁，结果自动持久化至 backtest_experiments 表。
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import math
import os
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text
from database import get_db_session
from models import (
    BacktestEquityPoint,
    BacktestResultResponse,
    BacktestRunRequest,
    BacktestTradeRecord,
    GridSearchRequest,
    GridSearchResponse,
    GridSearchResultItem,
    SignalAction,
)
from services.data_lake_service import DataLakeService
from services.position_sizer import position_sizer

logger = logging.getLogger("okx_dog.backtest")


def _load_plateau_validator():
    """按需加载 okx-dog-ai 中的参数平原与生死线检验器"""
    ai_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "okx-dog-ai"))
    if ai_dir not in sys.path:
        sys.path.append(ai_dir)
    from strategy_plateau_validator import StrategyPlateauValidator
    return StrategyPlateauValidator


class BacktestEngine:
    """金融级同构事件驱动回测与 Walk-Forward 检验中枢"""

    def __init__(self, data_lake: DataLakeService):
        self.data_lake = data_lake

    @staticmethod
    def _compute_indicators(candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """纯 Python 毫秒级增量计算 EMA20/50/200, Wilder ATR14, RSI14"""
        if not candles:
            return []

        out: List[Dict[str, Any]] = []
        first_close = float(candles[0]["close"])
        ema20 = first_close
        ema50 = first_close
        ema200 = first_close
        k20 = 2.0 / 21.0
        k50 = 2.0 / 51.0
        k200 = 2.0 / 201.0

        tr_seed: List[float] = []
        atr14: Optional[float] = None
        gains: List[float] = []
        losses: List[float] = []
        prev_close = first_close

        for i, c in enumerate(candles):
            close_p = float(c["close"])
            high_p = float(c["high"])
            low_p = float(c["low"])

            ema20 = close_p * k20 + ema20 * (1.0 - k20)
            ema50 = close_p * k50 + ema50 * (1.0 - k50)
            ema200 = close_p * k200 + ema200 * (1.0 - k200)

            tr = (high_p - low_p) if i == 0 else max(
                high_p - low_p,
                abs(high_p - prev_close),
                abs(low_p - prev_close),
            )
            if atr14 is None:
                tr_seed.append(tr)
                if len(tr_seed) >= 14:
                    atr14 = sum(tr_seed) / len(tr_seed)
                cur_atr = sum(tr_seed) / len(tr_seed)
            else:
                atr14 = (atr14 * 13.0 + tr) / 14.0
                cur_atr = atr14

            diff = close_p - prev_close
            gains.append(max(0.0, diff))
            losses.append(max(0.0, -diff))
            if len(gains) > 14:
                gains.pop(0)
                losses.pop(0)
            avg_gain = sum(gains) / len(gains) if gains else 0.0
            avg_loss = sum(losses) / len(losses) if losses else 1e-9
            rs = avg_gain / (avg_loss + 1e-9)
            rsi14 = 100.0 - (100.0 / (1.0 + rs))

            item = dict(c)
            item["ema20"] = ema20
            item["ema50"] = ema50
            item["ema200"] = ema200
            item["atr14"] = cur_atr if cur_atr > 0 else (close_p * 0.015)
            item["rsi14"] = rsi14
            out.append(item)
            prev_close = close_p

        return out

    @staticmethod
    def _compute_daily_sharpe(daily_equity: Dict[str, float]) -> float:
        """基于日频盯市净值曲线计算加密 7x24 年化夏普比率 (sqrt(365))"""
        if len(daily_equity) < 3:
            return 0.0
        dates = sorted(daily_equity.keys())
        returns: List[float] = []
        for idx in range(1, len(dates)):
            prev_eq = daily_equity[dates[idx - 1]]
            cur_eq = daily_equity[dates[idx]]
            if prev_eq > 0:
                returns.append((cur_eq - prev_eq) / prev_eq)
        if len(returns) < 2:
            return 0.0
        mean_r = sum(returns) / len(returns)
        var_r = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        std_r = math.sqrt(var_r)
        if std_r <= 1e-12:
            return 0.0
        return round((mean_r / std_r) * math.sqrt(365.0), 3)

    def simulate_bars(
        self,
        df: List[Dict[str, Any]],
        symbol: str,
        timeframe: str,
        strategy_name: str,
        params: Optional[Dict[str, Any]] = None,
        initial_balance: float = 10000.0,
        leverage: int = 3,
        taker_fee: float = 0.0005,
        maker_fee: float = 0.0002,
        slippage: float = 0.0002,
        atr_mult: float = 2.0,
        breakeven_ratio: float = 0.5,
        default_funding_rate_8h: float = 0.0001,
        risk_pct_override: Optional[float] = None,
        eval_start_ts: Optional[int] = None,
        eval_end_ts: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        核心纯内存事件驱动单标的回测执行器 (支持指定 eval_start_ts 预热窗口，供 Walk-Forward 零缝衔接复用)
        """
        equity = float(initial_balance)
        peak_equity = equity
        max_drawdown_pct = 0.0

        trades: List[BacktestTradeRecord] = []
        trades_notional: List[float] = []
        equity_curve: List[BacktestEquityPoint] = []
        daily_equity: Dict[str, float] = {}

        position: Optional[Dict[str, Any]] = None
        pending_entry: Optional[Dict[str, Any]] = None
        pending_close_reason: Optional[str] = None
        last_funding_window: int = -1
        trade_counter = 0
        total_fee_paid = 0.0
        total_funding_cost = 0.0

        from core.strategy_base import BacktestContext, BarData, BaseStrategy, create_strategy

        strategy: BaseStrategy = create_strategy(strategy_name, params or {})
        ctx = BacktestContext(
            symbol=symbol,
            timeframe=timeframe,
            initial_equity=initial_balance,
            leverage=leverage,
        )
        strategy.on_init(ctx)

        def _close_position_record(
            pos: Dict[str, Any],
            raw_exit_px: float,
            ts_ms: int,
            exit_reason: str,
            use_maker: bool = False,
        ) -> None:
            nonlocal equity, total_fee_paid, trade_counter
            pos_side = pos["pos_side"]
            entry_px = pos["entry_price"]
            size = pos["size"]
            slip = 0.0 if use_maker else slippage
            exit_px = raw_exit_px * (1.0 - slip) if pos_side == "long" else raw_exit_px * (1.0 + slip)
            gross_pnl = (exit_px - entry_px) * size if pos_side == "long" else (entry_px - exit_px) * size
            close_fee_rate = maker_fee if use_maker else taker_fee
            close_fee = exit_px * size * close_fee_rate

            # 现金只需结算价差盈亏 - 平仓费 (开仓费与资金费已在发生时扣减/入账)
            equity += (gross_pnl - close_fee)
            total_fee_paid += close_fee

            open_fee = pos.get("open_fee", 0.0)
            funding_paid = pos.get("funding_fee_paid", 0.0)
            # 单笔全口径净盈亏 == 账户真实净权益变动
            net_pnl = gross_pnl - close_fee - open_fee - funding_paid
            margin = max(pos.get("margin", 1e-6), 1e-6)

            trade_counter += 1
            trades.append(BacktestTradeRecord(
                trade_id=trade_counter,
                entry_time=pos["entry_time"],
                exit_time=ts_ms,
                pos_side=pos_side,
                entry_price=round(entry_px, 6),
                exit_price=round(exit_px, 6),
                size=round(size, 6),
                pnl=round(net_pnl, 4),
                roi_pct=round((net_pnl / margin) * 100.0, 2),
                fee=round(open_fee + close_fee, 4),
                exit_reason=exit_reason,
                mfe_pct=round(pos.get("mfe_pct", 0.0), 2),
                mae_pct=round(pos.get("mae_pct", 0.0), 2),
            ))
            trades_notional.append(pos.get("notional", entry_px * size) + exit_px * size)

        eval_started = eval_start_ts is None
        first_eval_ts: Optional[int] = None
        last_eval_ts: Optional[int] = None

        for i in range(len(df)):
            row = df[i]
            ts = int(row["timestamp"])
            if eval_end_ts is not None and ts > eval_end_ts:
                break

            open_px = float(row.get("open", row["close"]))
            close_px = float(row["close"])
            high_px = float(row["high"])
            low_px = float(row["low"])
            atr = float(row["atr14"])
            bar_funding_rate = float(row.get("funding_rate") or 0.0)
            eff_funding_rate = bar_funding_rate if bar_funding_rate != 0.0 else default_funding_rate_8h
            is_bullish = (close_px >= open_px)

            in_eval_window = (eval_start_ts is None or ts >= eval_start_ts)
            if in_eval_window and not eval_started:
                eval_started = True
                equity = float(initial_balance)
                peak_equity = equity
                max_drawdown_pct = 0.0
                position = None
                pending_entry = None
                pending_close_reason = None

            if in_eval_window:
                if first_eval_ts is None:
                    first_eval_ts = ts
                last_eval_ts = ts

            # 1. 真实 8 小时资金费率结算 (跨越 UTC 00:00/08:00/16:00 窗口，采用该时刻真实历史费率)
            funding_window = ts // (8 * 3600 * 1000)
            if in_eval_window and position is not None:
                if last_funding_window != -1 and funding_window > last_funding_window:
                    windows_crossed = funding_window - last_funding_window
                    cur_notional = position["size"] * close_px
                    if position["pos_side"] == "long":
                        f_cost = cur_notional * eff_funding_rate * windows_crossed
                    else:
                        f_cost = -cur_notional * eff_funding_rate * windows_crossed
                    equity -= f_cost
                    total_funding_cost += f_cost
                    position["funding_fee_paid"] = position.get("funding_fee_paid", 0.0) + f_cost
            last_funding_window = funding_window

            # 2. Next-Bar 开盘执行上一根 Bar 产生的平仓/开仓信号 (严格零 Look-ahead)
            if in_eval_window and pending_close_reason and position is not None:
                _close_position_record(position, open_px, ts, pending_close_reason, use_maker=False)
                position = None
                pending_close_reason = None

            if in_eval_window and pending_entry and position is None and equity > 100.0:
                sig_side = pending_entry["pos_side"]
                sig_atr = pending_entry["atr"]
                eff_atr_mult = pending_entry.get("trailing_atr_mult") or atr_mult
                eff_risk_pct = risk_pct_override if risk_pct_override is not None else (pending_entry.get("risk_pct") or 0.01)
                use_maker_entry = bool(pending_entry.get("use_maker", False))
                entry_slip = 0.0 if use_maker_entry else slippage
                entry_fee_rate = maker_fee if use_maker_entry else taker_fee

                if sig_side == "long":
                    entry_px = open_px * (1.0 + entry_slip)
                    sl_dist = sig_atr * eff_atr_mult
                    sl_px = max(1e-6, entry_px - sl_dist)
                    tp_px = None if pending_entry.get("disable_fixed_tp") else (entry_px + sl_dist * 2.0)
                else:
                    entry_px = open_px * (1.0 - entry_slip)
                    sl_dist = sig_atr * eff_atr_mult
                    sl_px = entry_px + sl_dist
                    tp_px = None if pending_entry.get("disable_fixed_tp") else max(1e-6, entry_px - sl_dist * 2.0)

                sizer_res = position_sizer.calculate_adaptive_size(
                    symbol=symbol,
                    equity=equity,
                    entry_price=entry_px,
                    is_short=(sig_side == "short"),
                    stop_loss_price=sl_px,
                    atr=sig_atr,
                    atr_multiplier=eff_atr_mult,
                    risk_pct=eff_risk_pct,
                    max_leverage=leverage,
                    max_order_usdt=equity * leverage * 0.95,
                    timeframe=timeframe,
                    silent=True,
                )
                size = sizer_res.calculated_size
                notional = size * entry_px
                order_margin = notional / max(1, leverage)
                if size > 0 and notional > 5.0:
                    open_fee = notional * entry_fee_rate
                    equity -= open_fee
                    total_fee_paid += open_fee
                    position = {
                        "pos_side": sig_side,
                        "entry_price": entry_px,
                        "sl_price": sl_px,
                        "tp_price": tp_px,
                        "trailing_atr_mult": pending_entry.get("trailing_atr_mult"),
                        "disable_breakeven": bool(pending_entry.get("disable_breakeven", False)),
                        "peak_high": max(entry_px, open_px),
                        "trough_low": min(entry_px, open_px),
                        "size": size,
                        "margin": order_margin,
                        "notional": notional,
                        "open_fee": open_fee,
                        "entry_time": ts,
                        "mfe_pct": 0.0,
                        "mae_pct": 0.0,
                        "breakeven_locked": False,
                        "funding_fee_paid": 0.0,
                    }
                pending_entry = None

            # 3. 持仓内止损 / 宽幅 ATR 吊灯止损 / 固定止盈巡检
            if in_eval_window and position is not None:
                pos_side = position["pos_side"]
                entry_px = position["entry_price"]
                sl_px = position["sl_price"]
                tp_px = position["tp_price"]
                trail_mult = position.get("trailing_atr_mult")

                if pos_side == "long":
                    position["mfe_pct"] = max(position["mfe_pct"], (high_px - entry_px) / entry_px * 100.0)
                    position["mae_pct"] = min(position["mae_pct"], (low_px - entry_px) / entry_px * 100.0)

                    # 固定止盈模式下的保本触发
                    if tp_px is not None and not position["disable_breakeven"] and not position["breakeven_locked"]:
                        if (high_px - entry_px) >= (tp_px - entry_px) * breakeven_ratio:
                            position["sl_price"] = max(sl_px, entry_px * (1.0 + taker_fee * 2.0))
                            position["breakeven_locked"] = True
                            sl_px = position["sl_price"]

                    sl_hit = (low_px <= sl_px)
                    tp_hit = (tp_px is not None and high_px >= tp_px)
                    if sl_hit and tp_hit:
                        final_exit = "STOP_LOSS" if is_bullish else "TAKE_PROFIT"
                    elif sl_hit:
                        final_exit = "STOP_LOSS"
                    elif tp_hit:
                        final_exit = "TAKE_PROFIT"
                    else:
                        final_exit = None

                    if final_exit == "STOP_LOSS":
                        # 真实跳空处理：若开盘价已穿透止损，按开盘价成交，杜绝虚构止损价
                        exec_ref = min(open_px, sl_px) if open_px < sl_px else sl_px
                        reason = "TRAILING_SL" if trail_mult else ("BREAKEVEN" if position["breakeven_locked"] else "STOP_LOSS")
                        _close_position_record(position, exec_ref, ts, reason, use_maker=False)
                        position = None
                    elif final_exit == "TAKE_PROFIT" and tp_px is not None:
                        _close_position_record(position, tp_px, ts, "TAKE_PROFIT", use_maker=False)
                        position = None
                    elif trail_mult:
                        # 收盘后更新吊灯移动止损 (仅向有利方向上移，严禁当根回看)
                        position["peak_high"] = max(position["peak_high"], high_px)
                        new_sl = position["peak_high"] - atr * trail_mult
                        if new_sl > position["sl_price"]:
                            position["sl_price"] = new_sl

                else:
                    position["mfe_pct"] = max(position["mfe_pct"], (entry_px - low_px) / entry_px * 100.0)
                    position["mae_pct"] = min(position["mae_pct"], (entry_px - high_px) / entry_px * 100.0)

                    if tp_px is not None and not position["disable_breakeven"] and not position["breakeven_locked"]:
                        if (entry_px - low_px) >= (entry_px - tp_px) * breakeven_ratio:
                            position["sl_price"] = min(sl_px, entry_px * (1.0 - taker_fee * 2.0))
                            position["breakeven_locked"] = True
                            sl_px = position["sl_price"]

                    sl_hit = (high_px >= sl_px)
                    tp_hit = (tp_px is not None and low_px <= tp_px)
                    if sl_hit and tp_hit:
                        final_exit = "STOP_LOSS" if (not is_bullish) else "TAKE_PROFIT"
                    elif sl_hit:
                        final_exit = "STOP_LOSS"
                    elif tp_hit:
                        final_exit = "TAKE_PROFIT"
                    else:
                        final_exit = None

                    if final_exit == "STOP_LOSS":
                        exec_ref = max(open_px, sl_px) if open_px > sl_px else sl_px
                        reason = "TRAILING_SL" if trail_mult else ("BREAKEVEN" if position["breakeven_locked"] else "STOP_LOSS")
                        _close_position_record(position, exec_ref, ts, reason, use_maker=False)
                        position = None
                    elif final_exit == "TAKE_PROFIT" and tp_px is not None:
                        _close_position_record(position, tp_px, ts, "TAKE_PROFIT", use_maker=False)
                        position = None
                    elif trail_mult:
                        position["trough_low"] = min(position["trough_low"], low_px)
                        new_sl = position["trough_low"] + atr * trail_mult
                        if new_sl < position["sl_price"]:
                            position["sl_price"] = new_sl

            # 4. 逐根喂给 BaseStrategy (包括预热期，让内部滚动指标完整收敛)
            ctx._equity = equity
            ctx.current_pos_side = position["pos_side"] if position is not None else None
            bar_obj = BarData(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=ts,
                open=open_px,
                high=high_px,
                low=low_px,
                close=close_px,
                volume=float(row.get("volume", 0.0)),
                funding_rate=eff_funding_rate,
                indicators={
                    "ema20": row["ema20"],
                    "ema50": row["ema50"],
                    "ema200": row["ema200"],
                    "rsi14": row["rsi14"],
                    "atr14": atr,
                },
            )
            strat_decision = strategy.on_bar(bar_obj, ctx)
            if in_eval_window and strat_decision is not None:
                if strat_decision.action == SignalAction.CLOSE_POSITION and position is not None:
                    pending_close_reason = "SIGNAL_EXIT"
                elif position is None and pending_entry is None:
                    if strat_decision.action == SignalAction.BUY_LONG:
                        pending_entry = {
                            "pos_side": "long",
                            "atr": atr,
                            "trailing_atr_mult": strat_decision.trailing_atr_mult,
                            "risk_pct": strat_decision.risk_pct,
                            "disable_fixed_tp": strat_decision.disable_fixed_tp,
                            "disable_breakeven": strat_decision.disable_breakeven,
                            "use_maker": False,  # 保守假设回测按 Taker+滑点压测
                        }
                    elif strat_decision.action == SignalAction.SELL_SHORT:
                        pending_entry = {
                            "pos_side": "short",
                            "atr": atr,
                            "trailing_atr_mult": strat_decision.trailing_atr_mult,
                            "risk_pct": strat_decision.risk_pct,
                            "disable_fixed_tp": strat_decision.disable_fixed_tp,
                            "disable_breakeven": strat_decision.disable_breakeven,
                            "use_maker": False,
                        }

            # 5. 计算实时盯市权益 (MTM Equity) 并采样日频净值曲线
            if in_eval_window:
                upl = 0.0
                if position is not None:
                    if position["pos_side"] == "long":
                        upl = (close_px - position["entry_price"]) * position["size"]
                    else:
                        upl = (position["entry_price"] - close_px) * position["size"]
                mtm_equity = max(0.0, equity + upl)
                peak_equity = max(peak_equity, mtm_equity)
                curr_dd = (peak_equity - mtm_equity) / peak_equity * 100.0 if peak_equity > 0 else 0.0
                max_drawdown_pct = max(max_drawdown_pct, curr_dd)

                day_key = datetime.datetime.fromtimestamp(ts / 1000.0, tz=datetime.timezone.utc).strftime("%Y-%m-%d")
                daily_equity[day_key] = mtm_equity

                if i % 5 == 0 or i == len(df) - 1:
                    equity_curve.append(BacktestEquityPoint(
                        timestamp=ts,
                        equity=round(mtm_equity, 2),
                        drawdown_pct=round(curr_dd, 2),
                    ))

        # 评估窗口结束时若有未平持仓，按最后一根收盘价平仓结算，避免悬空浮盈失真
        if position is not None and last_eval_ts is not None:
            last_row = df[-1]
            for r in reversed(df):
                if int(r["timestamp"]) <= last_eval_ts:
                    last_row = r
                    break
            _close_position_record(position, float(last_row["close"]), int(last_row["timestamp"]), "EOD_SETTLE", use_maker=False)
            day_key = datetime.datetime.fromtimestamp(int(last_row["timestamp"]) / 1000.0, tz=datetime.timezone.utc).strftime("%Y-%m-%d")
            daily_equity[day_key] = equity

        total_trades = len(trades)
        winning_trades = len([t for t in trades if t.pnl > 0])
        losing_trades = len([t for t in trades if t.pnl <= 0])
        win_rate = round(winning_trades / total_trades, 4) if total_trades > 0 else 0.0
        total_gain = sum(t.pnl for t in trades if t.pnl > 0)
        total_loss = abs(sum(t.pnl for t in trades if t.pnl < 0))
        profit_factor = round(total_gain / total_loss, 3) if total_loss > 1e-9 else (9.99 if total_gain > 0 else 0.0)

        total_return_pct = round((equity - initial_balance) / initial_balance * 100.0, 2)
        actual_days = max(1.0, ((last_eval_ts - first_eval_ts) / (86400.0 * 1000.0)) if (first_eval_ts and last_eval_ts and last_eval_ts > first_eval_ts) else 1.0)
        if equity > 0 and initial_balance > 0:
            cagr_pct = round((((equity / initial_balance) ** (365.0 / actual_days)) - 1.0) * 100.0, 2)
        else:
            cagr_pct = -100.0

        sharpe_ratio = self._compute_daily_sharpe(daily_equity)
        calmar_ratio = round(cagr_pct / max(max_drawdown_pct, 0.1), 2) if cagr_pct > 0 else round(cagr_pct / max(max_drawdown_pct, 1.0), 2)
        avg_trade_pnl = round(sum(t.pnl for t in trades) / total_trades, 2) if total_trades > 0 else 0.0

        return {
            "initial_balance": initial_balance,
            "final_balance": round(equity, 2),
            "total_return_pct": total_return_pct,
            "annualized_return_pct": cagr_pct,
            "max_drawdown_pct": round(max_drawdown_pct, 2),
            "sharpe_ratio": sharpe_ratio,
            "calmar_ratio": calmar_ratio,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "avg_trade_pnl": avg_trade_pnl,
            "total_fee_paid": round(total_fee_paid, 2),
            "total_funding_cost": round(total_funding_cost, 2),
            "trades": trades,
            "trades_notional": trades_notional,
            "equity_curve": equity_curve,
            "daily_equity": daily_equity,
            "start_time": first_eval_ts or (int(df[0]["timestamp"]) if df else 0),
            "end_time": last_eval_ts or (int(df[-1]["timestamp"]) if df else 0),
            "total_candles": len(df),
        }

    async def _persist_experiment(
        self,
        experiment_id: str,
        strategy_name: str,
        symbol: str,
        timeframe: str,
        start_time: int,
        end_time: int,
        parameters: Dict[str, Any],
        metrics: Dict[str, Any],
        detail: Dict[str, Any],
    ) -> None:
        """将回测 / 组合 / Walk-Forward 实验结果落库至 backtest_experiments 表"""
        try:
            async with get_db_session() as db:
                sql = text("""
                    INSERT INTO backtest_experiments (
                        id, strategy_name, symbol, timeframe, start_time, end_time,
                        parameters, total_return_pct, sharpe_ratio, max_drawdown_pct,
                        win_rate, profit_factor, total_trades, metrics_detail, created_at
                    ) VALUES (
                        :id, :strategy_name, :symbol, :timeframe, :start_time, :end_time,
                        :parameters, :total_return_pct, :sharpe_ratio, :max_drawdown_pct,
                        :win_rate, :profit_factor, :total_trades, :metrics_detail, CURRENT_TIMESTAMP
                    )
                """)
                await db.execute(sql, {
                    "id": experiment_id,
                    "strategy_name": strategy_name,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start_time": int(start_time),
                    "end_time": int(end_time),
                    "parameters": json.dumps(parameters, ensure_ascii=False),
                    "total_return_pct": float(metrics.get("total_return_pct", 0.0)),
                    "sharpe_ratio": float(metrics.get("sharpe_ratio", 0.0)),
                    "max_drawdown_pct": float(metrics.get("max_drawdown_pct", 0.0)),
                    "win_rate": float(metrics.get("win_rate", 0.0)),
                    "profit_factor": float(metrics.get("profit_factor", 0.0)),
                    "total_trades": int(metrics.get("total_trades", 0)),
                    "metrics_detail": json.dumps(detail, ensure_ascii=False),
                })
        except Exception as e:
            logger.error(f"持久化回测实验记录失败: {e}")

    async def run_backtest(self, req: BacktestRunRequest) -> BacktestResultResponse:
        """运行单标的高保真同构回测并持久化实验记录"""
        start_exec_ts = time.time()
        now_ms = int(time.time() * 1000)
        start_ts = now_ms - (req.days_back * 24 * 60 * 60 * 1000)

        candles = await self.data_lake.load_klines(
            symbol=req.symbol,
            timeframe=req.timeframe,
            start_ts=start_ts,
            limit=50000,
        )
        if not candles or len(candles) < 50:
            logger.info(f"数据湖中 {req.symbol} [{req.timeframe}] 数据不足，在线补齐...")
            await self.data_lake.sync_symbol_history(
                symbol=req.symbol,
                timeframes=[req.timeframe],
                days_back=req.days_back,
            )
            candles = await self.data_lake.load_klines(
                symbol=req.symbol,
                timeframe=req.timeframe,
                start_ts=start_ts,
                limit=50000,
            )

        if not candles or len(candles) < 30:
            raise ValueError(f"标的 {req.symbol} 历史数据不足，无法完成回测")

        df = self._compute_indicators(candles)
        sim = self.simulate_bars(
            df=df,
            symbol=req.symbol,
            timeframe=req.timeframe,
            strategy_name=req.strategy_name,
            params=req.params,
            initial_balance=req.initial_balance,
            leverage=req.leverage,
            taker_fee=req.taker_fee_rate,
            slippage=req.slippage_pct,
            atr_mult=req.atr_sl_multiplier,
            breakeven_ratio=req.breakeven_ratio,
            default_funding_rate_8h=getattr(req, "funding_rate_8h", 0.0001),
        )

        # 接入参数平原与生死线生存检验
        plateau_dict: Dict[str, Any] = {}
        try:
            validator = _load_plateau_validator()
            p_score = validator.evaluate_strategy_survival(
                trades_pnl=[t.pnl for t in sim["trades"]],
                trades_notional=sim["trades_notional"],
                total_fee_paid=sim["total_fee_paid"],
            )
            plateau_dict = p_score.model_dump() if hasattr(p_score, "model_dump") else p_score.dict()
        except Exception as pe:
            logger.debug(f"参数平原评估跳过: {pe}")

        exec_time_ms = int((time.time() - start_exec_ts) * 1000)
        experiment_id = str(uuid.uuid4())

        result = BacktestResultResponse(
            experiment_id=experiment_id,
            strategy_name=req.strategy_name,
            symbol=req.symbol,
            timeframe=req.timeframe,
            total_candles=sim["total_candles"],
            initial_balance=sim["initial_balance"],
            final_balance=sim["final_balance"],
            total_return_pct=sim["total_return_pct"],
            annualized_return_pct=sim["annualized_return_pct"],
            max_drawdown_pct=sim["max_drawdown_pct"],
            sharpe_ratio=sim["sharpe_ratio"],
            calmar_ratio=sim["calmar_ratio"],
            win_rate=sim["win_rate"],
            profit_factor=sim["profit_factor"],
            total_trades=sim["total_trades"],
            winning_trades=sim["winning_trades"],
            losing_trades=sim["losing_trades"],
            avg_trade_pnl=sim["avg_trade_pnl"],
            total_fee_paid=sim["total_fee_paid"],
            equity_curve=sim["equity_curve"],
            trades=sim["trades"],
            execution_time_ms=exec_time_ms,
        )

        await self._persist_experiment(
            experiment_id=experiment_id,
            strategy_name=req.strategy_name,
            symbol=req.symbol,
            timeframe=req.timeframe,
            start_time=sim["start_time"],
            end_time=sim["end_time"],
            parameters=req.model_dump(),
            metrics=sim,
            detail={
                "final_balance": sim["final_balance"],
                "annualized_return_pct": sim["annualized_return_pct"],
                "calmar_ratio": sim["calmar_ratio"],
                "total_fee_paid": sim["total_fee_paid"],
                "total_funding_cost": sim["total_funding_cost"],
                "plateau_score": plateau_dict,
                "execution_time_ms": exec_time_ms,
            },
        )
        return result

    async def run_portfolio_backtest(
        self,
        symbols: Sequence[str],
        timeframe: str = "4h",
        strategy_name: str = "KELTNER_TREND",
        params: Optional[Dict[str, Any]] = None,
        days_back: int = 1095,
        initial_balance: float = 6000.0,
        leverage: int = 3,
        taker_fee: float = 0.0005,
        slippage: float = 0.0002,
        portfolio_risk_pct: Optional[float] = 0.01,
        eval_start_ts: Optional[int] = None,
        eval_end_ts: Optional[int] = None,
        preloaded_dfs: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        persist: bool = True,
    ) -> Dict[str, Any]:
        """
        多标的组合同构回测 (等权分配单标的资本槽位，按组合总净值口径定仓，合并日频盯市净值曲线并输出完整 Gate 2 生存指标)
        """
        now_ms = int(time.time() * 1000)
        start_ts = now_ms - (days_back * 24 * 60 * 60 * 1000)
        n_syms = max(1, len(symbols))
        per_sym_capital = initial_balance / n_syms
        # 组合级单笔风险 (如 1% of $6,000 = $60) 换算为单标的槽位 ($1,200) 的等效风险比例 (5%)
        slot_risk_override = (portfolio_risk_pct * n_syms) if portfolio_risk_pct is not None else None

        per_symbol_results: Dict[str, Dict[str, Any]] = {}
        all_trades_pnl: List[float] = []
        all_trades_notional: List[float] = []
        long_pnls: List[float] = []
        short_pnls: List[float] = []
        all_dates: set = set()
        total_fee = 0.0
        total_funding = 0.0

        for sym in symbols:
            if preloaded_dfs is not None and sym in preloaded_dfs:
                df = preloaded_dfs[sym]
            else:
                candles = await self.data_lake.load_klines(sym, timeframe, start_ts=start_ts, limit=50000)
                if not candles or len(candles) < 60:
                    continue
                df = self._compute_indicators(candles)

            sim = self.simulate_bars(
                df=df,
                symbol=sym,
                timeframe=timeframe,
                strategy_name=strategy_name,
                params=params,
                initial_balance=per_sym_capital,
                leverage=leverage,
                taker_fee=taker_fee,
                slippage=slippage,
                risk_pct_override=slot_risk_override,
                eval_start_ts=eval_start_ts,
                eval_end_ts=eval_end_ts,
            )
            per_symbol_results[sym] = sim
            for t in sim["trades"]:
                all_trades_pnl.append(t.pnl)
                if t.pos_side == "long":
                    long_pnls.append(t.pnl)
                else:
                    short_pnls.append(t.pnl)
            all_trades_notional.extend(sim["trades_notional"])
            total_fee += sim["total_fee_paid"]
            total_funding += sim["total_funding_cost"]
            all_dates.update(sim["daily_equity"].keys())

        if not per_symbol_results:
            raise ValueError("组合回测无可用历史 K 线数据，请先同步数据湖")

        # 合并日频组合净值曲线 (按前值填充缺失交易日) 并按日历年统计收益
        sorted_dates = sorted(all_dates)
        portfolio_daily_eq: Dict[str, float] = {}
        last_known = {sym: per_sym_capital for sym in per_symbol_results}
        peak_eq = initial_balance
        max_dd_pct = 0.0
        by_year_eq: Dict[str, Tuple[float, float]] = {}

        for d in sorted_dates:
            day_sum = 0.0
            for sym, sim in per_symbol_results.items():
                if d in sim["daily_equity"]:
                    last_known[sym] = sim["daily_equity"][d]
                day_sum += last_known[sym]
            portfolio_daily_eq[d] = day_sum
            yr = d[:4]
            by_year_eq[yr] = (by_year_eq.get(yr, (day_sum, day_sum))[0], day_sum)
            peak_eq = max(peak_eq, day_sum)
            dd = (peak_eq - day_sum) / peak_eq * 100.0 if peak_eq > 0 else 0.0
            max_dd_pct = max(max_dd_pct, dd)

        final_eq = sum(sim["final_balance"] for sim in per_symbol_results.values())
        total_return_pct = round((final_eq - initial_balance) / initial_balance * 100.0, 2)
        span_days = max(1.0, float(len(sorted_dates)))
        cagr_pct = round((((final_eq / initial_balance) ** (365.0 / span_days)) - 1.0) * 100.0, 2) if final_eq > 0 else -100.0
        sharpe = self._compute_daily_sharpe(portfolio_daily_eq)
        calmar = round(cagr_pct / max(max_dd_pct, 0.1), 2)

        total_trades = len(all_trades_pnl)
        wins = sum(1 for p in all_trades_pnl if p > 0)
        win_rate = round(wins / total_trades, 4) if total_trades > 0 else 0.0
        gross_gain = sum(p for p in all_trades_pnl if p > 0)
        gross_loss = abs(sum(p for p in all_trades_pnl if p < 0))
        pf = round(gross_gain / gross_loss, 3) if gross_loss > 1e-9 else (9.99 if gross_gain > 0 else 0.0)

        def _dir_stat(arr: List[float]) -> Dict[str, Any]:
            if not arr:
                return {"trades": 0, "pnl": 0.0, "win_rate": 0.0, "pf": 0.0}
            g = sum(x for x in arr if x > 0)
            l = abs(sum(x for x in arr if x < 0))
            return {
                "trades": len(arr),
                "pnl": round(sum(arr), 2),
                "win_rate": round(sum(1 for x in arr if x > 0) / len(arr), 4),
                "pf": round(g / l, 3) if l > 1e-9 else (9.99 if g > 0 else 0.0),
            }

        validator = _load_plateau_validator()
        survival = validator.evaluate_strategy_survival(
            trades_pnl=all_trades_pnl,
            trades_notional=all_trades_notional,
            total_fee_paid=total_fee,
            slippage_stress_bps=40.0,
            min_top3_retention_ratio=0.50,
            max_fee_ratio=0.25,
        )
        survival_dict = survival.model_dump() if hasattr(survival, "model_dump") else survival.dict()
        yearly_returns = {
            y: round((e - s) / s * 100.0, 2) for y, (s, e) in sorted(by_year_eq.items()) if s > 0
        }

        report = {
            "strategy_name": strategy_name,
            "symbols": list(per_symbol_results.keys()),
            "timeframe": timeframe,
            "params": params or {},
            "initial_balance": initial_balance,
            "final_balance": round(final_eq, 2),
            "total_return_pct": total_return_pct,
            "annualized_return_pct": cagr_pct,
            "max_drawdown_pct": round(max_dd_pct, 2),
            "sharpe_ratio": sharpe,
            "calmar_ratio": calmar,
            "win_rate": win_rate,
            "profit_factor": pf,
            "total_trades": total_trades,
            "total_fee_paid": round(total_fee, 2),
            "total_funding_cost": round(total_funding, 2),
            "yearly_returns_pct": yearly_returns,
            "long_stats": _dir_stat(long_pnls),
            "short_stats": _dir_stat(short_pnls),
            "survival_gate": survival_dict,
            "trades_pnl": all_trades_pnl,
            "trades_notional": all_trades_notional,
            "daily_equity": portfolio_daily_eq,
            "per_symbol": {
                sym: {
                    "return_pct": s["total_return_pct"],
                    "sharpe": s["sharpe_ratio"],
                    "max_dd_pct": s["max_drawdown_pct"],
                    "pf": s["profit_factor"],
                    "win_rate": s["win_rate"],
                    "trades": s["total_trades"],
                    "fee": s["total_fee_paid"],
                    "funding": s["total_funding_cost"],
                }
                for sym, s in per_symbol_results.items()
            },
        }

        if persist:
            exp_id = str(uuid.uuid4())
            min_ts = min(s["start_time"] for s in per_symbol_results.values())
            max_ts = max(s["end_time"] for s in per_symbol_results.values())
            await self._persist_experiment(
                experiment_id=exp_id,
                strategy_name=f"PORTFOLIO:{strategy_name}",
                symbol=",".join(report["symbols"]),
                timeframe=timeframe,
                start_time=min_ts,
                end_time=max_ts,
                parameters={"params": params or {}, "leverage": leverage, "days_back": days_back, "portfolio_risk_pct": portfolio_risk_pct},
                metrics=report,
                detail={k: v for k, v in report.items() if k not in ("daily_equity", "trades_pnl", "trades_notional")},
            )
            report["experiment_id"] = exp_id

        return report

    async def run_dual_tf_trend_gate2a(
        self,
        symbols: Sequence[str],
        days_back: int = 1095,
        initial_balance: float = 6000.0,
        leverage: int = 3,
        keltner_params: Optional[Dict[str, Any]] = None,
        dual_ema_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Stage 2A 主引擎 B 官方 Gate 2A 验真入口:
        合并 50% 4H KELTNER_TREND + 50% 1D DUAL_EMA_TREND 双周期组合，
        校验全 3 年、严格后 2 年样本外 (2024.11~2026.11)、多空双向、分标的盈利度与全维度 ±20% 参数平原，
        输出可证伪的 Gate 2A 最终判决并落库 backtest_experiments。
        """
        kp = dict({"ema_n": 20, "band_k": 2.5, "sl_atr_mult": 3.0, "trend_ema": 200}, **(keltner_params or {}))
        ep = dict({"fast_n": 20, "slow_n": 50, "sl_atr_mult": 2.5}, **(dual_ema_params or {}))

        now_ms = int(time.time() * 1000)
        start_ts = now_ms - (days_back * 24 * 60 * 60 * 1000)

        pre_4h: Dict[str, List[Dict[str, Any]]] = {}
        pre_1d: Dict[str, List[Dict[str, Any]]] = {}
        for sym in symbols:
            c4 = await self.data_lake.load_klines(sym, "4h", start_ts=start_ts, limit=50000)
            c1 = await self.data_lake.load_klines(sym, "1d", start_ts=start_ts, limit=50000)
            if c4 and len(c4) >= 200 and c1 and len(c1) >= 100:
                pre_4h[sym] = self._compute_indicators(c4)
                pre_1d[sym] = self._compute_indicators(c1)

        valid_syms = list(pre_4h.keys())
        if not valid_syms:
            raise ValueError("数据湖缺少 Top 5 4H/1D 历史数据，请先同步数据湖")

        global_start = max(int(pre_4h[s][0]["timestamp"]) for s in valid_syms)
        global_end = min(int(pre_4h[s][-1]["timestamp"]) for s in valid_syms)
        # 严格时间切分: 前 1 年 (365 天) 样本内 -> 后 2 年 (730 天) 完全样本外
        oos_split_ts = global_start + 365 * 86400 * 1000
        half_cap = initial_balance * 0.5

        def _merge_sleeves(res_a: Dict[str, Any], res_b: Dict[str, Any]) -> Dict[str, Any]:
            da, db = res_a["daily_equity"], res_b["daily_equity"]
            dates = sorted(set(da.keys()) | set(db.keys()))
            la, lb = half_cap, half_cap
            merged_daily: Dict[str, float] = {}
            peak = initial_balance
            mdd = 0.0
            by_yr: Dict[str, Tuple[float, float]] = {}
            for d in dates:
                if d in da:
                    la = da[d]
                if d in db:
                    lb = db[d]
                eq = la + lb
                merged_daily[d] = eq
                yr = d[:4]
                by_yr[yr] = (by_yr.get(yr, (eq, eq))[0], eq)
                peak = max(peak, eq)
                mdd = max(mdd, (peak - eq) / peak * 100.0 if peak > 0 else 0.0)

            final_eq = res_a["final_balance"] + res_b["final_balance"]
            pnls = list(res_a["trades_pnl"]) + list(res_b["trades_pnl"])
            nots = list(res_a["trades_notional"]) + list(res_b["trades_notional"])
            tot_fee = res_a["total_fee_paid"] + res_b["total_fee_paid"]
            gains = sum(p for p in pnls if p > 0)
            losses = abs(sum(p for p in pnls if p < 0))
            pf = round(gains / losses, 3) if losses > 1e-9 else 0.0
            wr = round(sum(1 for p in pnls if p > 0) / len(pnls), 4) if pnls else 0.0
            span_days = max(1.0, float(len(dates)))
            cagr = round((((final_eq / initial_balance) ** (365.0 / span_days)) - 1.0) * 100.0, 2) if final_eq > 0 else -100.0
            sharpe = self._compute_daily_sharpe(merged_daily)
            validator = _load_plateau_validator()
            surv = validator.evaluate_strategy_survival(
                trades_pnl=pnls, trades_notional=nots, total_fee_paid=tot_fee,
                slippage_stress_bps=40.0, min_top3_retention_ratio=0.50, max_fee_ratio=0.25,
            )
            surv_d = surv.model_dump() if hasattr(surv, "model_dump") else surv.dict()
            return {
                "final_balance": round(final_eq, 2),
                "total_return_pct": round((final_eq - initial_balance) / initial_balance * 100.0, 2),
                "cagr_pct": cagr,
                "sharpe_ratio": sharpe,
                "max_drawdown_pct": round(mdd, 2),
                "calmar_ratio": round(cagr / max(mdd, 0.1), 2),
                "profit_factor": pf,
                "win_rate": wr,
                "total_trades": len(pnls),
                "total_fee_paid": round(tot_fee, 2),
                "yearly_returns_pct": {y: round((e - s) / s * 100.0, 2) for y, (s, e) in sorted(by_yr.items()) if s > 0},
                "survival_gate": surv_d,
            }

        # 1. 全 3 年双臂
        full_4h = await self.run_portfolio_backtest(
            valid_syms, "4h", "KELTNER_TREND", kp, days_back, half_cap, leverage,
            portfolio_risk_pct=0.01, preloaded_dfs=pre_4h, persist=False,
        )
        full_1d = await self.run_portfolio_backtest(
            valid_syms, "1d", "DUAL_EMA_TREND", ep, days_back, half_cap, leverage,
            portfolio_risk_pct=0.015, preloaded_dfs=pre_1d, persist=False,
        )
        full_ens = _merge_sleeves(full_4h, full_1d)

        # 2. 严格后 2 年样本外双臂
        oos_4h = await self.run_portfolio_backtest(
            valid_syms, "4h", "KELTNER_TREND", kp, days_back, half_cap, leverage,
            portfolio_risk_pct=0.01, eval_start_ts=oos_split_ts, preloaded_dfs=pre_4h, persist=False,
        )
        oos_1d = await self.run_portfolio_backtest(
            valid_syms, "1d", "DUAL_EMA_TREND", ep, days_back, half_cap, leverage,
            portfolio_risk_pct=0.015, eval_start_ts=oos_split_ts, preloaded_dfs=pre_1d, persist=False,
        )
        oos_ens = _merge_sleeves(oos_4h, oos_1d)

        # 3. 全维度 ±20% 参数平原检验 (同步扰动 4H ema_n/band_k/sl_atr_mult 与 1D fast_n/slow_n/sl_atr_mult)
        neighbor_pfs: List[float] = []
        neighbor_pnls: List[float] = []
        for mult in (0.8, 1.0, 1.2):
            kp_var = {
                "ema_n": max(8, int(round(kp["ema_n"] * mult))),
                "band_k": round(kp["band_k"] * mult, 2),
                "sl_atr_mult": round(kp["sl_atr_mult"] * mult, 2),
                "trend_ema": kp["trend_ema"],
            }
            ep_var = {
                "fast_n": max(6, int(round(ep["fast_n"] * mult))),
                "slow_n": max(15, int(round(ep["slow_n"] * mult))),
                "sl_atr_mult": round(ep["sl_atr_mult"] * mult, 2),
            }
            n4 = await self.run_portfolio_backtest(
                valid_syms, "4h", "KELTNER_TREND", kp_var, days_back, half_cap, leverage,
                portfolio_risk_pct=0.01, preloaded_dfs=pre_4h, persist=False,
            )
            n1 = await self.run_portfolio_backtest(
                valid_syms, "1d", "DUAL_EMA_TREND", ep_var, days_back, half_cap, leverage,
                portfolio_risk_pct=0.015, preloaded_dfs=pre_1d, persist=False,
            )
            n_ens = _merge_sleeves(n4, n1)
            neighbor_pfs.append(n_ens["profit_factor"])
            neighbor_pnls.append(n_ens["final_balance"] - initial_balance)

        # 4. Gate 2A 7 项硬门槛严格裁决 (以严格后 2 年样本外为主判据)
        oos_surv = oos_ens["survival_gate"]
        gate2a_checks = {
            "oos_pf_ge_1_30": oos_ens["profit_factor"] >= 1.30,
            "oos_sharpe_ge_1_00": oos_ens["sharpe_ratio"] >= 1.00,
            "oos_max_dd_le_20": oos_ens["max_drawdown_pct"] <= 20.0,
            "top3_retention_ge_50pct": float(oos_surv.get("drop_top3_decay", 0.0)) >= 0.50,
            "fee_ratio_le_25pct": float(oos_surv.get("fee_to_gross_ratio", 1.0)) <= 0.25,
            "stress_40bps_positive": float(oos_surv.get("friction_stress_pnl", -1.0)) > 0.0,
            "neighbors_pf_gt_1_10": all(pf > 1.10 for pf in neighbor_pfs),
        }
        passed_gate2a = all(gate2a_checks.values())

        report = {
            "strategy_name": "DUAL_TF_TREND_ENGINE_B",
            "symbols": valid_syms,
            "sleeves": {
                "4h_keltner": {"weight": 0.5, "params": kp, "full": {k: full_4h[k] for k in ("total_return_pct", "annualized_return_pct", "sharpe_ratio", "max_drawdown_pct", "profit_factor", "total_trades", "long_stats", "short_stats", "per_symbol")}},
                "1d_dual_ema": {"weight": 0.5, "params": ep, "full": {k: full_1d[k] for k in ("total_return_pct", "annualized_return_pct", "sharpe_ratio", "max_drawdown_pct", "profit_factor", "total_trades", "long_stats", "short_stats", "per_symbol")}},
            },
            "full_3y_ensemble": full_ens,
            "oos_2y_ensemble": oos_ens,
            "neighbor_pfs_pm20pct": neighbor_pfs,
            "gate2a_checks": gate2a_checks,
            "passed_gate2a": passed_gate2a,
        }

        exp_id = str(uuid.uuid4())
        await self._persist_experiment(
            experiment_id=exp_id,
            strategy_name="GATE2A:DUAL_TF_TREND_ENGINE_B",
            symbol=",".join(valid_syms),
            timeframe="4h+1d",
            start_time=global_start,
            end_time=global_end,
            parameters={"4h_keltner": kp, "1d_dual_ema": ep, "leverage": leverage},
            metrics={
                "total_return_pct": oos_ens["total_return_pct"],
                "sharpe_ratio": oos_ens["sharpe_ratio"],
                "max_drawdown_pct": oos_ens["max_drawdown_pct"],
                "win_rate": oos_ens["win_rate"],
                "profit_factor": oos_ens["profit_factor"],
                "total_trades": oos_ens["total_trades"],
            },
            detail=report,
        )
        report["experiment_id"] = exp_id
        return report

    async def run_walk_forward(
        self,
        symbols: Sequence[str],
        timeframe: str = "4h",
        strategy_name: str = "DONCHIAN_TREND",
        param_grid: Optional[List[Dict[str, Any]]] = None,
        days_back: int = 1095,
        train_days: int = 365,
        test_days: int = 90,
        step_days: int = 90,
        initial_balance: float = 6000.0,
        leverage: int = 3,
    ) -> Dict[str, Any]:
        """
        Stage 1 (T1.3): 滚动样本外 Walk-Forward 检验 + ±20% 参数平原门禁
        1. 每个 Fold 在 [train_start, train_end] 样本内网格选优 (带最低交易笔数约束防过拟合)；
        2. 冻结最优参数，在完全未见过的 [train_end, test_end] 样本外窗口实测；
        3. 拼接全量样本外收益曲线与逐笔流水，并运行 ±20% 邻域参数平原稳定度校验；
        4. 输出明确的 Gate 2 通过判决，并将完整实验记录持久化至 backtest_experiments。
        """
        now_ms = int(time.time() * 1000)
        start_ts = now_ms - (days_back * 24 * 60 * 60 * 1000)

        preloaded: Dict[str, List[Dict[str, Any]]] = {}
        min_ts_list: List[int] = []
        max_ts_list: List[int] = []
        for sym in symbols:
            candles = await self.data_lake.load_klines(sym, timeframe, start_ts=start_ts, limit=50000)
            if candles and len(candles) >= 120:
                preloaded[sym] = self._compute_indicators(candles)
                min_ts_list.append(int(candles[0]["timestamp"]))
                max_ts_list.append(int(candles[-1]["timestamp"]))

        if not preloaded:
            raise ValueError("Walk-Forward 缺少足够的历史数据，请先运行数据湖 3 年同步")

        valid_symbols = list(preloaded.keys())
        global_start = max(min_ts_list)
        global_end = min(max_ts_list)
        day_ms = 86400 * 1000

        if not param_grid:
            param_grid = [
                {"entry_n": en, "exit_n": ex, "atr_sl_mult": am, "trend_ema": te, "risk_pct": 0.01}
                for en in (18, 24, 32)
                for ex in (8, 12)
                for am in (2.5, 3.0, 3.5)
                for te in (0, 100)
            ]

        folds: List[Dict[str, Any]] = []
        oos_trades_pnl: List[float] = []
        oos_trades_notional: List[float] = []
        oos_total_fee = 0.0
        oos_daily_equity: Dict[str, float] = {}
        stitched_equity = float(initial_balance)
        peak_eq = stitched_equity
        max_dd_pct = 0.0

        cursor_train_start = global_start
        fold_idx = 0
        while cursor_train_start + (train_days + test_days) * day_ms <= global_end + 15 * day_ms:
            train_end = cursor_train_start + train_days * day_ms
            test_end = min(global_end, train_end + test_days * day_ms)
            if test_end - train_end < 30 * day_ms:
                break

            fold_idx += 1
            best_score = -1e9
            best_params = param_grid[0]
            best_train_metrics: Dict[str, Any] = {}

            # 样本内寻优 (仅使用 train 窗口数据，绝不偷看 test 窗口)
            for cand_params in param_grid:
                tr_res = await self.run_portfolio_backtest(
                    symbols=valid_symbols,
                    timeframe=timeframe,
                    strategy_name=strategy_name,
                    params=cand_params,
                    days_back=days_back,
                    initial_balance=initial_balance,
                    leverage=leverage,
                    eval_start_ts=cursor_train_start,
                    eval_end_ts=train_end,
                    preloaded_dfs=preloaded,
                    persist=False,
                )
                # 综合评分：要求样本内至少 12 笔交易，优先夏普与回撤控制
                if tr_res["total_trades"] < 12:
                    score = -100.0 + tr_res["sharpe_ratio"]
                else:
                    score = tr_res["sharpe_ratio"] * 2.0 + min(tr_res["profit_factor"], 3.0) - (tr_res["max_drawdown_pct"] / 25.0)
                if score > best_score:
                    best_score = score
                    best_params = cand_params
                    best_train_metrics = {
                        "return_pct": tr_res["total_return_pct"],
                        "sharpe": tr_res["sharpe_ratio"],
                        "pf": tr_res["profit_factor"],
                        "max_dd_pct": tr_res["max_drawdown_pct"],
                        "trades": tr_res["total_trades"],
                    }

            # 冻结最优参数跑样本外 test 窗口 (以当前拼接净值作为本 Fold 期初资金)
            oos_res = await self.run_portfolio_backtest(
                symbols=valid_symbols,
                timeframe=timeframe,
                strategy_name=strategy_name,
                params=best_params,
                days_back=days_back,
                initial_balance=stitched_equity,
                leverage=leverage,
                eval_start_ts=train_end,
                eval_end_ts=test_end,
                preloaded_dfs=preloaded,
                persist=False,
            )

            # 收集样本外日频净值与交易明细
            for d_key, d_eq in sorted(oos_res["daily_equity"].items()):
                oos_daily_equity[d_key] = d_eq
                peak_eq = max(peak_eq, d_eq)
                dd = (peak_eq - d_eq) / peak_eq * 100.0 if peak_eq > 0 else 0.0
                max_dd_pct = max(max_dd_pct, dd)

            # 重新采集本 Fold 逐笔盈亏与名义金额用于全量 OOS 生死线检验
            per_sym_cap = stitched_equity / len(valid_symbols)
            for sym in valid_symbols:
                s_oos = self.simulate_bars(
                    df=preloaded[sym],
                    symbol=sym,
                    timeframe=timeframe,
                    strategy_name=strategy_name,
                    params=best_params,
                    initial_balance=per_sym_cap,
                    leverage=leverage,
                    eval_start_ts=train_end,
                    eval_end_ts=test_end,
                )
                oos_trades_pnl.extend([t.pnl for t in s_oos["trades"]])
                oos_trades_notional.extend(s_oos["trades_notional"])

            oos_total_fee += oos_res["total_fee_paid"]
            stitched_equity = oos_res["final_balance"]

            folds.append({
                "fold": fold_idx,
                "train_range": [
                    datetime.datetime.fromtimestamp(cursor_train_start / 1000.0, tz=datetime.timezone.utc).strftime("%Y-%m-%d"),
                    datetime.datetime.fromtimestamp(train_end / 1000.0, tz=datetime.timezone.utc).strftime("%Y-%m-%d"),
                ],
                "test_range": [
                    datetime.datetime.fromtimestamp(train_end / 1000.0, tz=datetime.timezone.utc).strftime("%Y-%m-%d"),
                    datetime.datetime.fromtimestamp(test_end / 1000.0, tz=datetime.timezone.utc).strftime("%Y-%m-%d"),
                ],
                "selected_params": best_params,
                "in_sample": best_train_metrics,
                "out_of_sample": {
                    "return_pct": oos_res["total_return_pct"],
                    "sharpe": oos_res["sharpe_ratio"],
                    "pf": oos_res["profit_factor"],
                    "max_dd_pct": oos_res["max_drawdown_pct"],
                    "trades": oos_res["total_trades"],
                },
            })
            cursor_train_start += step_days * day_ms

        # 汇总纯样本外组合指标
        oos_total_return_pct = round((stitched_equity - initial_balance) / initial_balance * 100.0, 2)
        oos_days = max(1.0, float(len(oos_daily_equity)))
        oos_cagr_pct = round((((stitched_equity / initial_balance) ** (365.0 / oos_days)) - 1.0) * 100.0, 2) if stitched_equity > 0 else -100.0
        oos_sharpe = self._compute_daily_sharpe(oos_daily_equity)
        oos_trades = len(oos_trades_pnl)
        oos_wins = sum(1 for p in oos_trades_pnl if p > 0)
        oos_win_rate = round(oos_wins / oos_trades, 4) if oos_trades > 0 else 0.0
        oos_gross_gain = sum(p for p in oos_trades_pnl if p > 0)
        oos_gross_loss = abs(sum(p for p in oos_trades_pnl if p < 0))
        oos_pf = round(oos_gross_gain / oos_gross_loss, 3) if oos_gross_loss > 1e-9 else (9.99 if oos_gross_gain > 0 else 0.0)

        # 参数平原 ±20% 邻域扰动校验 (基于最后 1 折胜出参数做全局邻域检查)
        anchor_params = folds[-1]["selected_params"] if folds else param_grid[0]
        neighbor_pnls: List[float] = []
        neighbor_pfs: List[float] = []
        for mult in (0.8, 1.0, 1.2):
            p_var = dict(anchor_params)
            if "entry_n" in p_var:
                p_var["entry_n"] = max(6, int(round(p_var["entry_n"] * mult)))
            if "atr_sl_mult" in p_var:
                p_var["atr_sl_mult"] = round(max(1.2, float(p_var["atr_sl_mult"]) * mult), 2)
            n_res = await self.run_portfolio_backtest(
                symbols=valid_symbols,
                timeframe=timeframe,
                strategy_name=strategy_name,
                params=p_var,
                days_back=days_back,
                initial_balance=initial_balance,
                leverage=leverage,
                preloaded_dfs=preloaded,
                persist=False,
            )
            neighbor_pnls.append(n_res["final_balance"] - initial_balance)
            neighbor_pfs.append(n_res["profit_factor"])

        validator = _load_plateau_validator()
        plateau_score = validator.evaluate_strategy_survival(
            trades_pnl=oos_trades_pnl,
            trades_notional=oos_trades_notional,
            total_fee_paid=oos_total_fee,
            slippage_stress_bps=40.0,
            min_top3_retention_ratio=0.50,
            max_fee_ratio=0.25,
            neighbor_pnl_list=neighbor_pnls,
        )
        plateau_dict = plateau_score.model_dump() if hasattr(plateau_score, "model_dump") else plateau_score.dict()

        # Gate 2A 硬性验收门禁判定
        gate2_checks = {
            "oos_pf_ge_1_30": oos_pf >= 1.30,
            "oos_sharpe_ge_1_00": oos_sharpe >= 1.00,
            "oos_max_dd_le_20": max_dd_pct <= 20.0,
            "top3_retention_ge_50pct": float(plateau_dict.get("drop_top3_decay", 0.0)) >= 0.50,
            "fee_ratio_le_25pct": float(plateau_dict.get("fee_to_gross_ratio", 1.0)) <= 0.25,
            "stress_40bps_positive": float(plateau_dict.get("friction_stress_pnl", -1.0)) > 0.0,
            "neighbors_pf_gt_1_10": all(pf > 1.10 for pf in neighbor_pfs) if neighbor_pfs else False,
        }
        passed_gate2 = all(gate2_checks.values())

        wf_report = {
            "strategy_name": strategy_name,
            "symbols": valid_symbols,
            "timeframe": timeframe,
            "folds_count": len(folds),
            "initial_balance": initial_balance,
            "oos_final_balance": round(stitched_equity, 2),
            "oos_total_return_pct": oos_total_return_pct,
            "oos_cagr_pct": oos_cagr_pct,
            "oos_sharpe": oos_sharpe,
            "oos_max_drawdown_pct": round(max_dd_pct, 2),
            "oos_profit_factor": oos_pf,
            "oos_win_rate": oos_win_rate,
            "oos_total_trades": oos_trades,
            "oos_total_fee_paid": round(oos_total_fee, 2),
            "anchor_params": anchor_params,
            "neighbor_pfs": neighbor_pfs,
            "plateau_score": plateau_dict,
            "gate2_checks": gate2_checks,
            "passed_gate2": passed_gate2,
            "folds": folds,
        }

        exp_id = str(uuid.uuid4())
        await self._persist_experiment(
            experiment_id=exp_id,
            strategy_name=f"WALK_FORWARD:{strategy_name}",
            symbol=",".join(valid_symbols),
            timeframe=timeframe,
            start_time=global_start,
            end_time=global_end,
            parameters={"train_days": train_days, "test_days": test_days, "step_days": step_days, "leverage": leverage},
            metrics={
                "total_return_pct": oos_total_return_pct,
                "sharpe_ratio": oos_sharpe,
                "max_drawdown_pct": round(max_dd_pct, 2),
                "win_rate": oos_win_rate,
                "profit_factor": oos_pf,
                "total_trades": oos_trades,
            },
            detail=wf_report,
        )
        wf_report["experiment_id"] = exp_id
        return wf_report

    async def run_grid_search(self, req: GridSearchRequest) -> GridSearchResponse:
        """多参数网格搜索"""
        results: List[GridSearchResultItem] = []

        for atr in req.atr_multipliers:
            for be in req.breakeven_ratios:
                for lev in req.leverages:
                    run_req = BacktestRunRequest(
                        strategy_name=req.strategy_name,
                        symbol=req.symbol,
                        timeframe=req.timeframe,
                        days_back=req.days_back,
                        leverage=lev,
                        atr_sl_multiplier=atr,
                        breakeven_ratio=be,
                    )
                    try:
                        res = await self.run_backtest(run_req)
                        results.append(GridSearchResultItem(
                            params={"atr_multiplier": atr, "breakeven_ratio": be, "leverage": lev},
                            total_return_pct=res.total_return_pct,
                            sharpe_ratio=res.sharpe_ratio,
                            max_drawdown_pct=res.max_drawdown_pct,
                            win_rate=res.win_rate,
                            profit_factor=res.profit_factor,
                            total_trades=res.total_trades,
                        ))
                    except Exception as e:
                        logger.warning(f"网格参数 [atr={atr}, be={be}, lev={lev}] 回测异常: {e}")

        results.sort(key=lambda x: (x.sharpe_ratio, x.total_return_pct), reverse=True)
        best_params = results[0].params if results else {}

        return GridSearchResponse(
            best_params=best_params,
            items=results,
        )
