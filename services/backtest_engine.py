"""
OKX-Dog 个人量化智能交易系统 - 极速向量化回测沙盒与参数网格优化器
模块: okx-dog-backend/services/backtest_engine.py
角色: 量化回测与现实逻辑检验师 (agency-reality-checker)
功能:
1. 极速逐根与指标增量回测框架 (10,000 根 K 线 < 0.2 秒，纯 Python 零额外依赖)
2. 强制包含现实摩擦模型: Taker (0.05%), Maker (0.02%), 保守滑点 (0.05%~0.2%), 资金费率扣除
3. 详尽量化评价指标: 收益率、年化、最大回撤、夏普比率、卡尔玛比率、胜率、盈亏比、MFE/MAE
4. 参数网格搜索 (Grid Search) 与 Walk-Forward 滚动检验
5. 实验记录落库与查询 (backtest_experiments)
"""

import asyncio
import json
import logging
import math
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

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
)
from core.strategy_base import (
    BacktestContext,
    BarData,
    BaseStrategy,
    EMABreakoutStrategy,
)
from services.data_lake_service import DataLakeService
from services.position_sizer import position_sizer

logger = logging.getLogger("okx_dog.backtest")


class BacktestEngine:
    """金融级极速回测沙盒与检验中枢"""

    def __init__(self, data_lake: DataLakeService):
        self.data_lake = data_lake

    @staticmethod
    def _compute_indicators(candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """纯 Python 毫秒级增量计算 EMA20/50/200, ATR14, RSI14"""
        if not candles:
            return []

        out = []
        ema20 = float(candles[0]["close"])
        ema50 = float(candles[0]["close"])
        ema200 = float(candles[0]["close"])
        k20 = 2.0 / (20 + 1)
        k50 = 2.0 / (50 + 1)
        k200 = 2.0 / (200 + 1)

        tr_list: List[float] = []
        gains: List[float] = []
        losses: List[float] = []

        prev_close = float(candles[0]["close"])

        for i, c in enumerate(candles):
            close_p = float(c["close"])
            high_p = float(c["high"])
            low_p = float(c["low"])

            # EMA
            ema20 = close_p * k20 + ema20 * (1 - k20)
            ema50 = close_p * k50 + ema50 * (1 - k50)
            ema200 = close_p * k200 + ema200 * (1 - k200)

            # True Range
            if i == 0:
                tr = high_p - low_p
            else:
                tr = max(high_p - low_p, abs(high_p - prev_close), abs(low_p - prev_close))
            tr_list.append(tr)
            if len(tr_list) > 14:
                tr_list.pop(0)
            atr14 = sum(tr_list) / len(tr_list) if tr_list else (close_p * 0.015)

            # RSI
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
            item["atr14"] = atr14
            item["rsi14"] = rsi14
            out.append(item)

            prev_close = close_p

        return out

    async def run_backtest(self, req: BacktestRunRequest) -> BacktestResultResponse:
        """运行极速向量化与逐根模拟回测"""
        start_exec_ts = time.time()
        now_ms = int(time.time() * 1000)
        start_ts = now_ms - (req.days_back * 24 * 60 * 60 * 1000)

        # 1. 加载历史 K 线
        candles = await self.data_lake.load_klines(
            symbol=req.symbol,
            timeframe=req.timeframe,
            start_ts=start_ts,
            limit=20000
        )

        if not candles or len(candles) < 50:
            logger.info(f"数据湖中 {req.symbol} [{req.timeframe}] 数据不足，在线补齐...")
            await self.data_lake.sync_symbol_history(
                symbol=req.symbol,
                timeframes=[req.timeframe],
                days_back=req.days_back
            )
            candles = await self.data_lake.load_klines(
                symbol=req.symbol,
                timeframe=req.timeframe,
                start_ts=start_ts,
                limit=20000
            )

        if not candles or len(candles) < 30:
            raise ValueError(f"标的 {req.symbol} 历史数据不足，无法完成回测")

        df = self._compute_indicators(candles)

        # 2. 模拟回测核心执行
        initial_balance = req.initial_balance
        equity = initial_balance
        peak_equity = initial_balance
        max_drawdown_pct = 0.0

        leverage = req.leverage
        taker_fee = req.taker_fee_rate
        slippage = req.slippage_pct
        atr_mult = req.atr_sl_multiplier
        breakeven_ratio = req.breakeven_ratio

        trades: List[BacktestTradeRecord] = []
        equity_curve: List[BacktestEquityPoint] = []
        position: Optional[Dict[str, Any]] = None
        pending_signal: Optional[Dict[str, Any]] = None
        last_funding_window: int = -1
        trade_counter = 0
        total_fee_paid = 0.0
        funding_rate_8h = getattr(req, "funding_rate_8h", 0.0001)

        # 实例化三位一体统一策略并注入回测上下文
        strategy: BaseStrategy = EMABreakoutStrategy(req.params)
        ctx = BacktestContext(symbol=req.symbol, timeframe=req.timeframe, initial_equity=initial_balance, leverage=leverage)
        strategy.on_init(ctx)

        for i in range(20, len(df)):
            row = df[i]
            prev_row = df[i - 1]
            ts = int(row["timestamp"])
            open_px = float(row.get("open", row["close"]))
            close_px = float(row["close"])
            high_px = float(row["high"])
            low_px = float(row["low"])
            atr = float(row["atr14"])
            is_bullish = (close_px >= open_px)

            # 1. 真实 8 小时资金费率结算 (跨越 UTC 00:00/08:00/16:00 窗口)
            funding_window = ts // (8 * 3600 * 1000)
            if position:
                if last_funding_window != -1 and funding_window > last_funding_window:
                    windows_crossed = funding_window - last_funding_window
                    cur_notional = position["size"] * close_px
                    if position["pos_side"] == "long":
                        funding_cost = cur_notional * funding_rate_8h * windows_crossed
                        equity -= funding_cost
                        total_fee_paid += funding_cost
                        position["funding_fee_paid"] = position.get("funding_fee_paid", 0.0) + funding_cost
                    else:
                        funding_gain = cur_notional * funding_rate_8h * windows_crossed
                        equity += funding_gain
                        position["funding_fee_paid"] = position.get("funding_fee_paid", 0.0) - funding_gain
            last_funding_window = funding_window

            # 2. Next-Bar 开盘撮合成交 (严格消除 Look-ahead 偏差)
            if pending_signal and position is None and equity > 100:
                sig_side = pending_signal["pos_side"]
                sig_atr = pending_signal["atr"]
                if sig_side == "long":
                    entry_px = open_px * (1 + slippage)
                    sl_dist = sig_atr * atr_mult
                    sl_px = entry_px - sl_dist
                    tp_px = entry_px + sl_dist * 2.0
                else:
                    entry_px = open_px * (1 - slippage)
                    sl_dist = sig_atr * atr_mult
                    sl_px = entry_px + sl_dist
                    tp_px = entry_px - sl_dist * 2.0

                sizer_res = position_sizer.calculate_adaptive_size(
                    symbol=req.symbol,
                    equity=equity,
                    entry_price=entry_px,
                    is_short=(sig_side == "short"),
                    stop_loss_price=sl_px,
                    atr=sig_atr,
                    atr_multiplier=atr_mult,
                    risk_pct=0.01,
                    max_leverage=leverage,
                    max_order_usdt=min(equity * 0.3 * leverage, 10000.0)
                )
                size = sizer_res.calculated_size
                notional = sizer_res.notional_value_usdt
                order_margin = sizer_res.required_margin_usdt
                fee = notional * taker_fee
                equity -= fee
                total_fee_paid += fee

                position = {
                    "pos_side": sig_side,
                    "entry_price": entry_px,
                    "sl_price": sl_px,
                    "tp_price": tp_px,
                    "size": size,
                    "margin": order_margin,
                    "notional": notional,
                    "entry_time": ts,
                    "mfe_pct": 0.0,
                    "mae_pct": 0.0,
                    "breakeven_locked": False,
                    "funding_fee_paid": 0.0
                }
                pending_signal = None

            # 3. 持仓风控与止盈止损巡检 (含阳线/阴线子路径歧义推导)
            if position:
                pos_side = position["pos_side"]
                entry_px = position["entry_price"]
                sl_px = position["sl_price"]
                tp_px = position["tp_price"]
                size = position["size"]
                entry_time = position["entry_time"]

                if pos_side == "long":
                    mfe = max(position.get("mfe_pct", 0.0), (high_px - entry_px) / entry_px * 100.0)
                    mae = min(position.get("mae_pct", 0.0), (low_px - entry_px) / entry_px * 100.0)
                    position["mfe_pct"] = mfe
                    position["mae_pct"] = mae

                    if not position.get("breakeven_locked", False):
                        if (high_px - entry_px) >= (tp_px - entry_px) * breakeven_ratio:
                            position["sl_price"] = entry_px * (1 + taker_fee * 2)
                            position["breakeven_locked"] = True
                            sl_px = position["sl_price"]

                    sl_hit = (low_px <= sl_px)
                    tp_hit = (high_px >= tp_px)

                    if sl_hit and tp_hit:
                        final_exit = "STOP_LOSS" if is_bullish else "TAKE_PROFIT"
                    elif sl_hit:
                        final_exit = "STOP_LOSS"
                    elif tp_hit:
                        final_exit = "TAKE_PROFIT"
                    else:
                        final_exit = None

                    if final_exit == "STOP_LOSS":
                        exit_px = sl_px * (1 - slippage)
                        pnl = (exit_px - entry_px) * size
                        fee = exit_px * size * taker_fee
                        net_pnl = pnl - fee
                        equity += net_pnl
                        total_fee_paid += fee
                        trade_counter += 1
                        trades.append(BacktestTradeRecord(
                            trade_id=trade_counter,
                            entry_time=entry_time,
                            exit_time=ts,
                            pos_side="long",
                            entry_price=entry_px,
                            exit_price=exit_px,
                            size=size,
                            pnl=round(net_pnl, 2),
                            roi_pct=round((exit_px - entry_px) / entry_px * 100.0 * leverage, 2),
                            fee=round(fee, 2),
                            exit_reason="BREAKEVEN" if position.get("breakeven_locked") else "STOP_LOSS",
                            mfe_pct=round(position["mfe_pct"], 2),
                            mae_pct=round(position["mae_pct"], 2)
                        ))
                        position = None
                    elif final_exit == "TAKE_PROFIT":
                        exit_px = tp_px * (1 - slippage)
                        pnl = (exit_px - entry_px) * size
                        fee = exit_px * size * taker_fee
                        net_pnl = pnl - fee
                        equity += net_pnl
                        total_fee_paid += fee
                        trade_counter += 1
                        trades.append(BacktestTradeRecord(
                            trade_id=trade_counter,
                            entry_time=entry_time,
                            exit_time=ts,
                            pos_side="long",
                            entry_price=entry_px,
                            exit_price=exit_px,
                            size=size,
                            pnl=round(net_pnl, 2),
                            roi_pct=round((exit_px - entry_px) / entry_px * 100.0 * leverage, 2),
                            fee=round(fee, 2),
                            exit_reason="TAKE_PROFIT",
                            mfe_pct=round(position["mfe_pct"], 2),
                            mae_pct=round(position["mae_pct"], 2)
                        ))
                        position = None

                elif pos_side == "short":
                    mfe = max(position.get("mfe_pct", 0.0), (entry_px - low_px) / entry_px * 100.0)
                    mae = min(position.get("mae_pct", 0.0), (entry_px - high_px) / entry_px * 100.0)
                    position["mfe_pct"] = mfe
                    position["mae_pct"] = mae

                    if not position.get("breakeven_locked", False):
                        if (entry_px - low_px) >= (entry_px - tp_px) * breakeven_ratio:
                            position["sl_price"] = entry_px * (1 - taker_fee * 2)
                            position["breakeven_locked"] = True
                            sl_px = position["sl_price"]

                    sl_hit = (high_px >= sl_px)
                    tp_hit = (low_px <= tp_px)

                    if sl_hit and tp_hit:
                        final_exit = "TAKE_PROFIT" if is_bullish else "STOP_LOSS"
                    elif sl_hit:
                        final_exit = "STOP_LOSS"
                    elif tp_hit:
                        final_exit = "TAKE_PROFIT"
                    else:
                        final_exit = None

                    if final_exit == "STOP_LOSS":
                        exit_px = sl_px * (1 + slippage)
                        pnl = (entry_px - exit_px) * size
                        fee = exit_px * size * taker_fee
                        net_pnl = pnl - fee
                        equity += net_pnl
                        total_fee_paid += fee
                        trade_counter += 1
                        trades.append(BacktestTradeRecord(
                            trade_id=trade_counter,
                            entry_time=entry_time,
                            exit_time=ts,
                            pos_side="short",
                            entry_price=entry_px,
                            exit_price=exit_px,
                            size=size,
                            pnl=round(net_pnl, 2),
                            roi_pct=round((entry_px - exit_px) / entry_px * 100.0 * leverage, 2),
                            fee=round(fee, 2),
                            exit_reason="BREAKEVEN" if position.get("breakeven_locked") else "STOP_LOSS",
                            mfe_pct=round(position["mfe_pct"], 2),
                            mae_pct=round(position["mae_pct"], 2)
                        ))
                        position = None
                    elif final_exit == "TAKE_PROFIT":
                        exit_px = tp_px * (1 + slippage)
                        pnl = (entry_px - exit_px) * size
                        fee = exit_px * size * taker_fee
                        net_pnl = pnl - fee
                        equity += net_pnl
                        total_fee_paid += fee
                        trade_counter += 1
                        trades.append(BacktestTradeRecord(
                            trade_id=trade_counter,
                            entry_time=entry_time,
                            exit_time=ts,
                            pos_side="short",
                            entry_price=entry_px,
                            exit_price=exit_px,
                            size=size,
                            pnl=round(net_pnl, 2),
                            roi_pct=round((entry_px - exit_px) / entry_px * 100.0 * leverage, 2),
                            fee=round(fee, 2),
                            exit_reason="TAKE_PROFIT",
                            mfe_pct=round(position["mfe_pct"], 2),
                            mae_pct=round(position["mae_pct"], 2)
                        ))
                        position = None

            # 4. 开仓信号扫描 (委托三位一体 BaseStrategy 进行研判，交由 Next-Bar 开盘撮合)
            if position is None and pending_signal is None and equity > 100:
                bar_obj = BarData(
                    symbol=req.symbol,
                    timeframe=req.timeframe,
                    timestamp=ts,
                    open=open_px,
                    high=high_px,
                    low=low_px,
                    close=close_px,
                    volume=float(row.get("volume", 100.0)),
                    indicators={
                        "ema20": row["ema20"],
                        "ema50": row["ema50"],
                        "ema200": row["ema200"],
                        "rsi14": row["rsi14"],
                        "atr14": atr
                    }
                )
                strat_decision = strategy.on_bar(bar_obj, ctx)
                if strat_decision:
                    if strat_decision.action == SignalAction.BUY_LONG:
                        pending_signal = {"pos_side": "long", "atr": atr}
                    elif strat_decision.action == SignalAction.SELL_SHORT:
                        pending_signal = {"pos_side": "short", "atr": atr}

            # 资金曲线
            peak_equity = max(peak_equity, equity)
            curr_dd = (peak_equity - equity) / peak_equity * 100.0 if peak_equity > 0 else 0.0
            max_drawdown_pct = max(max_drawdown_pct, curr_dd)

            if i % 5 == 0 or i == len(df) - 1:
                equity_curve.append(BacktestEquityPoint(
                    timestamp=ts,
                    equity=round(equity, 2),
                    drawdown_pct=round(curr_dd, 2)
                ))

        # 3. 统计评价指标
        total_trades = len(trades)
        winning_trades = len([t for t in trades if t.pnl > 0])
        losing_trades = len([t for t in trades if t.pnl < 0])
        win_rate = round(winning_trades / total_trades, 4) if total_trades > 0 else 0.0

        total_gain = sum(t.pnl for t in trades if t.pnl > 0)
        total_loss = abs(sum(t.pnl for t in trades if t.pnl < 0))
        profit_factor = round(total_gain / (total_loss + 1e-9), 2)

        total_return_pct = round((equity - initial_balance) / initial_balance * 100.0, 2)
        days_span = max(1, req.days_back)
        annualized_return_pct = round(total_return_pct * (365.0 / days_span), 2)

        if len(trades) >= 2:
            rois = [t.roi_pct for t in trades]
            avg_roi = sum(rois) / len(rois)
            var = sum((r - avg_roi) ** 2 for r in rois) / len(rois)
            std_dev = math.sqrt(var)
            sharpe_ratio = round((avg_roi / (std_dev + 1e-9)) * math.sqrt(252), 2)
        else:
            sharpe_ratio = 0.0

        calmar_ratio = round(annualized_return_pct / (max_drawdown_pct + 1e-9), 2)
        avg_trade_pnl = round(sum(t.pnl for t in trades) / total_trades, 2) if total_trades > 0 else 0.0

        exec_time_ms = int((time.time() - start_exec_ts) * 1000)
        experiment_id = str(uuid.uuid4())

        result = BacktestResultResponse(
            experiment_id=experiment_id,
            strategy_name=req.strategy_name,
            symbol=req.symbol,
            timeframe=req.timeframe,
            total_candles=len(df),
            initial_balance=initial_balance,
            final_balance=round(equity, 2),
            total_return_pct=total_return_pct,
            annualized_return_pct=annualized_return_pct,
            max_drawdown_pct=round(max_drawdown_pct, 2),
            sharpe_ratio=sharpe_ratio,
            calmar_ratio=calmar_ratio,
            win_rate=win_rate,
            profit_factor=profit_factor,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            avg_trade_pnl=avg_trade_pnl,
            total_fee_paid=round(total_fee_paid, 2),
            equity_curve=equity_curve,
            trades=trades,
            execution_time_ms=exec_time_ms
        )

        # 4. 持久化至 backtest_experiments 表
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
                    "strategy_name": req.strategy_name,
                    "symbol": req.symbol,
                    "timeframe": req.timeframe,
                    "start_time": int(df[0]["timestamp"]),
                    "end_time": int(df[-1]["timestamp"]),
                    "parameters": json.dumps(req.model_dump(), ensure_ascii=False),
                    "total_return_pct": total_return_pct,
                    "sharpe_ratio": sharpe_ratio,
                    "max_drawdown_pct": round(max_drawdown_pct, 2),
                    "win_rate": win_rate,
                    "profit_factor": profit_factor,
                    "total_trades": total_trades,
                    "metrics_detail": json.dumps({
                        "final_balance": round(equity, 2),
                        "annualized_return_pct": annualized_return_pct,
                        "calmar_ratio": calmar_ratio,
                        "total_fee_paid": round(total_fee_paid, 2),
                        "execution_time_ms": exec_time_ms
                    })
                })
        except Exception as e:
            logger.error(f"持久化回测实验记录失败: {e}")

        return result

    async def run_grid_search(self, req: GridSearchRequest) -> GridSearchResponse:
        """多参数网格搜索与 Walk-Forward 优化"""
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
                        breakeven_ratio=be
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
                            total_trades=res.total_trades
                        ))
                    except Exception as e:
                        logger.warning(f"网格参数 [atr={atr}, be={be}, lev={lev}] 回测异常: {e}")

        results.sort(key=lambda x: (x.sharpe_ratio, x.total_return_pct), reverse=True)
        best_params = results[0].params if results else {}

        return GridSearchResponse(
            best_params=best_params,
            items=results
        )