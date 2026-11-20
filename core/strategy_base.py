"""
OKX-Dog 个人量化智能交易系统 - “三位一体”统一策略抽象基类与上下文协议
模块: okx-dog-backend/core/strategy_base.py
角色: 后端架构师 (Backend Architect) / 首席量化架构师 (Chief Quant Architect)

核心理念:
1. Research-Paper-Live 一体化: 同一份策略业务逻辑，无需修改即可在历史回测、沙盒模拟与实盘中无缝运行。
2. 依赖倒置与上下文隔离: 策略只面向抽象的 StrategyContext 交互，由具体运行环境注入 BacktestContext 或 LiveContext。
3. 严格契约对齐: 发单参数、技术指标、持仓快照与 contracts 统一事实源强类型绑定。
"""

from __future__ import annotations

import abc
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type, Union

from models import (
    InstType,
    OrderSide,
    OrderType,
    PosSide,
    SignalAction,
    TickerData,
)

logger = logging.getLogger("okx_dog.strategy_base")


@dataclass
class BarData:
    """标准单根 K 线数据"""
    symbol: str
    timeframe: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    funding_rate: float = 0.0
    indicators: Dict[str, float] = field(default_factory=dict)


@dataclass
class StrategyDecision:
    """策略决策动作"""
    action: SignalAction
    symbol: str
    confidence: float = 1.0
    suggested_price: Optional[float] = None
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    trailing_atr_mult: Optional[float] = None
    risk_pct: Optional[float] = None
    disable_fixed_tp: bool = False
    disable_breakeven: bool = False
    reason: str = ""
    order_type: OrderType = OrderType.POST_ONLY
    metadata: Dict[str, Any] = field(default_factory=dict)


class StrategyContext(abc.ABC):
    """策略运行时上下文抽象接口"""

    @property
    @abc.abstractmethod
    def symbol(self) -> str:
        """标的代码"""
        pass

    @property
    @abc.abstractmethod
    def timeframe(self) -> str:
        """运行主周期"""
        pass

    @property
    @abc.abstractmethod
    def equity(self) -> float:
        """当前账户净值 (USDT)"""
        pass

    @property
    @abc.abstractmethod
    def is_live(self) -> bool:
        """是否为实盘环境"""
        pass

    @abc.abstractmethod
    async def buy(
        self,
        price: Optional[float],
        size: float,
        pos_side: PosSide = PosSide.LONG,
        order_type: OrderType = OrderType.POST_ONLY,
        tp_price: Optional[float] = None,
        sl_price: Optional[float] = None,
    ) -> Any:
        """提交买入/做多委托"""
        pass

    @abc.abstractmethod
    async def sell(
        self,
        price: Optional[float],
        size: float,
        pos_side: PosSide = PosSide.SHORT,
        order_type: OrderType = OrderType.POST_ONLY,
        tp_price: Optional[float] = None,
        sl_price: Optional[float] = None,
    ) -> Any:
        """提交卖出/做空委托"""
        pass

    @abc.abstractmethod
    async def close_position(self, pos_side: PosSide) -> Any:
        """平仓指定方向头寸"""
        pass


class BaseStrategy(abc.ABC):
    """
    量化策略统一抽象基类:
    所有在系统中研发的策略均继承此类，自动获得跨环境执行能力
    """

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        self.name: str = name
        self.params: Dict[str, Any] = params or {}
        self.is_initialized: bool = False

    def on_init(self, context: StrategyContext) -> None:
        """策略初始化事件 (加载参数与指标状态)"""
        self.is_initialized = True
        logger.info(f"🚀 [Strategy: {self.name}] 初始化完成, 参数: {self.params}")

    @abc.abstractmethod
    def on_bar(self, bar: BarData, context: StrategyContext) -> Optional[StrategyDecision]:
        """
        每根 K 线闭合时触发的研判事件:
        返回 StrategyDecision 表示触发下单/平仓意图，返回 None 表示维持观望
        """
        pass

    def on_tick(self, tick: TickerData, context: StrategyContext) -> Optional[StrategyDecision]:
        """
        实时 Tick 行情到达时触发 (用于高频微观止损/止盈/盘口追单)
        默认不作处理，子类按需重写
        """
        return None


# =============================================================================
# 内置参考实现: 标准三位一体 EMA 趋势突破双均线策略
# =============================================================================

class EMABreakoutStrategy(BaseStrategy):
    """
    标准 EMA 双均线动量突破策略 (三位一体参考范例):
    - 当 EMA(fast) 上穿 EMA(slow) 且 RSI 处于健康多头区间时做多
    - 当 EMA(fast) 下穿 EMA(slow) 且 RSI 处于弱势空头区间时做空
    - 结合 ATR 自动生成自适应动态止损
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        default_params = {
            "fast_period": 20,
            "slow_period": 50,
            "rsi_buy_min": 50,
            "rsi_buy_max": 70,
            "rsi_sell_min": 30,
            "rsi_sell_max": 50,
            "atr_multiplier": 2.0,
        }
        if params:
            default_params.update(params)
        super().__init__(name="EMA_BREAKOUT", params=default_params)

        self._prev_fast: Optional[float] = None
        self._prev_slow: Optional[float] = None

    def on_bar(self, bar: BarData, context: StrategyContext) -> Optional[StrategyDecision]:
        fast = bar.indicators.get(f"ema{self.params['fast_period']}") or bar.indicators.get("ema20")
        slow = bar.indicators.get(f"ema{self.params['slow_period']}") or bar.indicators.get("ema50")
        rsi = bar.indicators.get("rsi14", 50.0)
        atr = bar.indicators.get("atr14", bar.close * 0.015)

        if fast is None or slow is None:
            return None

        decision = None

        if self._prev_fast is not None and self._prev_slow is not None:
            # 1. 金叉做多信号
            if self._prev_fast <= self._prev_slow and fast > slow:
                if self.params["rsi_buy_min"] <= rsi <= self.params["rsi_buy_max"]:
                    sl_dist = atr * self.params["atr_multiplier"]
                    decision = StrategyDecision(
                        action=SignalAction.BUY_LONG,
                        symbol=bar.symbol,
                        confidence=0.85,
                        suggested_price=bar.close,
                        stop_loss_price=round(bar.close - sl_dist, 2),
                        take_profit_price=round(bar.close + sl_dist * 2.0, 2),
                        reason=f"EMA{self.params['fast_period']} 上穿 EMA{self.params['slow_period']} 金叉 (RSI={rsi:.1f})",
                        order_type=OrderType.POST_ONLY
                    )

            # 2. 死叉做空信号
            elif self._prev_fast >= self._prev_slow and fast < slow:
                if self.params["rsi_sell_min"] <= rsi <= self.params["rsi_sell_max"]:
                    sl_dist = atr * self.params["atr_multiplier"]
                    decision = StrategyDecision(
                        action=SignalAction.SELL_SHORT,
                        symbol=bar.symbol,
                        confidence=0.85,
                        suggested_price=bar.close,
                        stop_loss_price=round(bar.close + sl_dist, 2),
                        take_profit_price=round(bar.close - sl_dist * 2.0, 2),
                        reason=f"EMA{self.params['fast_period']} 下穿 EMA{self.params['slow_period']} 死叉 (RSI={rsi:.1f})",
                        order_type=OrderType.POST_ONLY
                    )

        self._prev_fast = fast
        self._prev_slow = slow
        return decision


class DonchianTrendStrategy(BaseStrategy):
    """
    极简 4H/1D 低频趋势突破策略 (主引擎 B · 严格 ≤5 核心参数，100% 同构自计算指标):
    - 突破前 entry_n 根 K 线最高价 (且位于宏观 trend_ema 之上) -> 顺势做多
    - 跌破前 entry_n 根 K 线最低价 (且位于宏观 trend_ema 之下) -> 顺势做空
    - 离场采用宽幅 ATR 吊灯移动止损 (atr_sl_mult 倍 ATR) + exit_n 反向通道保护，不设 1.2% 过早固定止盈与保本锁死
    - 全部滚动通道、EMA 与 Wilder ATR 在策略内部基于逐根 BarData 自计算，保证回测与实盘零指标漂移
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        default_params: Dict[str, Any] = {
            "entry_n": 24,          # 入场唐奇安通道周期
            "exit_n": 12,           # 反向离场通道周期
            "atr_sl_mult": 3.0,     # 宽幅 ATR 吊灯移动止损倍数
            "trend_ema": 100,       # 宏观趋势过滤 EMA 周期 (0 表示关闭)
            "risk_pct": 0.01,       # 单笔固定风险预算 (总权益 1.0%)
        }
        if params:
            default_params.update(params)
        super().__init__(name="DONCHIAN_TREND", params=default_params)

        self._entry_n = max(5, int(self.params["entry_n"]))
        self._exit_n = max(3, int(self.params["exit_n"]))
        self._atr_mult = max(1.0, float(self.params["atr_sl_mult"]))
        self._trend_ema_len = max(0, int(self.params["trend_ema"]))
        self._risk_pct = max(0.002, min(0.03, float(self.params["risk_pct"])))

        max_win = max(self._entry_n, self._exit_n) + 2
        self._highs: deque = deque(maxlen=max_win)
        self._lows: deque = deque(maxlen=max_win)
        self._prev_close: Optional[float] = None
        self._atr14: Optional[float] = None
        self._tr_seed: List[float] = []
        self._trend_ema_val: Optional[float] = None
        self._bar_count: int = 0

    def _update_internal_indicators(self, bar: BarData) -> None:
        self._bar_count += 1
        # 1. Wilder ATR(14)
        if self._prev_close is None:
            tr = bar.high - bar.low
        else:
            tr = max(
                bar.high - bar.low,
                abs(bar.high - self._prev_close),
                abs(bar.low - self._prev_close),
            )
        if self._atr14 is None:
            self._tr_seed.append(tr)
            if len(self._tr_seed) >= 14:
                self._atr14 = sum(self._tr_seed) / len(self._tr_seed)
        else:
            self._atr14 = (self._atr14 * 13.0 + tr) / 14.0

        # 2. 宏观趋势 EMA
        if self._trend_ema_len > 0:
            alpha = 2.0 / (self._trend_ema_len + 1.0)
            if self._trend_ema_val is None:
                self._trend_ema_val = bar.close
            else:
                self._trend_ema_val = alpha * bar.close + (1.0 - alpha) * self._trend_ema_val

        self._prev_close = bar.close

    def on_bar(self, bar: BarData, context: StrategyContext) -> Optional[StrategyDecision]:
        # 在把当前 bar 加入通道前，先提取「前 N 根已收盘 K 线」的唐奇安通道边界 (严格杜绝未来函数)
        prev_entry_high: Optional[float] = None
        prev_entry_low: Optional[float] = None
        prev_exit_high: Optional[float] = None
        prev_exit_low: Optional[float] = None

        if len(self._highs) >= self._entry_n:
            recent_entry_h = list(self._highs)[-self._entry_n:]
            recent_entry_l = list(self._lows)[-self._entry_n:]
            prev_entry_high = max(recent_entry_h)
            prev_entry_low = min(recent_entry_l)

        if len(self._highs) >= self._exit_n:
            recent_exit_h = list(self._highs)[-self._exit_n:]
            recent_exit_l = list(self._lows)[-self._exit_n:]
            prev_exit_high = max(recent_exit_h)
            prev_exit_low = min(recent_exit_l)

        # 更新内部 ATR / EMA 与通道缓存
        self._update_internal_indicators(bar)
        self._highs.append(bar.high)
        self._lows.append(bar.low)

        warmup_bars = max(self._entry_n + 1, 20, min(self._trend_ema_len, 60))
        if self._bar_count < warmup_bars or prev_entry_high is None or prev_entry_low is None:
            return None

        atr = self._atr14 or bar.indicators.get("atr14") or (bar.close * 0.02)
        sl_dist = atr * self._atr_mult
        cur_pos_side = getattr(context, "current_pos_side", None)

        # 1. 若已有持仓，检查反向离场通道 (exit_n) 触发信号
        if cur_pos_side == "long" and prev_exit_low is not None and bar.close < prev_exit_low:
            return StrategyDecision(
                action=SignalAction.CLOSE_POSITION,
                symbol=bar.symbol,
                confidence=0.90,
                suggested_price=bar.close,
                reason=f"跌破 {self._exit_n} 周期唐奇安离场下轨 (${prev_exit_low:.4f})，顺势趋势结束平多",
                order_type=OrderType.MARKET,
            )
        if cur_pos_side == "short" and prev_exit_high is not None and bar.close > prev_exit_high:
            return StrategyDecision(
                action=SignalAction.CLOSE_POSITION,
                symbol=bar.symbol,
                confidence=0.90,
                suggested_price=bar.close,
                reason=f"突破 {self._exit_n} 周期唐奇安离场上轨 (${prev_exit_high:.4f})，顺势趋势结束平空",
                order_type=OrderType.MARKET,
            )

        # 2. 空仓状态扫描对称突破入场信号
        if cur_pos_side is None:
            allow_long = (self._trend_ema_val is None) or (bar.close > self._trend_ema_val)
            allow_short = (self._trend_ema_val is None) or (bar.close < self._trend_ema_val)

            if allow_long and bar.close > prev_entry_high:
                return StrategyDecision(
                    action=SignalAction.BUY_LONG,
                    symbol=bar.symbol,
                    confidence=0.85,
                    suggested_price=bar.close,
                    stop_loss_price=round(max(1e-6, bar.close - sl_dist), 4),
                    take_profit_price=None,
                    trailing_atr_mult=self._atr_mult,
                    risk_pct=self._risk_pct,
                    disable_fixed_tp=True,
                    disable_breakeven=True,
                    reason=f"放量突破 {self._entry_n} 周期上轨 (${prev_entry_high:.4f})，启动宽幅 {self._atr_mult}x ATR 吊灯趋势多单",
                    order_type=OrderType.POST_ONLY,
                )

            if allow_short and bar.close < prev_entry_low:
                return StrategyDecision(
                    action=SignalAction.SELL_SHORT,
                    symbol=bar.symbol,
                    confidence=0.85,
                    suggested_price=bar.close,
                    stop_loss_price=round(bar.close + sl_dist, 4),
                    take_profit_price=None,
                    trailing_atr_mult=self._atr_mult,
                    risk_pct=self._risk_pct,
                    disable_fixed_tp=True,
                    disable_breakeven=True,
                    reason=f"跌破 {self._entry_n} 周期下轨 (${prev_entry_low:.4f})，启动宽幅 {self._atr_mult}x ATR 吊灯趋势空单",
                    order_type=OrderType.POST_ONLY,
                )

        return None


class KeltnerTrendStrategy(BaseStrategy):
    """
    4H 凯尔特纳波动率通道趋势突破策略 (主引擎 B-1 · 严格 4 参数，100% 内部自计算指标):
    - 中轨 = EMA(ema_n)，上下轨 = 中轨 ± band_k * Wilder_ATR(14)
    - 收盘站上上轨 (且位于宏观 trend_ema 之上) -> 顺势做多
    - 收盘跌破下轨 (且位于宏观 trend_ema 之下) -> 顺势做空
    - 离场采用宽幅 ATR 吊灯移动止损 (sl_atr_mult * ATR) + 中轨回归离场，完全释放肥尾趋势利润
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        default_params: Dict[str, Any] = {
            "ema_n": 20,            # 凯尔特纳中轨 EMA 周期
            "band_k": 2.5,          # 通道突破 ATR 宽度倍数
            "sl_atr_mult": 3.0,     # 宽幅 ATR 吊灯移动止损倍数
            "trend_ema": 200,       # 宏观趋势过滤 EMA 周期 (0 表示关闭)
            "risk_pct": 0.01,       # 单笔占趋势组合净值的风险预算 (1.0%)
        }
        if params:
            default_params.update(params)
        super().__init__(name="KELTNER_TREND", params=default_params)

        self._ema_n = max(5, int(self.params["ema_n"]))
        self._band_k = max(0.5, float(self.params["band_k"]))
        self._sl_atr_mult = max(1.0, float(self.params["sl_atr_mult"]))
        self._trend_ema_len = max(0, int(self.params["trend_ema"]))
        self._risk_pct = max(0.002, min(0.05, float(self.params["risk_pct"])))

        self._mid_ema: Optional[float] = None
        self._trend_ema_val: Optional[float] = None
        self._prev_close: Optional[float] = None
        self._atr14: Optional[float] = None
        self._tr_seed: List[float] = []
        self._bar_count: int = 0

    def _update_indicators(self, bar: BarData) -> None:
        self._bar_count += 1
        tr = (bar.high - bar.low) if self._prev_close is None else max(
            bar.high - bar.low,
            abs(bar.high - self._prev_close),
            abs(bar.low - self._prev_close),
        )
        if self._atr14 is None:
            self._tr_seed.append(tr)
            if len(self._tr_seed) >= 14:
                self._atr14 = sum(self._tr_seed) / 14.0
        else:
            self._atr14 = (self._atr14 * 13.0 + tr) / 14.0

        alpha_mid = 2.0 / (self._ema_n + 1.0)
        self._mid_ema = bar.close if self._mid_ema is None else (alpha_mid * bar.close + (1.0 - alpha_mid) * self._mid_ema)

        if self._trend_ema_len > 0:
            alpha_tr = 2.0 / (self._trend_ema_len + 1.0)
            self._trend_ema_val = bar.close if self._trend_ema_val is None else (alpha_tr * bar.close + (1.0 - alpha_tr) * self._trend_ema_val)

        self._prev_close = bar.close

    def on_bar(self, bar: BarData, context: StrategyContext) -> Optional[StrategyDecision]:
        self._update_indicators(bar)
        warmup = max(self._ema_n + 5, 25)
        if self._bar_count < warmup or self._mid_ema is None or self._atr14 is None:
            return None

        atr = self._atr14
        mid = self._mid_ema
        upper = mid + self._band_k * atr
        lower = mid - self._band_k * atr
        sl_dist = atr * self._sl_atr_mult
        cur_pos_side = getattr(context, "current_pos_side", None)

        # 1. 持仓离场：回归中轨结束趋势
        if cur_pos_side == "long" and bar.close < mid:
            return StrategyDecision(
                action=SignalAction.CLOSE_POSITION,
                symbol=bar.symbol,
                confidence=0.90,
                suggested_price=bar.close,
                reason=f"4H 收盘跌破凯尔特纳中轨 EMA{self._ema_n} (${mid:.4f})，趋势结束平多",
                order_type=OrderType.MARKET,
            )
        if cur_pos_side == "short" and bar.close > mid:
            return StrategyDecision(
                action=SignalAction.CLOSE_POSITION,
                symbol=bar.symbol,
                confidence=0.90,
                suggested_price=bar.close,
                reason=f"4H 收盘站上凯尔特纳中轨 EMA{self._ema_n} (${mid:.4f})，趋势结束平空",
                order_type=OrderType.MARKET,
            )

        # 2. 空仓突破入场：对称多空
        if cur_pos_side is None:
            allow_long = (self._trend_ema_val is None) or (bar.close > self._trend_ema_val)
            allow_short = (self._trend_ema_val is None) or (bar.close < self._trend_ema_val)

            if allow_long and bar.close > upper:
                return StrategyDecision(
                    action=SignalAction.BUY_LONG,
                    symbol=bar.symbol,
                    confidence=0.88,
                    suggested_price=bar.close,
                    stop_loss_price=round(max(1e-6, bar.close - sl_dist), 4),
                    take_profit_price=None,
                    trailing_atr_mult=self._sl_atr_mult,
                    risk_pct=self._risk_pct,
                    disable_fixed_tp=True,
                    disable_breakeven=True,
                    reason=f"突破凯尔特纳上轨 (${upper:.4f}, k={self._band_k})，开启宽幅 {self._sl_atr_mult}x ATR 吊灯多单",
                    order_type=OrderType.POST_ONLY,
                )
            if allow_short and bar.close < lower:
                return StrategyDecision(
                    action=SignalAction.SELL_SHORT,
                    symbol=bar.symbol,
                    confidence=0.88,
                    suggested_price=bar.close,
                    stop_loss_price=round(bar.close + sl_dist, 4),
                    take_profit_price=None,
                    trailing_atr_mult=self._sl_atr_mult,
                    risk_pct=self._risk_pct,
                    disable_fixed_tp=True,
                    disable_breakeven=True,
                    reason=f"跌破凯尔特纳下轨 (${lower:.4f}, k={self._band_k})，开启宽幅 {self._sl_atr_mult}x ATR 吊灯空单",
                    order_type=OrderType.POST_ONLY,
                )
        return None


class DualEMAChandelierStrategy(BaseStrategy):
    """
    1D 极简双均线 + 宽幅 ATR 吊灯止损策略 (主引擎 B-2 · 严格 3 参数，100% 内部自计算指标):
    - 快线 EMA(fast_n) 上穿慢线 EMA(slow_n) -> 顺势做多
    - 快线 EMA(fast_n) 下穿慢线 EMA(slow_n) -> 顺势做空
    - 离场由宽幅 ATR 吊灯移动止损 (sl_atr_mult * ATR) 或均线反向交叉驱动，超低换手、极低费率磨损
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        default_params: Dict[str, Any] = {
            "fast_n": 20,           # 快线 EMA 周期
            "slow_n": 50,           # 慢线 EMA 周期
            "sl_atr_mult": 2.5,     # 宽幅 ATR 吊灯移动止损倍数
            "risk_pct": 0.015,      # 单笔占趋势组合净值的风险预算 (1.5%)
        }
        if params:
            default_params.update(params)
        super().__init__(name="DUAL_EMA_TREND", params=default_params)

        self._fast_n = max(3, int(self.params["fast_n"]))
        self._slow_n = max(self._fast_n + 2, int(self.params["slow_n"]))
        self._sl_atr_mult = max(1.0, float(self.params["sl_atr_mult"]))
        self._risk_pct = max(0.002, min(0.05, float(self.params["risk_pct"])))

        self._fast_ema: Optional[float] = None
        self._slow_ema: Optional[float] = None
        self._prev_fast: Optional[float] = None
        self._prev_slow: Optional[float] = None
        self._prev_close: Optional[float] = None
        self._atr14: Optional[float] = None
        self._tr_seed: List[float] = []
        self._bar_count: int = 0

    def _update_indicators(self, bar: BarData) -> None:
        self._bar_count += 1
        tr = (bar.high - bar.low) if self._prev_close is None else max(
            bar.high - bar.low,
            abs(bar.high - self._prev_close),
            abs(bar.low - self._prev_close),
        )
        if self._atr14 is None:
            self._tr_seed.append(tr)
            if len(self._tr_seed) >= 14:
                self._atr14 = sum(self._tr_seed) / 14.0
        else:
            self._atr14 = (self._atr14 * 13.0 + tr) / 14.0

        af = 2.0 / (self._fast_n + 1.0)
        as_ = 2.0 / (self._slow_n + 1.0)
        self._fast_ema = bar.close if self._fast_ema is None else (af * bar.close + (1.0 - af) * self._fast_ema)
        self._slow_ema = bar.close if self._slow_ema is None else (as_ * bar.close + (1.0 - as_) * self._slow_ema)
        self._prev_close = bar.close

    def on_bar(self, bar: BarData, context: StrategyContext) -> Optional[StrategyDecision]:
        prev_f, prev_s = self._fast_ema, self._slow_ema
        self._update_indicators(bar)
        if self._bar_count < max(25, self._fast_n + 5) or prev_f is None or prev_s is None or self._atr14 is None:
            return None

        cur_f, cur_s = self._fast_ema, self._slow_ema
        atr = self._atr14
        sl_dist = atr * self._sl_atr_mult
        cur_pos_side = getattr(context, "current_pos_side", None)

        # 1. 持仓死叉/金叉离场
        if cur_pos_side == "long" and cur_f < cur_s:
            return StrategyDecision(
                action=SignalAction.CLOSE_POSITION,
                symbol=bar.symbol,
                confidence=0.90,
                suggested_price=bar.close,
                reason=f"日线 EMA{self._fast_n} 下穿 EMA{self._slow_n}，趋势反转平多",
                order_type=OrderType.MARKET,
            )
        if cur_pos_side == "short" and cur_f > cur_s:
            return StrategyDecision(
                action=SignalAction.CLOSE_POSITION,
                symbol=bar.symbol,
                confidence=0.90,
                suggested_price=bar.close,
                reason=f"日线 EMA{self._fast_n} 上穿 EMA{self._slow_n}，趋势反转平空",
                order_type=OrderType.MARKET,
            )

        # 2. 空仓突破交叉入场
        if cur_pos_side is None:
            if prev_f <= prev_s and cur_f > cur_s:
                return StrategyDecision(
                    action=SignalAction.BUY_LONG,
                    symbol=bar.symbol,
                    confidence=0.88,
                    suggested_price=bar.close,
                    stop_loss_price=round(max(1e-6, bar.close - sl_dist), 4),
                    take_profit_price=None,
                    trailing_atr_mult=self._sl_atr_mult,
                    risk_pct=self._risk_pct,
                    disable_fixed_tp=True,
                    disable_breakeven=True,
                    reason=f"日线 EMA{self._fast_n} 上穿 EMA{self._slow_n} 金叉，开启宽幅 {self._sl_atr_mult}x ATR 吊灯多单",
                    order_type=OrderType.POST_ONLY,
                )
            if prev_f >= prev_s and cur_f < cur_s:
                return StrategyDecision(
                    action=SignalAction.SELL_SHORT,
                    symbol=bar.symbol,
                    confidence=0.88,
                    suggested_price=bar.close,
                    stop_loss_price=round(bar.close + sl_dist, 4),
                    take_profit_price=None,
                    trailing_atr_mult=self._sl_atr_mult,
                    risk_pct=self._risk_pct,
                    disable_fixed_tp=True,
                    disable_breakeven=True,
                    reason=f"日线 EMA{self._fast_n} 下穿 EMA{self._slow_n} 死叉，开启宽幅 {self._sl_atr_mult}x ATR 吊灯空单",
                    order_type=OrderType.POST_ONLY,
                )
        return None


# 全局三位一体策略注册表 (回测引擎、Walk-Forward 与实盘操盘共用同一工厂)
STRATEGY_REGISTRY: Dict[str, Type[BaseStrategy]] = {
    "EMA_BREAKOUT": EMABreakoutStrategy,
    "DONCHIAN_TREND": DonchianTrendStrategy,
    "KELTNER_TREND": KeltnerTrendStrategy,
    "DUAL_EMA_TREND": DualEMAChandelierStrategy,
}


def create_strategy(strategy_name: str, params: Optional[Dict[str, Any]] = None) -> BaseStrategy:
    """按策略名实例化统一 BaseStrategy 子类"""
    key = str(strategy_name or "KELTNER_TREND").upper().strip()
    cls = STRATEGY_REGISTRY.get(key)
    if cls is None:
        logger.warning(f"未在 STRATEGY_REGISTRY 中找到策略 [{key}]，回退至 KELTNER_TREND")
        cls = KeltnerTrendStrategy
    return cls(params=params)


# =============================================================================
# 回测专用上下文实现 (BacktestContext)
# =============================================================================

class BacktestContext(StrategyContext):
    """回测环境上下文注入"""

    def __init__(self, symbol: str, timeframe: str, initial_equity: float, leverage: int = 3):
        self._symbol = symbol
        self._timeframe = timeframe
        self._equity = initial_equity
        self._leverage = leverage
        self.current_pos_side: Optional[str] = None
        self.submitted_orders: List[Dict[str, Any]] = []

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def timeframe(self) -> str:
        return self._timeframe

    @property
    def equity(self) -> float:
        return self._equity

    @property
    def is_live(self) -> bool:
        return False

    async def buy(
        self,
        price: Optional[float],
        size: float,
        pos_side: PosSide = PosSide.LONG,
        order_type: OrderType = OrderType.POST_ONLY,
        tp_price: Optional[float] = None,
        sl_price: Optional[float] = None,
    ) -> Any:
        record = {
            "side": OrderSide.BUY.value,
            "pos_side": pos_side.value,
            "price": price,
            "size": size,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "timestamp": int(time.time() * 1000)
        }
        self.submitted_orders.append(record)
        return record

    async def sell(
        self,
        price: Optional[float],
        size: float,
        pos_side: PosSide = PosSide.SHORT,
        order_type: OrderType = OrderType.POST_ONLY,
        tp_price: Optional[float] = None,
        sl_price: Optional[float] = None,
    ) -> Any:
        record = {
            "side": OrderSide.SELL.value,
            "pos_side": pos_side.value,
            "price": price,
            "size": size,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "timestamp": int(time.time() * 1000)
        }
        self.submitted_orders.append(record)
        return record

    async def close_position(self, pos_side: PosSide) -> Any:
        record = {
            "action": "CLOSE",
            "pos_side": pos_side.value,
            "timestamp": int(time.time() * 1000)
        }
        self.submitted_orders.append(record)
        return record
