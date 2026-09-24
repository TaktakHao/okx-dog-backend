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
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

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
        slow = bar.indicators.get(f"slow_period") or bar.indicators.get("ema50")
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
