"""
OKX-Dog 个人量化智能交易系统 - L1 毫秒级流式微观狙击引擎 (The Sniper)
模块: okx-dog-backend/services/micro_sniper.py
实现 0 Token 消耗的 WebSocket 毫秒级微观结构指标计算 (OBI/CVD/OI Surge/清算监控)
以及四大黄金异动触发器 (Extreme Funding, OI Surge, Liquidity Sweep, Liquidation Cascade)
"""

import asyncio
import logging
import time
import uuid
from collections import deque
from typing import Any, Callable, Coroutine, Deque, Dict, List, Optional, Tuple

from models import (
    MarketAnomalyEvent,
    MarketAnomalyTriggerType,
    MicrostructureMetrics,
    TickerData,
)

logger = logging.getLogger("okx_dog.micro_sniper")


class MicroSniperEngine:
    """
    L1 毫秒级微观特征计算与异动拦截引擎
    - 纯 Python / collections.deque 极速环形内存缓冲 (微秒级)
    - 0 Token 消耗，日常静默守护
    - 毫秒级捕获结构性异动，生成 MarketAnomalyEvent 条件唤醒 L2 多智能体
    """

    def __init__(self, max_buffer_len: int = 300):
        self.max_buffer_len = max_buffer_len

        # 内存环形队列存储: symbol -> deque
        self.order_book_buffers: Dict[str, Deque[Tuple[float, float, float]]] = {}  # (ts, bids_vol_10, asks_vol_10)
        self.trade_buffers: Dict[str, Deque[Tuple[float, float, str]]] = {}        # (ts, vol, side: 'buy'/'sell')
        self.oi_buffers: Dict[str, Deque[Tuple[float, float]]] = {}                # (ts, open_interest)
        self.funding_rates: Dict[str, Tuple[float, float]] = {}                    # symbol -> (ts, rate)
        self.liquidation_buffers: Dict[str, Deque[Tuple[float, float, str]]] = {}  # (ts, notional_usdt, side)
        self.price_history: Dict[str, Deque[Tuple[float, float]]] = {}             # (ts, price)

        # 节流与冷却控制 (防止高频震荡连续唤醒 LLM)
        self.last_trigger_ts: Dict[str, float] = {}
        self.min_cooldown_seconds: float = 900.0  # 单标的默认 15 分钟节流

        # 异步回调监听列表
        self._anomaly_handlers: List[Callable[[MarketAnomalyEvent], Coroutine[Any, Any, None]]] = []
        self._lock = asyncio.Lock()

    def register_anomaly_handler(self, handler: Callable[[MarketAnomalyEvent], Coroutine[Any, Any, None]]):
        """注册异动事件触发时的异步回调 (如 AutoPilot 或 AI 桥接器)"""
        self._anomaly_handlers.append(handler)
        logger.info(f"[MicroSniper] 已注册异动监听回调: {handler.__name__ if hasattr(handler, '__name__') else str(handler)}")

    def push_order_book_snapshot(self, symbol: str, bids: List[List[float]], asks: List[List[float]]):
        """
        接收 OKX L2 深度前 10 档快照并推入环形队列
        bids / asks: [[price, sz], [price, sz], ...]
        """
        now = time.time()
        if symbol not in self.order_book_buffers:
            self.order_book_buffers[symbol] = deque(maxlen=self.max_buffer_len)

        # 取前 10 档聚合深度
        bids_top10_vol = sum(float(b[1]) for b in bids[:10]) if bids else 0.0
        asks_top10_vol = sum(float(a[1]) for a in asks[:10]) if asks else 0.0

        self.order_book_buffers[symbol].append((now, bids_top10_vol, asks_top10_vol))

    def push_trade(self, symbol: str, price: float, sz: float, side: str):
        """接收实时逐笔成交 Trade"""
        now = time.time()
        if symbol not in self.trade_buffers:
            self.trade_buffers[symbol] = deque(maxlen=self.max_buffer_len * 5)
        if symbol not in self.price_history:
            self.price_history[symbol] = deque(maxlen=self.max_buffer_len)

        self.trade_buffers[symbol].append((now, sz, side.lower()))
        self.price_history[symbol].append((now, price))

    def push_open_interest(self, symbol: str, oi: float):
        """接收持仓量 OI 快照"""
        now = time.time()
        if symbol not in self.oi_buffers:
            self.oi_buffers[symbol] = deque(maxlen=self.max_buffer_len)
        self.oi_buffers[symbol].append((now, oi))

    def push_funding_rate(self, symbol: str, rate: float):
        """接收资金费率推送"""
        now = time.time()
        self.funding_rates[symbol] = (now, rate)

    def push_liquidation(self, symbol: str, notional_usdt: float, side: str):
        """接收 OKX 清算爆仓事件"""
        now = time.time()
        if symbol not in self.liquidation_buffers:
            self.liquidation_buffers[symbol] = deque(maxlen=self.max_buffer_len)
        self.liquidation_buffers[symbol].append((now, notional_usdt, side))

    def compute_metrics(self, symbol: str) -> MicrostructureMetrics:
        """
        毫秒级提取微观结构指标 (OBI, CVD, 15m OI Surge, Funding Rate)
        """
        now = time.time()
        now_ms = int(now * 1000)

        # 1. 计算 OBI (Order Book Imbalance)
        obi = 0.0
        if symbol in self.order_book_buffers and self.order_book_buffers[symbol]:
            _, b_vol, a_vol = self.order_book_buffers[symbol][-1]
            total_vol = b_vol + a_vol
            if total_vol > 0:
                obi = round((b_vol - a_vol) / total_vol, 4)

        # 2. 计算 CVD (Cumulative Volume Delta - 过去 15 分钟/900秒)
        cvd = 0.0
        if symbol in self.trade_buffers:
            cutoff = now - 900.0
            trades = [t for t in self.trade_buffers[symbol] if t[0] >= cutoff]
            buy_vol = sum(t[1] for t in trades if t[2] in ["buy", "b"])
            sell_vol = sum(t[1] for t in trades if t[2] in ["sell", "s"])
            cvd = round(buy_vol - sell_vol, 4)

        # 3. 计算 15m OI Surge Rate (%)
        oi_surge_pct = 0.0
        if symbol in self.oi_buffers and len(self.oi_buffers[symbol]) >= 2:
            latest_oi = self.oi_buffers[symbol][-1][1]
            cutoff = now - 900.0
            # 找到 15m 前最早的一笔 OI
            past_ois = [item for item in self.oi_buffers[symbol] if item[0] <= cutoff]
            base_oi = past_ois[-1][1] if past_ois else self.oi_buffers[symbol][0][1]
            if base_oi > 0:
                oi_surge_pct = round(((latest_oi - base_oi) / base_oi) * 100.0, 2)

        # 4. 计算年化资金费率
        annualized_fr = 0.0
        if symbol in self.funding_rates:
            _, fr = self.funding_rates[symbol]
            # 8小时一期，一年 365 * 3 = 1095 期
            annualized_fr = round(fr * 3 * 365 * 100.0, 2)

        # 5. 近 5 分钟清算爆仓名义金额
        recent_liqs = 0.0
        if symbol in self.liquidation_buffers:
            liq_cutoff = now - 300.0
            recent_liqs = sum(liq[1] for liq in self.liquidation_buffers[symbol] if liq[0] >= liq_cutoff)

        return MicrostructureMetrics(
            symbol=symbol,
            timestamp=now_ms,
            order_book_imbalance=obi,
            cumulative_volume_delta=cvd,
            oi_surge_pct=oi_surge_pct,
            funding_rate_annualized=annualized_fr,
            recent_liquidations_usdt=round(recent_liqs, 2)
        )

    async def evaluate_triggers(self, symbol: str, ticker: Optional[TickerData] = None) -> Optional[MarketAnomalyEvent]:
        """
        多维黄金异动检测中枢
        若捕获到结构性异动且不在冷却期内，触发异步广播并返回 MarketAnomalyEvent
        """
        now = time.time()
        last_ts = self.last_trigger_ts.get(symbol, 0.0)

        metrics = self.compute_metrics(symbol)
        detected_trigger: Optional[Tuple[MarketAnomalyTriggerType, str, str]] = None

        # -------------------------------------------------------------
        # 异动 1: 极端资金费率与持仓量背离 (轧空/轧多 Squeeze)
        # 条件: 年化费率绝对值 >= 30% 或 (费率倒挂且持仓量在增加)
        # -------------------------------------------------------------
        if abs(metrics.funding_rate_annualized) >= 30.0:
            direction = "空头" if metrics.funding_rate_annualized < 0 else "多头"
            reason = f"资金费率年化达 {metrics.funding_rate_annualized:.1f}% 处于极端水平，市场{direction}杠杆极度拥挤，警惕清算反杀"
            detected_trigger = (MarketAnomalyTriggerType.EXTREME_FUNDING_DIVERGENCE, "CRITICAL", reason)

        # -------------------------------------------------------------
        # 异动 2: 15分钟持仓量暴增 (OI Surge Breakout)
        # 条件: 15m 内 OI 突增 >= 5.0%
        # -------------------------------------------------------------
        elif abs(metrics.oi_surge_pct) >= 5.0:
            reason = f"15分钟内持仓量 (OI) 异动突增 {metrics.oi_surge_pct:+.1f}%，大资金正在大举建仓布局突破"
            detected_trigger = (MarketAnomalyTriggerType.OI_SURGE_BREAKOUT, "WARNING", reason)

        # -------------------------------------------------------------
        # 异动 3: 流动性掠夺与假跌破/假突破 (Liquidity Sweep)
        # 条件: 盘口 OBI 极度失衡 (>= 0.6 或 <= -0.6) 且 CVD 出现反向背离
        # -------------------------------------------------------------
        elif abs(metrics.order_book_imbalance) >= 0.60:
            side_desc = "强劲买盘托单" if metrics.order_book_imbalance > 0 else "沉重卖盘压单"
            reason = f"订单簿前10档失衡率达 {metrics.order_book_imbalance:+.2f} ({side_desc})，盘口出现明显流动性吸收信号"
            detected_trigger = (MarketAnomalyTriggerType.LIQUIDITY_SWEEP_REVERSAL, "WARNING", reason)

        # -------------------------------------------------------------
        # 异动 4: 密集清算爆仓踩踏 (Massive Liquidation Cascade)
        # 条件: 近5分钟爆仓名义价值超过 300,000 USDT
        # -------------------------------------------------------------
        elif metrics.recent_liquidations_usdt >= 300_000.0:
            reason = f"近5分钟发生密集清算爆仓 (${metrics.recent_liquidations_usdt:,.0f} USDT)，动能消耗殆尽，存在均值回归机会"
            detected_trigger = (MarketAnomalyTriggerType.MASSIVE_LIQUIDATION_CASCADE, "CRITICAL", reason)

        if not detected_trigger:
            return None

        trigger_type, severity, trigger_reason = detected_trigger

        # 检查节流冷却期 (同一标的 15 分钟内最多触发 1 次，CRITICAL 可缩短至 5 分钟)
        cooldown_needed = 300.0 if severity == "CRITICAL" else self.min_cooldown_seconds
        if (now - last_ts) < cooldown_needed:
            logger.debug(f"[MicroSniper] 标的 {symbol} 触发 {trigger_type} 但处于冷却期中 (剩余 {int(cooldown_needed - (now - last_ts))}s)，静默跳过")
            return None

        # 记录本次触发时间戳
        self.last_trigger_ts[symbol] = now

        event = MarketAnomalyEvent(
            event_id=str(uuid.uuid4()),
            symbol=symbol,
            timestamp=int(now * 1000),
            trigger_type=trigger_type,
            severity=severity,
            metrics_snapshot=metrics,
            reason=trigger_reason
        )

        logger.warning(f"🎯【L1 狙击引擎捕获异动】标的 {symbol} 触发 [{trigger_type.value}] -> {trigger_reason}")

        # 异步广播所有回调
        for handler in self._anomaly_handlers:
            try:
                asyncio.create_task(handler(event))
            except Exception as e:
                logger.error(f"[MicroSniper] 执行异动回调异常: {e}")

        return event


# 单例微观狙击引擎
micro_sniper = MicroSniperEngine()
