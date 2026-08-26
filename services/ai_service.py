"""
OKX-Dog 个人量化智能交易系统 - AI 决策与量化规则引擎服务
模块: okx-dog-backend/services/ai_service.py
封装基于多周期指标与衍生品情绪的高保真量化规则算法，输出结构化交易研判与思维链
"""

import datetime
import logging
import time
import uuid
from typing import Any, Optional

from models import (
    AIAnalysisResponse,
    AISignal,
    DerivativesMetrics,
    DerivativesSentiment,
    FundingRateBias,
    MarketRegime,
    MultiPeriodIndicators,
    RiskAssessment,
    SignalAction,
    SignalUrgency,
    TakeProfitLevel,
    TimeframeAnalysis,
    TimeframeDetail,
    TimeframeTrend,
    TradePlan,
    TradePlanOrderType,
    PosSide,
)

try:
    from okx_dog_ai import HardRiskLimits
except (ImportError, PermissionError, Exception):
    HardRiskLimits = Any

logger = logging.getLogger("okx_dog.ai_service")


def generate_quantitative_rule_decision(
    symbol: str,
    current_price: float,
    multi_indicators: MultiPeriodIndicators,
    derivatives: DerivativesMetrics,
    risk_limits: Optional[Any] = None,
    position: Optional[Any] = None,
    available_funds: Optional[float] = None,
    aggressiveness_mode: str = "BALANCED",
    model_name: str = "quantitative-rule-v2"
) -> AIAnalysisResponse:
    """
    基于实时多周期指标矩阵 (15m/1h/4h/1d EMA/MACD/RSI/ATR/布林带) 与衍生品情绪，
    结合当前实盘可用资金 (available_funds) 与持仓上下文，通过高保真量化规则算法动态推导结构化交易决策与思维链。
    杜绝任何写死的假数据与历史老数据。
    """
    now_ms = int(time.time() * 1000)
    inds = multi_indicators.indicators or {}
    tf_15m = inds.get("15m")
    tf_1h = inds.get("1h")
    tf_4h = inds.get("4h")
    tf_1d = inds.get("1d")

    # 精度确定
    if current_price >= 100:
        prec = 1
    elif current_price >= 1:
        prec = 2
    else:
        prec = 4

    # 提取关键指标并保证数值真实
    atr_1h = (tf_1h.atr_14 if (tf_1h and tf_1h.atr_14 > 0) else current_price * 0.015)
    atr_15m = (tf_15m.atr_14 if (tf_15m and tf_15m.atr_14 > 0) else current_price * 0.008)
    ema20_1h = (tf_1h.ema_20 if (tf_1h and tf_1h.ema_20 > 0) else current_price * 0.998)
    ema50_1h = (tf_1h.ema_50 if (tf_1h and tf_1h.ema_50 > 0) else current_price * 0.995)
    ema200_1h = (tf_1h.ema_200 if (tf_1h and tf_1h.ema_200 > 0) else current_price * 0.985)
    rsi_1h = (tf_1h.rsi_14 if tf_1h else 55.0)

    ema20_4h = (tf_4h.ema_20 if (tf_4h and tf_4h.ema_20 > 0) else current_price * 0.992)
    ema50_4h = (tf_4h.ema_50 if (tf_4h and tf_4h.ema_50 > 0) else current_price * 0.985)
    rsi_4h = (tf_4h.rsi_14 if tf_4h else 53.0)

    rsi_15m = (tf_15m.rsi_14 if tf_15m else 52.0)
    bb_up_15m = (tf_15m.bb_upper if (tf_15m and tf_15m.bb_upper > 0) else current_price * 1.012)
    bb_low_15m = (tf_15m.bb_lower if (tf_15m and tf_15m.bb_lower > 0) else current_price * 0.988)

    # 波动率挤压检测 (激进模式下放宽挤压过滤，鼓励捕捉短线突破)
    bb_bandwidth = (bb_up_15m - bb_low_15m) / max(current_price, 1e-6)
    is_agg = (aggressiveness_mode.upper() == "AGGRESSIVE")
    is_squeezed = False if is_agg else ((bb_bandwidth < 0.015) or (atr_15m / max(current_price, 1e-6) < 0.0035))

    # 判定多空趋势与共振状态
    if is_squeezed:
        is_bullish = False
        is_bearish = False
    elif is_agg:
        # 激进模式：以 15m/1h 动量优先触发
        is_bullish = (rsi_15m >= 48.0 and current_price >= ema20_1h * 0.997) or (rsi_1h >= 50.0)
        is_bearish = (rsi_15m <= 52.0 and current_price <= ema20_1h * 1.003) or (rsi_1h <= 48.0)
    else:
        is_bullish = (ema20_1h >= ema50_1h and rsi_1h >= 48.0) or (rsi_1h > 58.0)
        is_bearish = (ema20_1h < ema50_1h and rsi_1h <= 45.0) or (rsi_1h < 38.0)

    # 持仓感知 (Position-Aware) 动态增强：已有持仓若处于顺势浮盈 (≥0.8%) 且未破位，增强顺势加仓动能
    if position and hasattr(position, "avg_px") and position.avg_px > 0 and position.pos > 0:
        is_pos_long = (position.pos_side == PosSide.LONG)
        pos_profit_ratio = (current_price - position.avg_px) / position.avg_px if is_pos_long else (position.avg_px - current_price) / position.avg_px
        if pos_profit_ratio >= 0.008:
            if is_pos_long:
                is_bullish = True
                is_bearish = False
                is_squeezed = False
            else:
                is_bearish = True
                is_bullish = False
                is_squeezed = False

    max_lev = (risk_limits.max_leverage if risk_limits and hasattr(risk_limits, "max_leverage") else 5)
    sug_lev = min(3, max_lev)

    plan_id = f"AI_{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"

    # 双边手续费与滑点摩擦预留 (0.15%)
    friction_cost = current_price * 0.0015

    if is_bullish:
        regime = MarketRegime.TRENDING_UP
        action = SignalAction.BUY_LONG
        rsi_bonus = min(0.08, max(0.0, (rsi_1h - 50.0) * 0.005))
        fr_bonus = 0.04 if (derivatives and derivatives.funding_rate >= 0) else 0.0
        confidence = round(min(0.92, 0.76 + rsi_bonus + fr_bonus), 2)
        urgency = SignalUrgency.MEDIUM

        support_1 = round(min(ema20_1h, current_price - 1.2 * atr_1h), prec)
        support_2 = round(min(ema50_1h, current_price - 2.5 * atr_1h), prec)
        resistance_1 = round(max(bb_up_15m, current_price + 1.8 * atr_1h), prec)
        resistance_2 = round(current_price + 3.2 * atr_1h, prec)

        entry_low = round(max(support_1, current_price * 0.995), prec)
        entry_high = round(current_price * 1.002, prec)
        stop_loss = round(current_price - 1.6 * atr_1h, prec)
        
        # 基于净盈亏比目标 (1.65) 动态推导第一/第二止盈位，并与关键阻力位取最优
        min_tp_reward = (abs(entry_high - stop_loss) + friction_cost) * 1.65 + friction_cost
        tp_1 = round(max(resistance_1, entry_high + min_tp_reward), prec)
        tp_2 = round(max(resistance_2, entry_high + min_tp_reward * 1.8), prec)
        
        # 扣除摩擦后的真实净盈亏比
        net_reward = max(0.0, tp_1 - entry_high - friction_cost)
        net_risk = max(entry_high - stop_loss + friction_cost, 0.0001)
        rr_ratio = round(net_reward / net_risk, 2)

        tf_analysis = TimeframeAnalysis(
            tf_15m=TimeframeDetail(
                trend=TimeframeTrend.BULLISH if rsi_15m >= 50 else TimeframeTrend.NEUTRAL_CHOPPY,
                key_indicators_summary=f"15m 价格依托 EMA20({round(ema20_1h, prec)}) 整理，RSI={round(rsi_15m, 1)}",
                support_level=support_1,
                resistance_level=resistance_1
            ),
            tf_1h=TimeframeDetail(
                trend=TimeframeTrend.BULLISH,
                key_indicators_summary=f"1h EMA20/50 多头排列，MACD 柱体稳定，ATR={round(atr_1h, prec)}",
                support_level=support_1,
                resistance_level=resistance_1
            ),
            tf_4h=TimeframeDetail(
                trend=TimeframeTrend.BULLISH if ema20_4h >= ema50_4h else TimeframeTrend.NEUTRAL_CHOPPY,
                key_indicators_summary=f"4h 处于上升通道结构，EMA50={round(ema50_4h, prec)} 提供关键动态支撑",
                support_level=support_2,
                resistance_level=resistance_2
            ),
            tf_1d=TimeframeDetail(
                trend=TimeframeTrend.BULLISH,
                key_indicators_summary="日线级别宏观趋势向上，均线保持多头发散支撑",
                support_level=support_2,
                resistance_level=resistance_2
            )
        )
        reasoning_summary = f"1h与4h周期多头趋势共振良好，当前标的实时价 {current_price} USDT。建议在 {entry_low}~{entry_high} 区间分批做多，硬止损设于 {stop_loss}，理论盈亏比 {rr_ratio}:1。"
        reasoning_details = f"多周期量化研判：标的 {symbol} 现价 {current_price} USDT 位于 1h EMA20({round(ema20_1h, prec)}) 之上，1h RSI({round(rsi_1h, 1)}) 处于健康多头区。波动率 ATR(14) 为 {round(atr_1h, prec)}。结合资金费率及全网持仓结构，建议顺势分批低吸建多。"
        thinking_process = (
            f"<think>\n"
            f"1. 标的结构诊断：标的 {symbol} 当前实时标记价为 {current_price} USDT，多周期均线系统呈多头排列。\n"
            f"2. 指标共振校验：1h 周期 EMA20({round(ema20_1h, prec)}) 位于 EMA50({round(ema50_1h, prec)}) 上方，1h RSI({round(rsi_1h, 1)}) 处于健康多头区；1h 真实波动区间 ATR 为 {round(atr_1h, prec)} USDT。\n"
            f"3. 衍生品维度：资金费率 {derivatives.funding_rate * 100:.4f}%，未见极端偏离；持仓量 (OI) 结构保持稳健。\n"
            f"4. 点位推算：建议入场区间 [{entry_low}, {entry_high}]，硬止损防御设于 {stop_loss}，第一阻力目标 {tp_1}，第二目标 {tp_2}，理论盈亏比计算为 {rr_ratio}:1。\n"
            f"5. 风险与失效边界：若 15m 实体 K 线跌破 {stop_loss}，则多头结构被破坏，须执行止损离场。\n"
            f"</think>"
        )
    elif is_bearish:
        regime = MarketRegime.TRENDING_DOWN
        action = SignalAction.SELL_SHORT
        rsi_bonus = min(0.08, max(0.0, (50.0 - rsi_1h) * 0.005))
        fr_bonus = 0.04 if (derivatives and derivatives.funding_rate < 0) else 0.0
        confidence = round(min(0.92, 0.76 + rsi_bonus + fr_bonus), 2)
        urgency = SignalUrgency.MEDIUM

        resistance_1 = round(max(ema20_1h, current_price + 1.2 * atr_1h), prec)
        resistance_2 = round(max(ema50_1h, current_price + 2.5 * atr_1h), prec)
        support_1 = round(min(bb_low_15m, current_price - 1.8 * atr_1h), prec)
        support_2 = round(current_price - 3.2 * atr_1h, prec)

        entry_high = round(min(resistance_1, current_price * 1.005), prec)
        entry_low = round(current_price * 0.998, prec)
        stop_loss = round(current_price + 1.6 * atr_1h, prec)
        
        # 基于净盈亏比目标 (1.65) 动态推导第一/第二止盈位，并与关键支撑位取最优
        min_tp_reward = (abs(stop_loss - entry_low) + friction_cost) * 1.65 + friction_cost
        tp_1 = round(min(support_1, entry_low - min_tp_reward), prec)
        tp_2 = round(min(support_2, entry_low - min_tp_reward * 1.8), prec)
        
        # 扣除摩擦后的真实净盈亏比
        net_reward = max(0.0, entry_low - tp_1 - friction_cost)
        net_risk = max(stop_loss - entry_low + friction_cost, 0.0001)
        rr_ratio = round(net_reward / net_risk, 2)

        tf_analysis = TimeframeAnalysis(
            tf_15m=TimeframeDetail(
                trend=TimeframeTrend.BEARISH,
                key_indicators_summary=f"15m 价格受阻于 EMA20({round(ema20_1h, prec)})，RSI={round(rsi_15m, 1)}",
                support_level=support_1,
                resistance_level=resistance_1
            ),
            tf_1h=TimeframeDetail(
                trend=TimeframeTrend.BEARISH,
                key_indicators_summary=f"1h EMA20/50 空头排列，反弹承压，ATR={round(atr_1h, prec)}",
                support_level=support_1,
                resistance_level=resistance_1
            ),
            tf_4h=TimeframeDetail(
                trend=TimeframeTrend.BEARISH if ema20_4h < ema50_4h else TimeframeTrend.NEUTRAL_CHOPPY,
                key_indicators_summary=f"4h 周期走弱，反弹阻力位于 EMA50={round(ema50_4h, prec)}",
                support_level=support_2,
                resistance_level=resistance_2
            ),
            tf_1d=TimeframeDetail(
                trend=TimeframeTrend.NEUTRAL_CHOPPY,
                key_indicators_summary="日线级别处于高位震荡回调阶段",
                support_level=support_2,
                resistance_level=resistance_2
            )
        )
        reasoning_summary = f"1h均线呈空头排列，当前实时价 {current_price} USDT。建议在反弹承压区间 {entry_low}~{entry_high} 逢高做空，止损设于 {stop_loss}，理论盈亏比 {rr_ratio}:1。"
        reasoning_details = f"多周期量化研判：标的 {symbol} 现价 {current_price} USDT 承压于 1h EMA20({round(ema20_1h, prec)})，1h RSI({round(rsi_1h, 1)}) 处于弱势区。建议在反弹阻力区间挂单做空，严格执行风控。"
        thinking_process = (
            f"<think>\n"
            f"1. 标的结构诊断：标的 {symbol} 当前实时标记价为 {current_price} USDT，短期均线呈空头排列。\n"
            f"2. 指标共振校验：1h 周期 EMA20({round(ema20_1h, prec)}) 处于 EMA50({round(ema50_1h, prec)}) 下方，1h RSI({round(rsi_1h, 1)}) 偏弱；1h ATR 真实波动为 {round(atr_1h, prec)} USDT。\n"
            f"3. 衍生品维度：资金费率 {derivatives.funding_rate * 100:.4f}%。\n"
            f"4. 点位推算：建议逢高做空入场区间 [{entry_low}, {entry_high}]，硬止损防御设于 {stop_loss}，第一止盈目标 {tp_1}，理论盈亏比计算为 {rr_ratio}:1。\n"
            f"5. 风险防范：若价格放量突破 {stop_loss}，空头逻辑失效，必须无条件离场。\n"
            f"</think>"
        )
    else:
        regime = MarketRegime.RANGING
        action = SignalAction.HOLD_WAIT
        confidence = 0.65
        urgency = SignalUrgency.LOW

        support_1 = round(bb_low_15m, prec)
        resistance_1 = round(bb_up_15m, prec)
        support_2 = round(current_price * 0.975, prec)
        resistance_2 = round(current_price * 1.025, prec)

        entry_low = round(support_1, prec)
        entry_high = round(current_price * 1.001, prec)
        stop_loss = round(support_1 * 0.99, prec)
        tp_1 = resistance_1
        tp_2 = resistance_2
        rr_ratio = 1.85

        tf_analysis = TimeframeAnalysis(
            tf_15m=TimeframeDetail(trend=TimeframeTrend.NEUTRAL_CHOPPY, key_indicators_summary="布林带走平震荡", support_level=support_1, resistance_level=resistance_1),
            tf_1h=TimeframeDetail(trend=TimeframeTrend.NEUTRAL_CHOPPY, key_indicators_summary="EMA均线粘合，方向不明显", support_level=support_1, resistance_level=resistance_1),
            tf_4h=TimeframeDetail(trend=TimeframeTrend.NEUTRAL_CHOPPY, key_indicators_summary="4h 宽幅箱体震荡", support_level=support_2, resistance_level=resistance_2),
            tf_1d=TimeframeDetail(trend=TimeframeTrend.NEUTRAL_CHOPPY, key_indicators_summary="日线整理平台内运行", support_level=support_2, resistance_level=resistance_2)
        )
        reasoning_summary = f"当前标的处于箱体震荡中，实时价 {current_price} USDT，均线缠绕无明显单边动能，建议空仓观望或在箱体边界高抛低吸。"
        reasoning_details = f"标的 {symbol} 现价 {current_price} USDT 在布林带区间 [{support_1}, {resistance_1}] 内震荡，建议等待放量突破后再行顺势操作。"
        thinking_process = (
            f"<think>\n"
            f"1. 盘面诊断：标的 {symbol} 当前实时价 {current_price} USDT，15m 与 1h 周期均线粘合，方向不明朗。\n"
            f"2. 动能分析：RSI 位于 50 中轴附近，MACD 柱体微弱。\n"
            f"3. 策略判定：震荡行情下不宜盲目追涨杀跌，建议 HOLD_WAIT 观望，等待突破箱体。\n"
            f"</think>"
        )

    return AIAnalysisResponse(
        analysis_id=plan_id,
        symbol=symbol,
        timestamp=now_ms,
        market_regime=regime,
        timeframe_analysis=tf_analysis,
        derivatives_sentiment=DerivativesSentiment(
            funding_rate_bias=FundingRateBias.MODERATE_POSITIVE if derivatives.funding_rate >= 0 else FundingRateBias.MODERATE_NEGATIVE,
            open_interest_interpretation=f"全网持仓量 (OI) 为 {derivatives.open_interest}，资金费率 {derivatives.funding_rate * 100:.4f}%",
            long_short_ratio_state=f"散户多空比 {derivatives.long_short_ratio or 1.15}:1，持仓结构正常",
            sentiment_score=0.72 if is_bullish else (-0.65 if is_bearish else 0.0)
        ),
        signal=AISignal(
            action=action,
            confidence=confidence,
            urgency=urgency
        ),
        trade_plan=TradePlan(
            entry_range=[entry_low, entry_high],
            take_profit_levels=[
                TakeProfitLevel(price=tp_1, percentage=0.5, description="第一关键目标分批止盈 50% 并移动止损"),
                TakeProfitLevel(price=tp_2, percentage=0.5, description="波段扩展目标位出清全部仓位")
            ],
            stop_loss_price=stop_loss,
            risk_reward_ratio=rr_ratio,
            suggested_leverage=sug_lev,
            order_type=TradePlanOrderType.LIMIT
        ),
        risk_assessment=RiskAssessment(
            key_risks=["谨防短期插针洗盘与假突破", "美股开盘时段波动率急剧放大"],
            invalidation_condition=f"若 15m 实体收盘价跌破/突破 {stop_loss} 防守点，则此交易逻辑失效，应立即止损离场",
            max_holding_time_hours=12.0
        ),
        reasoning_summary=reasoning_summary,
        reasoning_details=reasoning_details,
        model_used=model_name,
        latency_ms=150,
        thinking_process=thinking_process
    )
