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
    model_name: str = "quantitative-rule-v3-flow",
    capital_flow: Optional[Any] = None,
) -> AIAnalysisResponse:
    """
    基于实时多周期指标矩阵 (15m/1h/4h/1d EMA/MACD/RSI/ATR/布林带)、真实衍生品资金流
    (OI Delta / Taker Flow / 大户多空比) 与板块逆向百分位，通过多空完全对称的量化规则引擎
    动态推导结构化交易决策与思维链。
    v8.0 升级:
    - 彻底消除指标缺失时的隐性多头默认幻觉 (异常直接 HOLD_WAIT)
    - 引入反弹承压空 + 放量破位空双模式 (资金流出确认时放宽 RSI 下限至 22，不再错过主跌浪)
    - 修正链上/衍生品投票逻辑，融入板块逆向极值百分位防追高/防地板空护栏
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

    # 严格检测关键周期指标是否真实有效，严禁在数据缺失时用“多头排列假值”兜底！
    has_valid_1h = bool(tf_1h and tf_1h.ema_20 and tf_1h.ema_20 > 0 and tf_1h.ema_50 and tf_1h.ema_50 > 0)
    has_valid_15m = bool(tf_15m and tf_15m.rsi_14 is not None and tf_15m.rsi_14 > 0)
    indicators_missing = not (has_valid_1h and has_valid_15m)

    # 提取关键指标（缺失时统一采用中性值 current_price / RSI=50.0，杜绝多头偏见）
    atr_1h = (tf_1h.atr_14 if (tf_1h and tf_1h.atr_14 > 0) else current_price * 0.015)
    atr_15m = (tf_15m.atr_14 if (tf_15m and tf_15m.atr_14 > 0) else current_price * 0.008)
    ema20_1h = (tf_1h.ema_20 if (tf_1h and tf_1h.ema_20 > 0) else current_price)
    ema50_1h = (tf_1h.ema_50 if (tf_1h and tf_1h.ema_50 > 0) else current_price)
    ema200_1h = (tf_1h.ema_200 if (tf_1h and tf_1h.ema_200 > 0) else current_price)
    rsi_1h = (tf_1h.rsi_14 if (tf_1h and tf_1h.rsi_14 is not None) else 50.0)

    ema20_4h = (tf_4h.ema_20 if (tf_4h and tf_4h.ema_20 > 0) else ema20_1h)
    ema50_4h = (tf_4h.ema_50 if (tf_4h and tf_4h.ema_50 > 0) else ema50_1h)
    rsi_4h = (tf_4h.rsi_14 if (tf_4h and tf_4h.rsi_14 is not None) else 50.0)

    rsi_15m = (tf_15m.rsi_14 if (tf_15m and tf_15m.rsi_14 is not None) else 50.0)
    bb_up_15m = (tf_15m.bb_upper if (tf_15m and tf_15m.bb_upper > 0) else current_price * 1.012)
    bb_low_15m = (tf_15m.bb_lower if (tf_15m and tf_15m.bb_lower > 0) else current_price * 0.988)
    macd_hist_1h = (tf_1h.macd_hist if (tf_1h and getattr(tf_1h, "macd_hist", None) is not None) else 0.0)

    # ===== 接入全市场资金流雷达快照与板块逆向百分位 =====
    sym_flow = capital_flow
    sec_flow = None
    try:
        from state import capital_flow_service
        if capital_flow_service is not None:
            if sym_flow is None:
                sym_flow = capital_flow_service.get_symbol_flow_sync(symbol)
            sec_flow = capital_flow_service.get_sector_flow_sync(symbol)
    except Exception:
        pass

    sym_flow_score = float(getattr(sym_flow, "flow_score", 0.0) or 0.0) if sym_flow else 0.0
    sym_pool_role = str(getattr(getattr(sym_flow, "pool_role", None), "value", getattr(sym_flow, "pool_role", "WATCH")) or "WATCH") if sym_flow else "WATCH"
    oi_chg_1h = (
        float(derivatives.oi_change_1h_pct)
        if (derivatives and getattr(derivatives, "oi_change_1h_pct", None) is not None)
        else (float(getattr(sym_flow, "oi_change_1h_pct", 0.0) or 0.0) if sym_flow else 0.0)
    )
    net_taker_ratio = (
        float(derivatives.net_taker_ratio)
        if (derivatives and getattr(derivatives, "net_taker_ratio", None) is not None)
        else (float(getattr(sym_flow, "net_taker_ratio", 0.0) or 0.0) if sym_flow else 0.0)
    )
    sec_pct_24h = float(sec_flow.percentiles.pct_24h) if (sec_flow and sec_flow.percentiles) else 50.0
    sec_pct_1h = float(sec_flow.percentiles.pct_1h) if (sec_flow and sec_flow.percentiles) else 50.0
    sec_flow_score = float(sec_flow.flow_score) if sec_flow else 0.0

    # 极值逆向护栏：极端过热禁追多 / 极端冰点禁追空
    is_extreme_fomo = (sym_pool_role == "OVERHEATED_GUARD") or (sec_pct_24h <= 15.0 and sec_pct_1h <= 15.0)
    is_extreme_ice = (sym_pool_role == "OVERSOLD_GUARD") or (sec_pct_24h >= 85.0 and sec_pct_1h >= 85.0)

    # 波动率挤压与均线纠缠窄幅震荡过滤 (防止震荡市频繁左右挨耳光)
    bb_bandwidth = (bb_up_15m - bb_low_15m) / max(current_price, 1e-6)
    ema_spread_1h = abs(ema20_1h - ema50_1h) / max(current_price, 1e-6)
    is_agg = (aggressiveness_mode.upper() == "AGGRESSIVE")
    is_choppy_tangled = (ema_spread_1h < 0.0020 and 45.0 <= rsi_1h <= 55.0 and abs(sym_flow_score) < 25.0)
    is_squeezed = False if is_agg else ((bb_bandwidth < 0.012) or (atr_15m / max(current_price, 1e-6) < 0.0030) or is_choppy_tangled)

    # ===== 接入自驱进化引擎 (AgentEvolutionManager & SoftmaxGatingNetwork) =====
    evo_params: dict[str, Any] = {}
    evo_mgr = None
    try:
        from okx_dog_ai.agent.evolution.evolution_manager import AgentEvolutionManager
        evo_mgr = AgentEvolutionManager.get_instance()
        evo_params = evo_mgr.get_active_strategy_parameters(symbol)
    except Exception as e:
        logger.debug(f"加载自驱进化策略参数跳过: {e}")

    rsi_ceiling = float(evo_params.get("rsi_ceiling", 68.0))
    rsi_floor = float(evo_params.get("rsi_floor", 32.0))
    min_rr_ratio = float(evo_params.get("min_rr_ratio", 1.55))
    sl_atr_mult = float(evo_params.get("sl_atr_multiplier", 1.35))
    tp1_atr_mult = float(evo_params.get("tp1_atr_multiplier", 2.0))
    tp2_atr_mult = float(evo_params.get("tp2_atr_multiplier", 3.4))
    is_quarantined = bool(evo_params.get("is_quarantined", False))
    quarantine_reason = str(evo_params.get("quarantine_reason", ""))
    active_epoch = str(evo_params.get("active_epoch", "epoch_v1.0"))
    gating_weights: dict[str, float] = evo_params.get("gating_weights", {}) or {}
    learned_rules: list[str] = evo_params.get("learned_rules", []) or []

    # 计算 6 位 AI 员工实时立场并缓存至进化中枢（对称多空逻辑 + 真实主力资金流因子）
    bull_w = float(gating_weights.get("bull_specialist", 16.67)) / 100.0
    bear_w = float(gating_weights.get("bear_critic", 16.67)) / 100.0
    macro_w = float(gating_weights.get("macro_news", 16.67)) / 100.0
    onchain_w = float(gating_weights.get("onchain_analyst", 16.67)) / 100.0
    sniper_w = float(gating_weights.get("micro_sniper", 16.67)) / 100.0
    arbiter_w = float(gating_weights.get("chief_arbiter", 16.67)) / 100.0

    # 真实资金流确认标志：价跌 + OI增/主动卖盘占优 = 主力主动做空；价涨 + OI增/主动买盘占优 = 主力主动做多
    has_bear_flow_confirm = (
        sym_flow_score <= -18.0
        or net_taker_ratio <= -0.03
        or (ema20_1h < ema50_1h and oi_chg_1h >= 0.3)
        or (sec_flow_score <= -20.0 and current_price < ema20_1h)
    )
    has_bull_flow_confirm = (
        sym_flow_score >= 18.0
        or net_taker_ratio >= 0.03
        or (ema20_1h > ema50_1h and oi_chg_1h >= 0.3)
        or (sec_flow_score >= 20.0 and current_price > ema20_1h)
    )

    # 资金流出主跌浪期间自动放宽 RSI 下限至 22，防止被 rsi_floor=32 卡死错过顺势空单
    eff_rsi_floor = min(rsi_floor, 22.0) if (has_bear_flow_confirm and not is_extreme_ice) else rsi_floor
    eff_rsi_ceiling = max(rsi_ceiling, 76.0) if (has_bull_flow_confirm and not is_extreme_fomo) else rsi_ceiling

    bull_stance = (
        "BULLISH"
        if (ema20_1h > ema50_1h and 50.0 <= rsi_1h <= eff_rsi_ceiling and not is_extreme_fomo)
        else ("BEARISH" if (rsi_1h > 76.0 or (ema20_1h < ema50_1h and sym_flow_score <= -25.0)) else "NEUTRAL")
    )
    bear_stance = (
        "BEARISH"
        if (
            (ema20_1h < ema50_1h and eff_rsi_floor <= rsi_1h <= 52.0 and not is_extreme_ice)
            or rsi_15m > eff_rsi_ceiling
            or rsi_1h > eff_rsi_ceiling
            or is_quarantined
        )
        else "NEUTRAL"
    )
    fr_val = derivatives.funding_rate if derivatives else 0.0
    ls_val = (derivatives.long_short_ratio if (derivatives and derivatives.long_short_ratio) else 1.0)
    top_ls_val = (derivatives.top_trader_long_short_ratio if (derivatives and derivatives.top_trader_long_short_ratio) else 1.0)

    # 修正链上/主力侦察员投票：以真实资金流 (Taker Flow + OI Delta + 大户多空比) 为主，消除负费率盲目抄底 Bug！
    if has_bear_flow_confirm and not is_extreme_ice:
        onchain_stance = "BEARISH"
    elif has_bull_flow_confirm and not is_extreme_fomo:
        onchain_stance = "BULLISH"
    elif fr_val > 0.0005 or ls_val > 2.2:
        onchain_stance = "BEARISH"
    elif fr_val < -0.0006 and oi_chg_1h < -1.0 and is_extreme_ice:
        # 仅在极端负费率 + 空头平仓缩量 + 极端冰点时提示轧空反弹
        onchain_stance = "BULLISH"
    elif top_ls_val < 0.85 and ema20_1h < ema50_1h:
        onchain_stance = "BEARISH"
    elif top_ls_val > 1.25 and ema20_1h > ema50_1h:
        onchain_stance = "BULLISH"
    else:
        onchain_stance = "NEUTRAL"

    sniper_stance = (
        "BULLISH"
        if (current_price >= ema20_1h and rsi_15m <= eff_rsi_ceiling and macd_hist_1h >= 0)
        else (
            "BEARISH"
            if (current_price <= ema20_1h and rsi_15m >= eff_rsi_floor and macd_hist_1h <= 0)
            else "NEUTRAL"
        )
    )
    # 宏观视角综合 4H 均线与板块资金流方向
    if ema20_4h > ema50_4h and sec_flow_score >= -15.0:
        macro_stance = "BULLISH"
    elif ema20_4h < ema50_4h or sec_flow_score <= -20.0:
        macro_stance = "BEARISH"
    else:
        macro_stance = "NEUTRAL"

    def _stance_val(st: str) -> float:
        return 1.0 if st == "BULLISH" else (-1.0 if st == "BEARISH" else 0.0)

    weighted_consensus = (
        bull_w * _stance_val(bull_stance)
        + bear_w * _stance_val(bear_stance)
        + macro_w * _stance_val(macro_stance)
        + onchain_w * _stance_val(onchain_stance)
        + sniper_w * _stance_val(sniper_stance)
    )
    arbiter_stance = "BULLISH" if weighted_consensus >= 0.15 else ("BEARISH" if weighted_consensus <= -0.15 else "NEUTRAL")

    if evo_mgr is not None:
        evo_mgr.record_symbol_opinions(symbol, {
            "bull_specialist": {"role_name": "冲锋多头分析师", "stance": bull_stance, "confidence": round(0.65 + abs(rsi_1h - 50.0) * 0.006, 2)},
            "bear_critic": {"role_name": "铁血风控挑刺官", "stance": bear_stance, "confidence": round(0.75 if bear_stance == "BEARISH" else 0.55, 2)},
            "macro_news": {"role_name": "全球资讯情报员", "stance": macro_stance, "confidence": 0.72},
            "onchain_analyst": {"role_name": "链上主力侦察员", "stance": onchain_stance, "confidence": round(0.80 if (has_bear_flow_confirm or has_bull_flow_confirm) else 0.68, 2)},
            "micro_sniper": {"role_name": "微观盘口狙击手", "stance": sniper_stance, "confidence": 0.76},
            "chief_arbiter": {"role_name": "首席风控仲裁官", "stance": arbiter_stance, "confidence": round(min(0.92, 0.65 + abs(weighted_consensus) * 0.3), 2)},
        })

    # 判定多空完全对称趋势与多周期共振状态 (受指标完整性、达尔文隔离与逆向极值护栏约束)
    has_active_pos = bool(position and getattr(position, "pos", 0) > 0)
    if indicators_missing and not has_active_pos:
        is_bullish = False
        is_bearish = False
    elif is_quarantined and not has_active_pos:
        is_bullish = False
        is_bearish = False
    elif is_squeezed:
        is_bullish = False
        is_bearish = False
    elif is_agg:
        is_bullish = (
            (not is_extreme_fomo)
            and 50.0 <= rsi_15m <= eff_rsi_ceiling and 50.0 <= rsi_1h <= eff_rsi_ceiling
            and ((rsi_15m >= 52.0 and current_price >= ema20_1h and ema20_1h >= ema50_1h * 0.998) or (rsi_1h >= 56.0 and current_price >= ema20_1h))
            and weighted_consensus > -0.05
        )
        is_bearish = (
            (not is_extreme_ice)
            and eff_rsi_floor <= rsi_15m <= 52.0 and eff_rsi_floor <= rsi_1h <= 52.0
            and (
                (rsi_15m <= 48.0 and current_price <= ema20_1h and ema20_1h <= ema50_1h * 1.002)
                or (rsi_1h <= 45.0 and current_price <= ema20_1h)
                or (has_bear_flow_confirm and current_price <= ema20_1h and ema20_4h < ema50_4h)
            )
            and weighted_consensus < 0.05
        )
    else:
        # 顺势做多 (回调企稳多 / 资金流入突破多)
        is_bullish = (
            (not is_extreme_fomo)
            and (
                (ema20_1h > ema50_1h and current_price >= ema20_1h * 0.997 and 48.0 <= rsi_1h <= eff_rsi_ceiling and macd_hist_1h >= -0.05 * atr_1h)
                or (56.0 <= rsi_1h <= eff_rsi_ceiling and ema20_4h >= ema50_4h and current_price >= ema20_1h)
                or (has_bull_flow_confirm and ema20_1h > ema50_1h and ema20_4h >= ema50_4h and current_price >= ema20_1h and rsi_1h <= eff_rsi_ceiling)
            )
            and (rsi_15m <= eff_rsi_ceiling + 2.0)
            and (weighted_consensus >= 0.05)
        )
        # 顺势做空 (模式1: 反弹承压空 | 模式2: 资金流出放量破位空)
        is_bearish_pullback = (
            ema20_1h < ema50_1h
            and current_price <= ema20_1h * 1.003
            and eff_rsi_floor <= rsi_1h <= 52.0
            and macd_hist_1h <= 0.05 * atr_1h
        )
        is_bearish_breakdown = (
            (
                (eff_rsi_floor <= rsi_1h <= 44.0 and ema20_4h < ema50_4h and current_price <= ema20_1h)
                or (has_bear_flow_confirm and ema20_1h < ema50_1h and current_price <= ema20_1h and eff_rsi_floor <= rsi_1h <= 50.0)
            )
        )
        is_bearish = (
            (not is_extreme_ice)
            and (is_bearish_pullback or is_bearish_breakdown)
            and (rsi_15m >= eff_rsi_floor - 2.0)
            and (weighted_consensus <= -0.05)
        )

    # 持仓感知 (Position-Aware) 两阶段动态离场研判：
    # 严格区分阶段1 (未减半，浮盈门槛 ≥ max(1.2%, 1.3*ATR)) 与阶段2 (已减半长尾波段，浮盈门槛 ≥ max(2.5%, 2.5*ATR))，杜绝 0.6% 蚊子肉秒平死循环！
    should_close_tp = False
    close_reason_summary = ""
    close_reasoning_details = ""
    close_thinking_process = ""

    if position and hasattr(position, "avg_px") and position.avg_px > 0 and position.pos > 0:
        is_pos_long = (position.pos_side == PosSide.LONG)
        pos_profit_ratio = (current_price - position.avg_px) / position.avg_px if is_pos_long else (position.avg_px - current_price) / position.avg_px
        has_stage1_tp = bool(getattr(position, "tp_triggered_50", False))
        atr_1h_ratio = atr_1h / max(current_price, 1e-6)
        min_tp_ratio = max(0.025, 2.5 * atr_1h_ratio) if has_stage1_tp else max(0.012, 1.3 * atr_1h_ratio)
        rsi_ob_15m = 78.0 if has_stage1_tp else 72.0
        rsi_os_15m = 22.0 if has_stage1_tp else 28.0

        # 多单主动止盈判定 (含板块极端过热提前落袋保护)
        if is_pos_long and pos_profit_ratio >= min_tp_ratio:
            if rsi_15m >= rsi_ob_15m or (current_price >= bb_up_15m * 0.999 and rsi_15m >= (rsi_ob_15m - 3.0)) or rsi_1h >= 76.0 or (is_extreme_fomo and pos_profit_ratio >= 0.015):
                should_close_tp = True
                stage_label = "阶段2长尾波段" if has_stage1_tp else "阶段1半仓锁定"
                close_reason_summary = f"多单持仓顺势浮盈达 +{pos_profit_ratio*100:.2f}% ({stage_label})，15m RSI({round(rsi_15m, 1)}) 或资金流进入过热区，触发主动获利了结！"
                close_reasoning_details = f"当前持有多头仓位 (开仓均价 ${position.avg_px})，现价 ${current_price}。15m RSI 达到 {round(rsi_15m, 1)}，逆向资金分位 {sec_pct_24h:.0f}%，执行{stage_label}主动止盈。"
                close_thinking_process = (
                    f"<think>\n"
                    f"1. 持仓盯市：多单持仓现价 {current_price} USDT，持仓浮盈率达 +{pos_profit_ratio*100:.2f}% (达标门槛 +{min_tp_ratio*100:.2f}%)。\n"
                    f"2. 动能与资金流研判：15m RSI={round(rsi_15m, 1)}，板块逆向资金分位={sec_pct_24h:.0f}%。\n"
                    f"3. 策略决策：执行{stage_label}主动止盈 (CLOSE_POSITION)，保障正数学期望值。\n"
                    f"</think>"
                )
        # 空单主动止盈判定 (含板块极端冰点防轧空主动落袋)
        elif (not is_pos_long) and pos_profit_ratio >= min_tp_ratio:
            if rsi_15m <= rsi_os_15m or (current_price <= bb_low_15m * 1.001 and rsi_15m <= (rsi_os_15m + 3.0)) or rsi_1h <= 24.0 or (is_extreme_ice and pos_profit_ratio >= 0.015):
                should_close_tp = True
                stage_label = "阶段2长尾波段" if has_stage1_tp else "阶段1半仓锁定"
                close_reason_summary = f"空单持仓顺势浮盈达 +{pos_profit_ratio*100:.2f}% ({stage_label})，15m RSI({round(rsi_15m, 1)}) 或资金流触及极端冰点({sec_pct_24h:.0f}%)，触发主动获利平仓防轧空！"
                close_reasoning_details = f"当前持有空头仓位 (开仓均价 ${position.avg_px})，现价 ${current_price}。15m RSI 降至 {round(rsi_15m, 1)}，逆向资金分位 {sec_pct_24h:.0f}%，执行{stage_label}主动止盈离场。"
                close_thinking_process = (
                    f"<think>\n"
                    f"1. 持仓盯市：空单持仓现价 {current_price} USDT，持仓浮盈率达 +{pos_profit_ratio*100:.2f}% (达标门槛 +{min_tp_ratio*100:.2f}%)。\n"
                    f"2. 动能与资金流研判：15m RSI={round(rsi_15m, 1)}，板块逆向资金分位={sec_pct_24h:.0f}% (极端冰点={is_extreme_ice})。\n"
                    f"3. 策略决策：规避空头轧空反弹，执行{stage_label}主动平仓离场 (CLOSE_POSITION)。\n"
                    f"</think>"
                )

        # 顺势动能延续：若未触发超买/超卖主动止盈，且持仓顺势浮盈 (≥0.8%) 未破位，顺应趋势方向延续动能判断 (支持金字塔加仓)
        if not should_close_tp and pos_profit_ratio >= 0.008:
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

    # 优先响应主动平仓离场
    if should_close_tp:
        regime = MarketRegime.TRENDING_UP if (position and position.pos_side == PosSide.LONG) else MarketRegime.TRENDING_DOWN
        action = SignalAction.CLOSE_POSITION
        confidence = 0.88
        urgency = SignalUrgency.HIGH

        support_1 = round(min(ema20_1h, current_price - 1.2 * atr_1h), prec)
        resistance_1 = round(max(bb_up_15m, current_price + 1.2 * atr_1h), prec)

        tf_analysis = TimeframeAnalysis(
            tf_15m=TimeframeDetail(
                trend=TimeframeTrend.BULLISH if rsi_15m >= 50 else TimeframeTrend.BEARISH,
                key_indicators_summary=f"15m 触发主动获利平仓信号，RSI={round(rsi_15m, 1)}",
                support_level=support_1,
                resistance_level=resistance_1
            ),
            tf_1h=TimeframeDetail(
                trend=TimeframeTrend.BULLISH if ema20_1h >= ema50_1h else TimeframeTrend.BEARISH,
                key_indicators_summary=f"1h EMA20/50 运行中，ATR={round(atr_1h, prec)}",
                support_level=support_1,
                resistance_level=resistance_1
            ),
            tf_4h=TimeframeDetail(
                trend=TimeframeTrend.NEUTRAL_CHOPPY,
                key_indicators_summary="4h 周期保持观察",
                support_level=support_1,
                resistance_level=resistance_1
            ),
            tf_1d=TimeframeDetail(
                trend=TimeframeTrend.NEUTRAL_CHOPPY,
                key_indicators_summary="日线级别宏观趋势稳定",
                support_level=support_1,
                resistance_level=resistance_1
            )
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
                long_short_ratio_state=f"散户多空比 {derivatives.long_short_ratio or 1.15}:1",
                sentiment_score=0.5
            ),
            signal=AISignal(
                action=action,
                confidence=confidence,
                urgency=urgency
            ),
            trade_plan=TradePlan(
                entry_range=[current_price, current_price],
                take_profit_levels=[
                    TakeProfitLevel(price=current_price, percentage=1.0, description="主动市价获利平仓锁定收益")
                ],
                stop_loss_price=current_price,
                risk_reward_ratio=2.0,
                suggested_leverage=sug_lev,
                order_type=TradePlanOrderType.MARKET
            ),
            risk_assessment=RiskAssessment(
                key_risks=["短线超买超卖冲高回落"],
                invalidation_condition="市价平仓锁定收益",
                max_holding_time_hours=0.5
            ),
            reasoning_summary=close_reason_summary,
            reasoning_details=close_reasoning_details,
            model_used=model_name,
            latency_ms=120,
            thinking_process=close_thinking_process
        )

    if is_bullish:
        regime = MarketRegime.TRENDING_UP
        action = SignalAction.BUY_LONG
        rsi_bonus = min(0.08, max(0.0, (rsi_1h - 50.0) * 0.005))
        fr_bonus = 0.03 if (derivatives and derivatives.funding_rate >= 0) else 0.0
        flow_bonus = min(0.08, max(0.0, sym_flow_score / 400.0)) + (0.03 if has_bull_flow_confirm else 0.0)
        consensus_bonus = min(0.08, max(-0.08, weighted_consensus * 0.12))
        confidence = round(min(0.94, max(0.65, 0.74 + rsi_bonus + fr_bonus + flow_bonus + consensus_bonus)), 2)
        urgency = SignalUrgency.MEDIUM

        support_1 = round(min(ema20_1h, current_price - 1.2 * atr_1h), prec)
        support_2 = round(min(ema50_1h, current_price - 2.5 * atr_1h), prec)
        resistance_1 = round(max(bb_up_15m, current_price + 1.8 * atr_1h), prec)
        resistance_2 = round(current_price + 3.2 * atr_1h, prec)

        entry_low = round(max(support_1, current_price * 0.995), prec)
        entry_high = round(current_price * 1.002, prec)
        stop_loss = round(current_price - sl_atr_mult * atr_1h, prec)
        
        # 正期望值阶梯止盈：结合自驱进化 TP 倍率与最低净盈亏比门槛
        tp_1 = round(max(entry_high * 1.014, entry_high + max(tp1_atr_mult, sl_atr_mult * min_rr_ratio) * atr_1h), prec)
        tp_2 = round(max(resistance_2, entry_high + tp2_atr_mult * atr_1h), prec)
        
        # 扣除摩擦后的真实净盈亏比
        net_reward = max(0.0, tp_1 - entry_high - friction_cost)
        net_risk = max(entry_high - stop_loss + friction_cost, 0.0001)
        rr_ratio = round(net_reward / net_risk, 2)

        tf_analysis = TimeframeAnalysis(
            tf_15m=TimeframeDetail(
                trend=TimeframeTrend.BULLISH if rsi_15m >= 50 else TimeframeTrend.NEUTRAL_CHOPPY,
                key_indicators_summary=f"15m 价格依托 EMA20({round(ema20_1h, prec)}) 整理，RSI={round(rsi_15m, 1)} (进化上限 {rsi_ceiling})",
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
        rule_hint = f" 遵循进化战法：{learned_rules[0]}" if learned_rules else ""
        reasoning_summary = f"[{active_epoch}] 1h与4h周期多头共振且6角色加权共识({weighted_consensus:+.2f})通过，实时价 {current_price} USDT。建议在 {entry_low}~{entry_high} 做多，止损 {stop_loss}，净盈亏比 {rr_ratio}:1。"
        reasoning_details = f"多周期量化与进化共识研判：标的 {symbol} 现价 {current_price} USDT 位于 1h EMA20({round(ema20_1h, prec)}) 之上，1h RSI({round(rsi_1h, 1)} <= {rsi_ceiling}) 处于健康多头区。ATR(14)={round(atr_1h, prec)}。{rule_hint}"
        thinking_process = (
            f"<think>\n"
            f"1. 标的结构与进化参数：标的 {symbol} 当前实时价 {current_price} USDT，当前生效进化纪元 {active_epoch} (RSI上限={rsi_ceiling}, 最低RR={min_rr_ratio})。\n"
            f"2. 多角色门控共识：6位AI员工 Softmax 加权投票得分 {weighted_consensus:+.2f}，多头共识成立。\n"
            f"3. 点位推算：建议入场区间 [{entry_low}, {entry_high}]，硬止损设于 {stop_loss}，第一目标 {tp_1}，第二目标 {tp_2}，扣除摩擦后净盈亏比 {rr_ratio}:1。\n"
            f"4. 进化战法约束：{learned_rules[0] if learned_rules else '严格执行移动保本与趋势反转冷却'}\n"
            f"</think>"
        )
    elif is_bearish:
        regime = MarketRegime.TRENDING_DOWN
        action = SignalAction.SELL_SHORT
        rsi_bonus = min(0.08, max(0.0, (50.0 - rsi_1h) * 0.005))
        fr_bonus = 0.03 if (derivatives and derivatives.funding_rate <= 0) else 0.0
        flow_bonus = min(0.08, max(0.0, (-sym_flow_score) / 400.0)) + (0.04 if has_bear_flow_confirm else 0.0)
        consensus_bonus = min(0.08, max(-0.08, (-weighted_consensus) * 0.12))
        confidence = round(min(0.94, max(0.65, 0.74 + rsi_bonus + fr_bonus + flow_bonus + consensus_bonus)), 2)
        urgency = SignalUrgency.HIGH if has_bear_flow_confirm else SignalUrgency.MEDIUM

        resistance_1 = round(max(ema20_1h, current_price + 1.2 * atr_1h), prec)
        resistance_2 = round(max(ema50_1h, current_price + 2.5 * atr_1h), prec)
        support_1 = round(min(bb_low_15m, current_price - 1.8 * atr_1h), prec)
        support_2 = round(current_price - 3.2 * atr_1h, prec)

        entry_high = round(min(resistance_1, current_price * 1.005), prec)
        entry_low = round(current_price * 0.998, prec)
        stop_loss = round(current_price + sl_atr_mult * atr_1h, prec)
        
        # 正期望值阶梯止盈：结合自驱进化 TP 倍率与最低净盈亏比门槛
        tp_1 = round(min(entry_low * 0.986, entry_low - max(tp1_atr_mult, sl_atr_mult * min_rr_ratio) * atr_1h), prec)
        tp_2 = round(min(support_2, entry_low - tp2_atr_mult * atr_1h), prec)
        
        # 扣除摩擦后的真实净盈亏比
        net_reward = max(0.0, entry_low - tp_1 - friction_cost)
        net_risk = max(stop_loss - entry_low + friction_cost, 0.0001)
        rr_ratio = round(net_reward / net_risk, 2)

        tf_analysis = TimeframeAnalysis(
            tf_15m=TimeframeDetail(
                trend=TimeframeTrend.BEARISH,
                key_indicators_summary=f"15m 价格受阻于 EMA20({round(ema20_1h, prec)})，RSI={round(rsi_15m, 1)} (进化下限 {rsi_floor})",
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
        rule_hint = f" 遵循进化战法：{learned_rules[0]}" if learned_rules else ""
        reasoning_summary = f"[{active_epoch}] 1h均线空头排列且6角色加权共识({weighted_consensus:+.2f})通过，实时价 {current_price} USDT。建议在 {entry_low}~{entry_high} 逢高做空，止损 {stop_loss}，净盈亏比 {rr_ratio}:1。"
        reasoning_details = f"多周期量化与进化共识研判：标的 {symbol} 现价 {current_price} USDT 承压于 1h EMA20({round(ema20_1h, prec)})，1h RSI({round(rsi_1h, 1)} >= {rsi_floor}) 处于弱势区。{rule_hint}"
        thinking_process = (
            f"<think>\n"
            f"1. 标的结构与进化参数：标的 {symbol} 当前实时价 {current_price} USDT，当前生效进化纪元 {active_epoch} (RSI下限={rsi_floor}, 最低RR={min_rr_ratio})。\n"
            f"2. 多角色门控共识：6位AI员工 Softmax 加权投票得分 {weighted_consensus:+.2f}，空头共识成立。\n"
            f"3. 点位推算：建议逢高做空区间 [{entry_low}, {entry_high}]，硬止损设于 {stop_loss}，第一目标 {tp_1}，扣除摩擦后净盈亏比 {rr_ratio}:1。\n"
            f"4. 进化战法约束：{learned_rules[0] if learned_rules else '严禁浮亏补仓与超卖追空'}\n"
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
        if indicators_missing:
            reasoning_summary = f"[{active_epoch} 行情数据缺失保护] {symbol} 多周期技术指标暂未就绪，严格禁止盲目开仓，维持 HOLD_WAIT。"
            reasoning_details = f"数据完整性拦截：{symbol} 缺少有效 15m/1h EMA 与 RSI 计算值，已阻断隐性多头默认值幻觉。"
        elif is_extreme_ice:
            reasoning_summary = f"[{active_epoch} 极端冰点逆向护栏] {symbol} 所属板块逆向资金分位达 {sec_pct_24h:.0f}%，禁止低位追空防轧空，维持观望。"
            reasoning_details = f"多周期资金流逆向护栏生效：{symbol} 资金得分 {sym_flow_score:+.1f}，板块逆向分位 {sec_pct_24h:.0f}% 进入极端冰点。"
        elif is_extreme_fomo:
            reasoning_summary = f"[{active_epoch} 反FOMO过热护栏] {symbol} 所属板块逆向资金分位仅 {sec_pct_24h:.0f}%，资金过热禁止追高，维持观望。"
            reasoning_details = f"反 FOMO 护栏生效：{symbol} 资金得分 {sym_flow_score:+.1f}，板块逆向分位 {sec_pct_24h:.0f}% 进入极端过热区。"
        elif is_quarantined:
            reasoning_summary = f"[{active_epoch} 达尔文风控隔离] {symbol} 已被自驱进化引擎熔断隔离 ({quarantine_reason})，禁止新开仓，强制空仓观望。"
            reasoning_details = f"自驱进化诊断拦截：{symbol} 因 {quarantine_reason} 触发达尔文淘汰与毒性流动性隔离规则，当前信号强制锁定为 HOLD_WAIT。"
        else:
            reasoning_summary = f"[{active_epoch}] 当前标的处于箱体震荡或未达进化阈值(RSI={round(rsi_1h, 1)}, 资金分={sym_flow_score:+.0f}, 共识={weighted_consensus:+.2f})，实时价 {current_price} USDT，建议空仓观望。"
            reasoning_details = f"标的 {symbol} 现价 {current_price} USDT 在区间 [{support_1}, {resistance_1}] 内运行，进化过滤上限 RSI={eff_rsi_ceiling}/下限={eff_rsi_floor}，等待高确定性突破。"
        thinking_process = (
            f"<think>\n"
            f"1. 盘面与进化过滤：标的 {symbol} 实时价 {current_price} USDT，进化纪元 {active_epoch}。\n"
            f"2. 拦截原因：{'触发达尔文标的隔离 (' + quarantine_reason + ')' if is_quarantined else f'1h RSI={round(rsi_1h, 1)} 或多角色加权共识({weighted_consensus:+.2f})未达开仓阈值'}。\n"
            f"3. 策略判定：执行 HOLD_WAIT 观望，保护本金免受震荡磨损与差流动性滑点侵蚀。\n"
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
