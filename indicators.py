"""
OKX-Dog 个人量化智能交易系统 - 量化技术指标高精度计算引擎
模块: okx-dog-backend/indicators.py
实现多周期 K 线的 EMA20/50/200、MACD(12,26,9)、RSI(14)、布林带(20,2)、ATR(14) 纯 Python 高性能无损计算
"""

import math
from typing import Dict, List, Optional
from models import KlineCandle, SinglePeriodIndicators, MultiPeriodIndicators


def calculate_ema(prices: List[float], period: int) -> List[float]:
    """计算指数移动平均线 (EMA)"""
    if not prices:
        return []
    if len(prices) < period:
        # 数据不足时返回 SMA 填充
        sma = sum(prices) / len(prices)
        return [sma] * len(prices)

    alpha = 2.0 / (period + 1.0)
    ema_list = [prices[0]]
    for i in range(1, len(prices)):
        val = alpha * prices[i] + (1.0 - alpha) * ema_list[-1]
        ema_list.append(val)
    return ema_list


def calculate_sma(prices: List[float], period: int) -> List[float]:
    """计算简单移动平均线 (SMA)"""
    if len(prices) < period:
        return [sum(prices) / len(prices)] * len(prices) if prices else []
    sma_list = []
    for i in range(len(prices)):
        if i < period - 1:
            sma_list.append(sum(prices[: i + 1]) / (i + 1))
        else:
            sma_list.append(sum(prices[i - period + 1 : i + 1]) / period)
    return sma_list


def calculate_rsi(closes: List[float], period: int = 14) -> float:
    """计算相对强弱指标 RSI (14)"""
    if len(closes) < 2:
        return 50.0

    gains = []
    losses = []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        if change >= 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))

    if len(gains) < period:
        avg_gain = sum(gains) / len(gains) if gains else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
    else:
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(rsi, 2)


def calculate_macd(
    closes: List[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9
) -> Dict[str, float]:
    """计算平滑异同移动平均线 MACD (12, 26, 9)"""
    if len(closes) < slow:
        return {"dif": 0.0, "dea": 0.0, "macd_hist": 0.0}

    ema_fast = calculate_ema(closes, fast)
    ema_slow = calculate_ema(closes, slow)

    dif_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    dea_line = calculate_ema(dif_line, signal)

    current_dif = dif_line[-1]
    current_dea = dea_line[-1]
    current_hist = (current_dif - current_dea) * 2.0  # 国内与主流交易终端标准 (DIF - DEA) * 2

    return {
        "dif": round(current_dif, 2),
        "dea": round(current_dea, 2),
        "macd_hist": round(current_hist, 2)
    }


def calculate_bollinger_bands(
    closes: List[float],
    period: int = 20,
    std_dev_mult: float = 2.0
) -> Dict[str, float]:
    """计算布林带 Bollinger Bands (20, 2)"""
    if not closes:
        return {"upper": 0.0, "middle": 0.0, "lower": 0.0}

    data_len = len(closes)
    window_data = closes[-period:] if data_len >= period else closes
    mid = sum(window_data) / len(window_data)

    variance = sum((x - mid) ** 2 for x in window_data) / len(window_data)
    std_dev = math.sqrt(variance)

    upper = mid + std_dev_mult * std_dev
    lower = mid - std_dev_mult * std_dev

    return {
        "upper": round(upper, 2),
        "middle": round(mid, 2),
        "lower": round(lower, 2)
    }


def calculate_atr(candles: List[KlineCandle], period: int = 14) -> float:
    """计算真实波动区间 ATR (14)"""
    if not candles:
        return 0.0
    if len(candles) == 1:
        return round(candles[0].high - candles[0].low, 2)

    tr_list = []
    for i in range(len(candles)):
        h = candles[i].high
        l = candles[i].low
        if i == 0:
            tr_list.append(h - l)
        else:
            prev_close = candles[i - 1].close
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
            tr_list.append(tr)

    atr_ema = calculate_ema(tr_list, period)
    return round(atr_ema[-1], 2)


def compute_single_timeframe_indicators(
    candles: List[KlineCandle],
    timeframe: str
) -> SinglePeriodIndicators:
    """
    计算单一周期的全部关键技术指标
    """
    if not candles:
        return SinglePeriodIndicators(
            timeframe=timeframe,
            ema_20=0.0,
            ema_50=0.0,
            ema_200=0.0,
            macd_dif=0.0,
            macd_dea=0.0,
            macd_hist=0.0,
            rsi_14=50.0,
            bb_upper=0.0,
            bb_middle=0.0,
            bb_lower=0.0,
            atr_14=0.0
        )

    closes = [c.close for c in candles]

    ema_20_list = calculate_ema(closes, 20)
    ema_50_list = calculate_ema(closes, 50)
    ema_200_list = calculate_ema(closes, 200)

    macd_res = calculate_macd(closes, 12, 26, 9)
    rsi_val = calculate_rsi(closes, 14)
    bb_res = calculate_bollinger_bands(closes, 20, 2.0)
    atr_val = calculate_atr(candles, 14)

    return SinglePeriodIndicators(
        timeframe=timeframe,
        ema_20=round(ema_20_list[-1], 2),
        ema_50=round(ema_50_list[-1], 2),
        ema_200=round(ema_200_list[-1], 2) if len(closes) >= 50 else round(ema_50_list[-1], 2),
        macd_dif=macd_res["dif"],
        macd_dea=macd_res["dea"],
        macd_hist=macd_res["macd_hist"],
        rsi_14=rsi_val,
        bb_upper=bb_res["upper"],
        bb_middle=bb_res["middle"],
        bb_lower=bb_res["lower"],
        atr_14=atr_val
    )
