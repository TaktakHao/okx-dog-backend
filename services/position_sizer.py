"""
OKX-Dog 个人量化智能交易系统 - ATR 波动率自适应头寸管理与固定风险预算模型 (Position Sizer)
模块: okx-dog-backend/services/position_sizer.py
角色: AI 与量化算法工程师 (AI & Quant Engineer) / 交易风控架构师 (Trading Risk Architect)

核心理念:
1. 资金风险恒定 (Fixed Fractional Risk): 单笔交易的最大可承受亏损金额严格锁定为总净值的固定比例 (默认 1%)。
2. 波动率自适应 (ATR Adaptive): 止损距离由标的真实波动幅度 (ATR) 决定。高波动轻仓防插针，低波动重仓平滑收益。
3. 波动率过热惩罚 (Volatility Overheat Guard): 当短期 ATR / Price 超出历史阈值时，自动按惩罚系数缩减敞口。
4. 交易所精度与合约面值对齐: 严格按照 OKX 合约最小粒度 (lot_size, min_size) 规格化向下取整，避免被拒单。
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("okx_dog.position_sizer")


@dataclass
class PositionSizeResult:
    """自适应头寸计算详细结果"""
    symbol: str
    equity: float                     # 当前账户净值 (USDT)
    entry_price: float                # 建议或当前入场价格
    stop_loss_price: float            # 最终自适应止损价格
    stop_distance: float              # 绝对止损距离 (USDT)
    stop_distance_pct: float          # 止损距离百分比 (%)
    risk_budget_usdt: float           # 本笔交易所允许的最大亏损 (USDT)
    calculated_size: float            # 严格对齐精度后的开仓币数/张数
    notional_value_usdt: float        # 开仓名义价值 (USDT)
    required_margin_usdt: float       # 所需估算保证金 (USDT)
    effective_leverage: float         # 实际名义杠杆倍数
    atr: Optional[float]              # 标的 ATR (14) 波动值
    volatility_ratio_pct: float       # ATR / Entry 波动比率 (%)
    volatility_regime: str            # 波动状态 (LOW, NORMAL, HIGH, EXTREME)
    warning: Optional[str] = None     # 风险提示 (如有)


class PositionSizer:
    """ATR 波动率自适应头寸规模管理引擎"""

    @staticmethod
    def calculate_atr_from_candles(candles: List[Dict[str, float]], period: int = 14) -> Optional[float]:
        """
        从 K 线序列计算 Wilder's RMA 平滑 ATR (Average True Range)
        candles 格式需包含: 'high', 'low', 'close'
        """
        if not candles or len(candles) < period + 1:
            return None

        tr_list = []
        for i in range(1, len(candles)):
            c_curr = candles[i]
            c_prev = candles[i - 1]
            high = float(c_curr["high"])
            low = float(c_curr["low"])
            prev_close = float(c_prev["close"])

            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            tr_list.append(tr)

        if len(tr_list) < period:
            return None

        # 初始 ATR 为前 period 个 TR 的简单算术均值
        atr = sum(tr_list[:period]) / period
        # 后续使用 Wilder 平滑: ATR = (prev_ATR * (period - 1) + current_TR) / period
        for tr in tr_list[period:]:
            atr = (atr * (period - 1) + tr) / period

        return round(atr, 4)

    @classmethod
    def calculate_adaptive_size(
        cls,
        symbol: str,
        equity: float,
        entry_price: float,
        is_short: bool = False,
        stop_loss_price: Optional[float] = None,
        atr: Optional[float] = None,
        atr_multiplier: float = 2.0,
        risk_pct: float = 0.01,              # 默认固定 1% 资金风险预算
        max_leverage: int = 5,
        max_order_usdt: float = 3000.0,
        contract_val: float = 1.0,           # 合约面值 (币币=1.0)
        lot_size: float = 0.001,             # 最小下单精度步进
        min_size: float = 0.001,             # 最小可下单量
        available_margin: Optional[float] = None,
    ) -> PositionSizeResult:
        """
        计算符合企业级量化风控标准的自适应头寸大小:
        公式:
          RiskBudget = Equity * RiskPct * VolOverheatPenalty
          StopDist = max(|Entry - SL|, ATR * 0.5, Entry * 0.005)
          RawSize = RiskBudget / (StopDist * ContractVal)
          BoundedSize = min(RawSize, LeverageCap, MaxNotionalCap, AvailMarginCap)
          FinalSize = floor(BoundedSize / lot_size) * lot_size
        """
        if equity <= 0 or entry_price <= 0:
            raise ValueError(f"账户净值与入场价格必须大于 0: equity={equity}, entry_price={entry_price}")

        # 1. 波动状态与 ATR 评估
        vol_ratio_pct = 0.0
        vol_regime = "NORMAL"
        vol_factor = 1.0

        if atr and atr > 0:
            vol_ratio_pct = (atr / entry_price) * 100.0
            if vol_ratio_pct > 4.0:
                vol_regime = "EXTREME"
                vol_factor = 0.60   # 极端高波动插针环境，主动防守收缩 40% 仓位
            elif vol_ratio_pct > 2.5:
                vol_regime = "HIGH"
                vol_factor = 0.80   # 较高波动环境，收缩 20%
            elif vol_ratio_pct < 0.8:
                vol_regime = "LOW"
                vol_factor = 1.05   # 超低波动窄幅震荡，小幅放大利用率
            else:
                vol_regime = "NORMAL"
                vol_factor = 1.0

        # 2. 确定自适应止损价格与止损距离
        final_sl_px = stop_loss_price
        if final_sl_px is None or final_sl_px <= 0:
            # 未提供止损时，优先依据 ATR 自适应生成，否则采用 1.5% 兜底
            atr_dist = (atr * atr_multiplier) if (atr and atr > 0) else (entry_price * 0.015)
            final_sl_px = round(
                (entry_price + atr_dist) if is_short else (entry_price - atr_dist),
                2 if entry_price > 10 else 4
            )

        # 止损距离不能为 0，且设置最小硬底 (防止因过于靠近入场价导致算出天文数字仓位)
        min_dist_guard = max(entry_price * 0.005, (atr * 0.5) if (atr and atr > 0) else 0.0)
        raw_stop_dist = abs(entry_price - final_sl_px)
        safe_stop_dist = max(raw_stop_dist, min_dist_guard)
        stop_dist_pct = (safe_stop_dist / entry_price) * 100.0

        # 3. 计算固定风险预算 (USDT)
        risk_budget_usdt = round(equity * risk_pct * vol_factor, 2)

        # 4. 根据风险预算与止损距离反算理论开仓规模 (Fixed Fractional Risk)
        # 亏损 = Size * StopDistance * ContractVal = RiskBudget
        raw_size = risk_budget_usdt / (safe_stop_dist * contract_val)

        # 5. 上限硬约束约束链 (Boundaries)
        warning = None

        # 约束 A: 杠杆上限 (名义价值 / 净值 <= max_leverage)
        max_size_by_lev = (equity * max_leverage) / (entry_price * contract_val)

        # 约束 B: 订单单笔最大名义金额上限 (max_order_usdt)
        max_size_by_notional = max_order_usdt / (entry_price * contract_val)

        # 约束 C: 可用保证金上限 (若传入可用资金，保留 20% 安全垫)
        max_size_by_avail = None
        if available_margin is not None and available_margin > 0:
            safe_margin = available_margin * 0.80
            max_size_by_avail = (safe_margin * max_leverage) / (entry_price * contract_val)

        # 取各风控上限的交集最小值
        bounded_size = min(raw_size, max_size_by_lev, max_size_by_notional)
        if max_size_by_avail is not None:
            bounded_size = min(bounded_size, max_size_by_avail)

        if bounded_size < raw_size:
            if bounded_size == max_size_by_notional:
                warning = f"头寸受单笔限额 ${max_order_usdt} 限制截断"
            elif bounded_size == max_size_by_lev:
                warning = f"头寸受最大名义杠杆 {max_leverage}x 限制截断"
            elif max_size_by_avail is not None and bounded_size == max_size_by_avail:
                warning = "头寸受可用可用保证金安全垫限制截断"

        # 6. 对齐 OKX 交易粒度规格化向下取整
        precision_step = lot_size if lot_size > 0 else 0.001
        step_factor = 1.0 / precision_step
        quantized_size = math.floor(bounded_size * step_factor) / step_factor
        final_size = max(min_size, round(quantized_size, 4))

        # 7. 计算衍生财务指标
        notional_val = round(final_size * entry_price * contract_val, 2)
        est_margin = round(notional_val / max(1, max_leverage), 2)
        effective_lev = round(notional_val / equity, 2) if equity > 0 else 0.0

        logger.info(
            f"📐 [PositionSizer] {symbol} 计算完成: 净值=${equity:.2f}, 风险预算=${risk_budget_usdt:.2f} ({risk_pct*100:.1f}%), "
            f"波动状态={vol_regime}(ATR={atr or 'N/A'}, {vol_ratio_pct:.2f}%), "
            f"入场=${entry_price}, 止损=${final_sl_px} (距={stop_dist_pct:.2f}%) -> "
            f"开仓={final_size} 币 (名义价值=${notional_val:.2f}, 实际杠杆={effective_lev:.2f}x)"
        )

        return PositionSizeResult(
            symbol=symbol,
            equity=equity,
            entry_price=entry_price,
            stop_loss_price=final_sl_px,
            stop_distance=round(safe_stop_dist, 4),
            stop_distance_pct=round(stop_dist_pct, 2),
            risk_budget_usdt=risk_budget_usdt,
            calculated_size=final_size,
            notional_value_usdt=notional_val,
            required_margin_usdt=est_margin,
            effective_leverage=effective_lev,
            atr=atr,
            volatility_ratio_pct=round(vol_ratio_pct, 2),
            volatility_regime=vol_regime,
            warning=warning,
        )


# 单例实例
position_sizer = PositionSizer()
