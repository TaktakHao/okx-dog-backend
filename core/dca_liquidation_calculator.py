"""
OKX-Dog 量化数理核心 - DCA 与网格动态强平线穿透预判与保证金精算引擎
模块: okx-dog-backend/core/dca_liquidation_calculator.py
融合自: oke_auto_trade (calculate_dca.py & calculate_multi_group_margin.py)

核心职责:
1. 逐层前瞻推导 DCA / 马丁补仓链条：计算每轮加仓触发价、加仓后持仓均价、累计保证金、以及动态爆仓/强平价。
2. 强平穿透硬拦截 (Liquidation Penetration Check):
   - 做多场景: 若当前计划加仓价格 <= 上一轮持仓强平价，表明价格尚未跌到补仓点，前序持仓就已先行爆仓！
   - 做空场景: 若当前计划加仓价格 >= 上一轮持仓强平价，同理阻断。
3. 极限逆势承压保证金精算 (Multi-Group Margin Stress):
   - 测算在承受目标极端单边波动 (如 15%~30% 暴跌/暴涨) 时，网格或 DCA 所需锁定的绝对总保证金底线与安全缓冲。
"""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    from models import DcaLiquidationAnalysis
except ImportError:
    from pydantic import BaseModel, Field

    class DcaLiquidationAnalysis(BaseModel):
        is_safe: bool
        max_safe_dca_orders: int
        breakeven_price: float
        current_liq_price: float
        next_dca_liq_price: float
        next_order_price: float
        liq_safety_buffer_pct: float
        required_extreme_margin: float
        violation_reason: Optional[str] = None


logger = logging.getLogger("DcaLiquidationCalculator")


class DcaLiquidationCalculator:
    """DCA 与网格动态强平线穿透预判计算器"""

    @staticmethod
    def calculate_dca_chain(
        direction: str,
        initial_price: float,
        leverage: float,
        initial_margin: float,
        dca_margin_base: float,
        max_dca_orders: int,
        price_deviation_percent: float,
        price_step_multiplier: float = 1.0,
        amount_multiplier: float = 1.2,
        tp_target_percent: float = 1.5,
        extra_margin: float = 0.0,
        maint_margin_ratio: float = 0.005,  # OKX 永续合约默认维持保证金率 MMR (0.5%)
    ) -> DcaLiquidationAnalysis:
        """
        计算完整的 DCA 补仓链条并检验强平穿透风险。

        :param direction: 持仓方向 ('long' 做多 / 'short' 做空)
        :param initial_price: 初始开仓价格
        :param leverage: 杠杆倍数 (>= 1.0)
        :param initial_margin: 初始订单保证金 (USDT)
        :param dca_margin_base: 第一档 DCA 基础加仓保证金 (USDT)
        :param max_dca_orders: 最大计划加仓轮数 (>= 1)
        :param price_deviation_percent: 初始加仓跌/涨幅步长 (%)
        :param price_step_multiplier: 加仓价差步长递增乘数 (默认 1.0 匀速)
        :param amount_multiplier: 加仓金额乘数 (马丁倍投倍率，如 1.2 或 1.5)
        :param tp_target_percent: 目标止盈幅度 (%)
        :param extra_margin: 额外隔离保证金储备 (USDT)
        :param maint_margin_ratio: 维持保证金率 MMR
        :return: DcaLiquidationAnalysis
        """
        dir_norm = direction.lower().strip()
        if dir_norm not in ["long", "short"]:
            raise ValueError(f"direction 必须为 'long' 或 'short'，当前为: {direction}")
        if initial_price <= 0 or leverage <= 0 or initial_margin <= 0:
            raise ValueError("初始价格、杠杆倍数与初始保证金必须 > 0")

        is_long = dir_norm == "long"
        sign = 1.0 if is_long else -1.0

        # 1. 预计算总计划分配的保证金预算
        total_allocated_margin = initial_margin + extra_margin
        for i in range(1, max_dca_orders + 1):
            total_allocated_margin += dca_margin_base * (amount_multiplier ** (i - 1))

        # 2. 模拟逐轮递推
        total_size = 0.0       # 累计持仓币数/标的规模
        total_notional = 0.0   # 累计持仓成本名义总值 (USDT)
        cumulative_margin_invested = 0.0
        current_step_gap = price_deviation_percent
        cumulative_dev_percent = 0.0

        rounds_info = []
        is_safe = True
        violation_reason = None
        max_safe_dca_orders = 0

        # 第 0 轮：初始建仓
        init_notional = initial_margin * leverage
        init_size = init_notional / initial_price
        total_size += init_size
        total_notional += init_notional
        cumulative_margin_invested += initial_margin

        # 初始强平价计算 (逐仓模型考虑维持保证金率与追加保证金):
        # 多头强平价 = 开仓均价 * (1 - 1/leverage + MMR) - (extra_margin / size)
        # 空头强平价 = 开仓均价 * (1 + 1/leverage - MMR) + (extra_margin / size)
        init_liq = initial_price * (1.0 - sign * (1.0 / leverage - maint_margin_ratio)) - sign * (extra_margin / total_size)
        init_liq = max(0.0, init_liq)

        rounds_info.append({
            "round": 0,
            "order_price": initial_price,
            "order_margin": initial_margin,
            "order_size": init_size,
            "avg_cost": initial_price,
            "liq_price": init_liq,
            "cum_margin": cumulative_margin_invested,
        })

        # 依次推演后续各档 DCA
        for i in range(1, max_dca_orders + 1):
            # 计算本轮计划加仓价格
            cumulative_dev_percent += current_step_gap
            current_step_gap *= price_step_multiplier

            if is_long:
                order_price = initial_price * (1.0 - cumulative_dev_percent / 100.0)
            else:
                order_price = initial_price * (1.0 + cumulative_dev_percent / 100.0)

            order_price = max(0.000001, order_price)
            order_margin = dca_margin_base * (amount_multiplier ** (i - 1))

            # 【核心风控拦截：强平穿透检查】
            # 在触发本轮计划挂单前，价格是否已经先跌破/涨破上一轮持仓的强平价？
            prev_liq = rounds_info[-1]["liq_price"]
            if is_long and order_price <= prev_liq:
                is_safe = False
                violation_reason = (
                    f"DCA #{i} 计划加仓价 {order_price:.4f} <= 上一轮强平价 {prev_liq:.4f}！"
                    f"在加仓委托成交前仓位将被强制平仓 (未决暴毙死穴)"
                )
                logger.warning(f"[DCA_LIQUIDATION_INTERCEPT] {violation_reason}")
                break
            elif (not is_long) and order_price >= prev_liq:
                is_safe = False
                violation_reason = (
                    f"DCA #{i} 计划加仓价 {order_price:.4f} >= 上一轮强平价 {prev_liq:.4f}！"
                    f"在加仓委托成交前仓位将被强制平仓 (未决暴毙死穴)"
                )
                logger.warning(f"[DCA_LIQUIDATION_INTERCEPT] {violation_reason}")
                break

            # 若未被击穿，计算加仓后的新持仓结构
            order_notional = order_margin * leverage
            order_size = order_notional / order_price

            total_size += order_size
            total_notional += order_notional
            cumulative_margin_invested += order_margin

            new_avg_cost = total_notional / total_size
            new_liq = new_avg_cost * (1.0 - sign * (1.0 / leverage - maint_margin_ratio)) - sign * (extra_margin / total_size)
            new_liq = max(0.0, new_liq)

            rounds_info.append({
                "round": i,
                "order_price": order_price,
                "order_margin": order_margin,
                "order_size": order_size,
                "avg_cost": new_avg_cost,
                "liq_price": new_liq,
                "cum_margin": cumulative_margin_invested,
            })
            max_safe_dca_orders = i

        # 计算返回指标
        curr_round = rounds_info[0]
        next_round = rounds_info[1] if len(rounds_info) > 1 else curr_round

        breakeven_px = round(rounds_info[-1]["avg_cost"], 4)
        current_liq_px = round(curr_round["liq_price"], 4)
        next_liq_px = round(next_round["liq_price"], 4)
        next_order_px = round(next_round["order_price"], 4)

        # 强平安全缓冲百分比 = |当前价格 - 当前强平价| / 当前价格 * 100
        buffer_pct = abs(initial_price - current_liq_px) / initial_price * 100.0 if initial_price > 0 else 0.0

        # 测算 30% 极端逆势下所需总隔离保证金
        extreme_required = DcaLiquidationCalculator.calculate_extreme_margin_stress(
            direction=direction,
            initial_price=initial_price,
            leverage=leverage,
            target_loss_percent=30.0,
            initial_size=init_size,
            maint_margin_ratio=maint_margin_ratio,
        )

        return DcaLiquidationAnalysis(
            is_safe=is_safe,
            max_safe_dca_orders=max_safe_dca_orders,
            breakeven_price=breakeven_px,
            current_liq_price=current_liq_px,
            next_dca_liq_price=next_liq_px,
            next_order_price=next_order_px,
            liq_safety_buffer_pct=round(buffer_pct, 2),
            required_extreme_margin=round(extreme_required, 2),
            violation_reason=violation_reason,
        )

    @staticmethod
    def calculate_extreme_margin_stress(
        direction: str,
        initial_price: float,
        leverage: float,
        target_loss_percent: float = 30.0,
        initial_size: float = 0.1,
        maint_margin_ratio: float = 0.005,
    ) -> float:
        """
        极端逆势承压保证金精算 (源自 calculate_multi_group_margin.py 核心精髓)。
        计算持仓要承受 target_loss_percent (如 30%) 的单边极端暴跌/暴涨，
        在不触发强平爆仓的前提下，账户必须实际锁定的最低隔离保证金总额。

        :param direction: 'long' 或 'short'
        :param initial_price: 初始价格
        :param leverage: 杠杆
        :param target_loss_percent: 最大承受逆势百分比 (如 30.0 代表 30%)
        :param initial_size: 开仓数量 (币数)
        :param maint_margin_ratio: 维持保证金率 MMR
        :return: 所需的绝对最低保证金 (USDT)
        """
        notional = initial_price * initial_size
        # 1. 开仓原始保证金
        init_margin = notional / leverage
        # 2. 极端逆势下的浮动亏损金额
        extreme_pnl_loss = notional * (target_loss_percent / 100.0)
        # 3. 维持保证金要求
        mmr_req = notional * maint_margin_ratio
        # 4. 必须锁定的总保证金 = 浮亏损失 + 维持保证金要求
        required_margin = extreme_pnl_loss + mmr_req
        return max(init_margin, required_margin)
