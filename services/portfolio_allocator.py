"""
OKX-Dog 个人量化智能交易系统 - 60/30/10 双引擎组合资金槽位分配器
模块: okx-dog-backend/services/portfolio_allocator.py
角色: 后端架构师 / AI 与量化算法工程师

核心职责:
1. 严格执行 60% 主引擎 B (双周期趋势) + 30% 底座引擎 A (期现费率套利) + 10% USDT 安全垫 的资金槽位硬隔离；
2. 防止趋势引擎超额占用套利保证金或侵蚀 10% 安全缓冲，反之亦然；
3. 联动全局硬风控红线 (单日 3% 熔断 + 总权益 18% 回撤全局停机)，输出组合实时资金全景。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from config import settings

logger = logging.getLogger("okx_dog.portfolio_allocator")


@dataclass
class EngineSlotBudget:
    """单个策略引擎的资金槽位预算"""
    engine_id: str
    display_name: str
    target_weight: float
    allocated_usdt: float
    used_margin_usdt: float
    available_usdt: float
    max_leverage: float
    single_trade_risk_pct: float
    status: str = "ACTIVE"


class PortfolioAllocator:
    """60/30/10 进取趋势型双引擎资金槽位隔离与预算中枢"""

    def __init__(
        self,
        trend_weight: float = 0.60,
        carry_weight: float = 0.30,
        buffer_weight: float = 0.10,
        trend_4h_subweight: float = 0.50,
        trend_1d_subweight: float = 0.50,
    ):
        total_w = trend_weight + carry_weight + buffer_weight
        if abs(total_w - 1.0) > 1e-6:
            raise ValueError(f"组合目标权重之和必须为 1.0，当前为 {total_w}")

        self.trend_weight = trend_weight
        self.carry_weight = carry_weight
        self.buffer_weight = buffer_weight
        self.trend_4h_subweight = trend_4h_subweight
        self.trend_1d_subweight = trend_1d_subweight

        # 实时追踪各引擎已占用保证金 (USDT)
        self.used_margin_by_engine: Dict[str, float] = {
            "ENGINE_B_TREND_4H": 0.0,
            "ENGINE_B_TREND_1D": 0.0,
            "ENGINE_A_CARRY": 0.0,
        }

    def compute_slot_budgets(
        self,
        total_equity_usdt: float,
        initial_equity_usdt: float = 10000.0,
        today_realized_loss_usdt: float = 0.0,
    ) -> Dict[str, Any]:
        """根据当前账户总净值计算各引擎实时可用槽位预算与全局硬停机状态"""
        eq = max(0.0, float(total_equity_usdt))
        init_eq = max(1.0, float(initial_equity_usdt))

        trend_total = round(eq * self.trend_weight, 2)
        trend_4h_alloc = round(trend_total * self.trend_4h_subweight, 2)
        trend_1d_alloc = round(trend_total * self.trend_1d_subweight, 2)
        carry_alloc = round(eq * self.carry_weight, 2)
        buffer_alloc = round(eq * self.buffer_weight, 2)

        equity_floor = round(init_eq * settings.DEFAULT_EQUITY_FLOOR_RATIO, 2)
        daily_loss_limit = round(init_eq * 0.03, 2)
        drawdown_from_init_pct = round(max(0.0, (init_eq - eq) / init_eq * 100.0), 2)

        is_global_stop = (eq > 0 and eq <= equity_floor)
        is_daily_broken = (today_realized_loss_usdt >= daily_loss_limit)

        def _make_slot(
            eid: str,
            name: str,
            weight: float,
            alloc: float,
            max_lev: float,
            risk_pct: float,
        ) -> EngineSlotBudget:
            used = round(float(self.used_margin_by_engine.get(eid, 0.0)), 2)
            avail = round(max(0.0, alloc - used), 2)
            status = "LOCKED_GLOBAL_STOP" if is_global_stop else ("LOCKED_DAILY_BREAK" if is_daily_broken else "ACTIVE")
            return EngineSlotBudget(
                engine_id=eid,
                display_name=name,
                target_weight=round(weight, 4),
                allocated_usdt=alloc,
                used_margin_usdt=used,
                available_usdt=avail,
                max_leverage=max_lev,
                single_trade_risk_pct=risk_pct,
                status=status,
            )

        slots = {
            "ENGINE_B_TREND_4H": _make_slot(
                "ENGINE_B_TREND_4H",
                "主引擎 B-1: 4H 凯尔特纳趋势突破",
                self.trend_weight * self.trend_4h_subweight,
                trend_4h_alloc,
                max_lev=3.0,
                risk_pct=0.01,
            ),
            "ENGINE_B_TREND_1D": _make_slot(
                "ENGINE_B_TREND_1D",
                "主引擎 B-2: 1D 双均线吊灯趋势",
                self.trend_weight * self.trend_1d_subweight,
                trend_1d_alloc,
                max_lev=3.0,
                risk_pct=0.015,
            ),
            "ENGINE_A_CARRY": _make_slot(
                "ENGINE_A_CARRY",
                "底座引擎 A: 期现资金费率套利 (Delta 中性)",
                self.carry_weight,
                carry_alloc,
                max_lev=1.0,
                risk_pct=0.0,
            ),
            "SAFETY_BUFFER": EngineSlotBudget(
                engine_id="SAFETY_BUFFER",
                display_name="USDT 跨币种保证金安全垫",
                target_weight=self.buffer_weight,
                allocated_usdt=buffer_alloc,
                used_margin_usdt=0.0,
                available_usdt=buffer_alloc,
                max_leverage=0.0,
                single_trade_risk_pct=0.0,
                status="RESERVED",
            ),
        }

        return {
            "timestamp": int(time.time() * 1000),
            "total_equity_usdt": round(eq, 2),
            "initial_equity_usdt": round(init_eq, 2),
            "equity_floor_usdt": equity_floor,
            "daily_loss_limit_usdt": daily_loss_limit,
            "today_realized_loss_usdt": round(today_realized_loss_usdt, 2),
            "drawdown_from_initial_pct": drawdown_from_init_pct,
            "is_global_stop_triggered": is_global_stop,
            "is_daily_breaker_triggered": is_daily_broken,
            "allocation_profile": "AGGRESSIVE_TREND_60_30_10",
            "slots": {k: vars(v) for k, v in slots.items()},
        }

    def can_engine_open(
        self,
        engine_id: str,
        required_margin_usdt: float,
        total_equity_usdt: float,
        initial_equity_usdt: float = 10000.0,
        today_realized_loss_usdt: float = 0.0,
    ) -> Dict[str, Any]:
        """校验指定引擎的新增开仓保证金是否在自身槽位预算内，严禁跨引擎侵占资金或动用 10% 安全垫"""
        overview = self.compute_slot_budgets(total_equity_usdt, initial_equity_usdt, today_realized_loss_usdt)
        if overview["is_global_stop_triggered"]:
            return {"allowed": False, "reason": f"触发全局 18% 回撤停机底线 (${overview['equity_floor_usdt']})，禁止一切新开仓"}
        if overview["is_daily_breaker_triggered"]:
            return {"allowed": False, "reason": f"触发单日 3% 亏损熔断上限 (${overview['daily_loss_limit_usdt']})，今日禁止新开仓"}

        slot = overview["slots"].get(engine_id)
        if not slot:
            return {"allowed": False, "reason": f"未知引擎槽位: {engine_id}"}

        if required_margin_usdt > slot["available_usdt"]:
            return {
                "allowed": False,
                "reason": (
                    f"引擎 [{engine_id}] 槽位可用预算不足: 需 ${required_margin_usdt:.2f} USDT，"
                    f"当前槽位剩余 ${slot['available_usdt']:.2f} / 总额 ${slot['allocated_usdt']:.2f} USDT (严格隔离不侵占其他引擎)"
                ),
            }
        return {"allowed": True, "reason": "通过双引擎资金槽位隔离校验", "slot": slot}

    def update_engine_used_margin(self, engine_id: str, used_margin_usdt: float) -> None:
        """同步更新单个引擎当前已占用的保证金"""
        if engine_id in self.used_margin_by_engine:
            self.used_margin_by_engine[engine_id] = max(0.0, float(used_margin_usdt))
