"""
OKX-Dog 高级数理量化与风控分析单元测试
测试内容:
1. DcaLiquidationCalculator: 强平穿透预判、连锁加仓安全性、极限逆势保证金精算
2. KalmanPairEngine: 两状态自适应卡尔曼滤波、动态协整斜率/截距、Z-Score偏离与发散保护
3. RiskController: 加仓强平穿透前置硬拦截逻辑 (LIQUIDATION_PENETRATION_LOCK)
"""

import math
import pytest
import numpy as np

from core.dca_liquidation_calculator import DcaLiquidationCalculator
from core.kalman_pair_engine import KalmanPairEngine
from models import (
    InstType,
    OrderSide,
    OrderType,
    PlaceOrderRequest,
    PositionModel,
    PosSide,
    RiskStatus,
    TickerData,
)
from risk_controller import RiskController


def test_dca_liquidation_calculator_safe_case():
    """测试安全的 DCA 加仓链条 (低杠杆+小步长)"""
    res = DcaLiquidationCalculator.calculate_dca_chain(
        direction="long",
        initial_price=60000.0,
        leverage=3.0,
        initial_margin=100.0,
        dca_margin_base=50.0,
        max_dca_orders=3,
        price_deviation_percent=1.0,
        extra_margin=10.0
    )
    assert res.is_safe is True
    assert res.max_safe_dca_orders >= 1
    assert res.breakeven_price <= 60000.0
    assert res.current_liq_price < 60000.0
    assert res.liq_safety_buffer_pct > 0
    assert res.required_extreme_margin > 0
    assert res.violation_reason is None


def test_dca_liquidation_calculator_penetration_detected():
    """测试极端高杠杆且步长过大导致未决穿仓的情况"""
    # 50倍杠杆，跌 2.5% 就直接爆仓，若设置加仓步长为 5% 则必定在加仓前先爆仓
    res = DcaLiquidationCalculator.calculate_dca_chain(
        direction="long",
        initial_price=100.0,
        leverage=50.0,
        initial_margin=100.0,
        dca_margin_base=100.0,
        max_dca_orders=3,
        price_deviation_percent=5.0,  # 计划在 -5% 加仓，但 50x 杠杆在 -2% 左右就已经爆仓
        extra_margin=0.0
    )
    assert res.is_safe is False
    assert res.violation_reason is not None
    assert "先行爆仓" in res.violation_reason or "未决暴毙死穴" in res.violation_reason


def test_dca_extreme_margin_stress():
    """测试 30% 极端逆势下的最低隔离保证金精算"""
    # 1 BTC @ 60000 USDT, 3x 杠杆, 承受 30% 暴跌
    margin_req = DcaLiquidationCalculator.calculate_extreme_margin_stress(
        direction="long",
        initial_price=60000.0,
        leverage=3.0,
        target_loss_percent=30.0,
        initial_size=1.0,
        maint_margin_ratio=0.005
    )
    # 浮亏 18000 + MMR 300 = 18300 USDT
    assert margin_req >= 18300.0


def test_kalman_pair_engine_convergence_and_signals():
    """测试卡尔曼滤波协整跟踪与 Z-Score 计算"""
    engine = KalmanPairEngine(
        main_symbol="BTC-USDT-SWAP",
        sub_symbol="ETH-USDT-SWAP",
        z_lookback=20,
        z_entry=1.5,
        z_exit=0.5
    )

    # 模拟合成数据: log(ETH) = 0.8 * log(BTC) - 2.0 + noise
    np.random.seed(42)
    btc_prices = np.linspace(60000, 65000, 50)
    eth_prices = []
    for btc in btc_prices:
        log_btc = math.log(btc)
        noise = np.random.normal(0, 0.002)
        log_eth = 0.8 * log_btc - 2.0 + noise
        eth_prices.append(math.exp(log_eth))

    metrics_list = engine.batch_train_sequence(list(btc_prices), eth_prices)
    assert len(metrics_list) == 50

    latest = metrics_list[-1]
    # 动态 beta 应该接近 0.8
    assert 0.5 <= latest.dynamic_beta <= 1.1
    assert latest.is_divergent is False
    assert latest.suggested_arbitrage_action in ["NONE", "SHORT_SUB_LONG_MAIN", "LONG_SUB_SHORT_MAIN", "CLOSE"]


@pytest.mark.asyncio
async def test_risk_controller_liquidation_penetration_interception():
    """测试风控中枢前置拦截击穿强平价的危险加仓单"""
    rc = RiskController()

    # 构造当前持多仓：开仓价 60000，强平价 55000
    pos = PositionModel(
        symbol="BTC-USDT-SWAP",
        inst_type=InstType.SWAP,
        pos_side=PosSide.LONG,
        pos=1.0,
        avg_price=60000.0,
        mark_price=58000.0,
        liquidation_price=55000.0,
        leverage=10,
        margin=6000.0
    )

    # 尝试提交一个委托价格 54000 的加仓买单 (低于强平价 55000)
    order = PlaceOrderRequest(
        symbol="BTC-USDT-SWAP",
        inst_type=InstType.SWAP,
        side=OrderSide.BUY,
        pos_side=PosSide.LONG,
        order_type=OrderType.POST_ONLY,
        size=0.1,
        price=54000.0,  # 54000 <= 55000，先行爆仓！
        leverage=10,
        stop_loss_price=50000.0
    )

    ticker = TickerData(
        symbol="BTC-USDT-SWAP",
        last_price=58000.0,
        mark_price=58000.0,
        best_bid=57999.0,
        best_ask=58001.0,
        volume_24h=1000.0,
        high_24h=59000.0,
        low_24h=57000.0,
        change_24h=0.0,
        change_24h_pct=0.0,
        volume_24h_currency=58000000.0,
        timestamp=int(1000000)
    )

    result = await rc.check_order(order=order, current_ticker=ticker, current_positions=[pos])

    assert result.passed is False
    assert result.rejected_rule == "LIQUIDATION_PENETRATION_LOCK"
    assert "强平穿透硬拦截" in result.message
