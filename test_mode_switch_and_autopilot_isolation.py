"""
OKX-Dog 个人量化智能交易系统 - 模拟盘/实盘脱机量化隔离与模式切换安全性测试
模块: okx-dog-backend/test_mode_switch_and_autopilot_isolation.py
"""

import pytest
from state import auto_pilot_manager, paper_engine, rest_client


@pytest.mark.asyncio
async def test_autopilot_dual_environment_isolation_and_safe_switching():
    """
    验证模拟盘与实盘脱机量化收益的彻底物理隔离与模式切换安全机制
    """
    print("\n--- [Test Mode Isolation] 模拟盘/实盘脱机量化收益彻底物理隔离与切换安全验证 ---")

    # 1. 模拟盘环境初始化与启动
    paper_engine.reset_account(10000.0)
    demo_status = await auto_pilot_manager.start(
        initial_equity=10000.0,
        is_live_mode=False,
        symbol="BTC-USDT-SWAP",
        focus_symbols=["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
        focus_mode="SINGLE_SYMBOL",
        max_scale_layers=2,
        scale_in_strategy="PYRAMIDING",
    )
    assert demo_status.is_running is True
    assert demo_status.is_live_mode is False
    assert demo_status.max_scale_layers == 2

    # 模拟盘产生 2 笔模拟平仓战绩与快照
    trade1 = await auto_pilot_manager.record_trade(
        symbol="BTC-USDT-SWAP",
        pos_side="long",
        entry_price=90000.0,
        exit_price=91500.0,
        size=0.1,
        leverage=3,
        realized_pnl=150.0,
        roi_pct=5.0,
        reason="模拟盘移动止盈",
        is_live_mode=False,
    )
    await auto_pilot_manager.record_snapshot(
        current_equity=10150.0,
        realized_pnl=150.0,
        unrealized_pnl=0.0,
        is_live_mode=False,
    )

    demo_check = auto_pilot_manager.get_status(is_live_mode=False)
    demo_hist = auto_pilot_manager.get_history(is_live_mode=False)
    assert demo_check.realized_pnl_usdt == 150.0
    assert demo_check.total_trades_count == 1
    assert len(demo_hist.trades) == 1
    assert len(demo_hist.curve) >= 2
    print(f"  [OK] 模拟盘操盘已启动并记录收益: +${demo_check.realized_pnl_usdt} USDT, 历史交易数: {demo_check.total_trades_count}")

    # 2. 模拟交易员在顶部切换至【真实实盘】
    switched_status = await auto_pilot_manager.switch_environment(
        target_live_mode=True,
        current_equity=5000.0,
    )

    # 验证原模拟盘操盘被安全停止
    assert auto_pilot_manager.get_state(False).is_running is False
    print("  [OK] 切换至实盘时，原模拟盘操盘已被安全停止 (Safety Auto-Stop)")

    # 验证当前激活环境已变为实盘，且实盘默认处于停止状态（需交易员确认开启）
    assert switched_status.is_live_mode is True
    assert switched_status.is_running is False
    print("  [OK] 目标实盘环境默认处于未开启状态，等待交易员确认")

    # 验证实盘环境的收益战绩与模拟盘 100% 隔离（实盘没有任何模拟盘的平仓数据）
    live_status = auto_pilot_manager.get_status(is_live_mode=True)
    live_hist = auto_pilot_manager.get_history(is_live_mode=True)
    assert live_status.realized_pnl_usdt == 0.0
    assert live_status.total_trades_count == 0
    assert len(live_hist.trades) == 0
    print("  [OK] 实盘收益数据与战绩 100% 物理隔离，未包含任何模拟盘数据")

    # 3. 实盘配置独立的策略参数并启动
    await auto_pilot_manager.update_config(
        focus_mode="MULTI_SYMBOL_POOL",
        focus_symbols=["BTC-USDT-SWAP", "XAU-USDT-SWAP", "NVDA-USDT-SWAP"],
        max_scale_layers=4,
        scale_in_strategy="SMART_HYBRID",
        is_live_mode=True,
    )
    live_start = await auto_pilot_manager.start(
        initial_equity=5000.0,
        is_live_mode=True,
        symbol="XAU-USDT-SWAP",
        focus_symbols=["BTC-USDT-SWAP", "XAU-USDT-SWAP", "NVDA-USDT-SWAP"],
        focus_mode="MULTI_SYMBOL_POOL",
        max_scale_layers=4,
        scale_in_strategy="SMART_HYBRID",
    )
    assert live_start.is_running is True
    assert live_start.is_live_mode is True
    assert live_start.focus_mode == "MULTI_SYMBOL_POOL"
    assert live_start.max_scale_layers == 4

    # 实盘产生 1 笔真实战绩
    await auto_pilot_manager.record_trade(
        symbol="XAU-USDT-SWAP",
        pos_side="short",
        entry_price=2680.0,
        exit_price=2660.0,
        size=1.0,
        leverage=5,
        realized_pnl=20.0,
        roi_pct=3.7,
        reason="实盘AI量化止盈",
        is_live_mode=True,
    )
    live_check = auto_pilot_manager.get_status(is_live_mode=True)
    assert live_check.realized_pnl_usdt == 20.0
    assert live_check.total_trades_count == 1
    print(f"  [OK] 实盘操盘已启动并记录实盘战绩: +${live_check.realized_pnl_usdt} USDT")

    # 4. 再次切换回【模拟操盘沙盒】
    switched_back = await auto_pilot_manager.switch_environment(
        target_live_mode=False,
        current_equity=10150.0,
    )

    # 验证实盘操盘已自动安全暂停
    assert auto_pilot_manager.get_state(True).is_running is False
    print("  [OK] 切换回模拟盘时，原实盘操盘已被安全停止")

    # 验证模拟盘的原有战绩完好无损保留，且记住了上一次的配置
    demo_restored = auto_pilot_manager.get_status(is_live_mode=False)
    demo_hist_restored = auto_pilot_manager.get_history(is_live_mode=False)
    assert demo_restored.is_live_mode is False
    assert demo_restored.realized_pnl_usdt == 150.0
    assert demo_restored.total_trades_count == 1
    assert len(demo_hist_restored.trades) == 1
    assert demo_restored.max_scale_layers == 2
    assert demo_restored.focus_mode == "SINGLE_SYMBOL"
    print(f"  [OK] 模拟盘原历史战绩 (+${demo_restored.realized_pnl_usdt} USDT) 与策略配置完全保留无损！")

    # 验证实盘的原有战绩依然独立保存在实盘状态中
    live_recheck = auto_pilot_manager.get_status(is_live_mode=True)
    assert live_recheck.realized_pnl_usdt == 20.0
    assert live_recheck.max_scale_layers == 4
    print(f"  [OK] 实盘战绩 (+${live_recheck.realized_pnl_usdt} USDT) 与配置亦独立保存在实盘中！")
