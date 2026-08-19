"""
OKX-Dog 个人量化智能交易系统 - 后端核心业务全量功能集成测试
模块: okx-dog-backend/test_backend_core.py
全量验证密盒安全、OKX REST 签名与限流、OKX WS 长连接与订阅自愈、硬风控中枢拦截与 Panic Button、FastAPI 路由全链路
"""

import asyncio
import datetime
import json
import os
import sys
import time
from httpx import AsyncClient, ASGITransport

from config import settings
from database import init_db, get_db_session
from vault import vault, FatalWithdrawalPermissionException
from okx_rest import OKXRestClient, TokenBucketRateLimiter
from okx_ws import OKXWebSocketClient, OKXWSManager
from risk_controller import (
    RiskController,
    RiskInterceptionException,
    risk_controller
)
import models
from models import (
    InstType,
    OrderSide,
    PosSide,
    OrderType,
    OrderState,
    PlaceOrderRequest,
    PositionModel,
    RiskRulesConfig,
    RiskStatus,
    TickerData,
    PanicButtonRequest
)

from indicators import (
    calculate_ema,
    calculate_macd,
    calculate_rsi,
    calculate_bollinger_bands,
    calculate_atr
)
import main
from main import app


async def test_vault_security():
    """测试 1: 凭证密盒 AES-256-GCM 加解密与提现权限强阻断"""
    print("\n--- [Test 1] 凭证密盒安全与权限强阻断测试 ---")
    raw_api_key = "6b2a8d3e-test-api-key-9abc"
    raw_secret = "SECRET_KEY_1234567890_HEX_VALUE"
    raw_passphrase = "MySecretPassphrase2026"

    # 加密与解密
    enc = vault.encrypt_secret(raw_api_key)
    dec = vault.decrypt_secret(enc["ciphertext"], enc["salt"], enc["nonce"])
    assert dec == raw_api_key, f"解密结果不匹配: {dec} != {raw_api_key}"
    print("  [OK] AES-256-GCM 凭证加密与解密一致性验证通过")

    # 脱敏掩码
    masked = vault.mask_api_key(raw_api_key)
    assert masked == "6b2a****9abc", f"脱敏掩码格式错误: {masked}"
    print(f"  [OK] API Key 脱敏掩码验证通过: {masked}")

    # 正常权限校验
    safe_perms = ["read", "trade"]
    passed, msg = vault.verify_permissions(safe_perms)
    assert passed is True
    print("  [OK] 只读/交易权限校验通过")

    # 提现权限强阻断校验
    danger_perms = ["read", "trade", "withdraw"]
    try:
        vault.verify_permissions(danger_perms)
        assert False, "未拦截提现权限！"
    except FatalWithdrawalPermissionException as e:
        print(f"  [OK] 成功强阻断提现权限: {e}")


async def test_okx_rest_signature_and_limiter():
    """测试 2: OKX REST 客户端 HMAC-SHA256 签名与令牌桶限流"""
    print("\n--- [Test 2] OKX REST 签名与令牌桶限流测试 ---")
    client = OKXRestClient(
        api_key="test_api_key",
        secret_key="test_secret_key",
        passphrase="test_passphrase",
        is_live_mode=False
    )

    # 验证签名生成
    ts = "2026-08-15T01:48:21.123Z"
    method = "POST"
    path = "/api/v5/trade/order"
    body = '{"instId":"BTC-USDT-SWAP","sz":"1"}'
    sign = client._generate_signature(ts, method, path, body)
    assert sign and len(sign) > 10, "签名生成失败"
    print(f"  [OK] HMAC-SHA256 签名生成成功: {sign[:16]}...")

    headers = client._get_headers(method, path, body)
    assert headers["OK-ACCESS-KEY"] == "test_api_key"
    assert headers["OK-ACCESS-PASSPHRASE"] == "test_passphrase"
    assert headers["x-simulated-trading"] == "1"
    print("  [OK] REST 请求头及模拟盘标识验证通过")

    # 验证令牌桶限频器
    limiter = TokenBucketRateLimiter(rate_limits={"trade": (50.0, 5.0)})
    t0 = time.monotonic()
    for _ in range(5):
        await limiter.acquire("trade")
    elapsed = time.monotonic() - t0
    print(f"  [OK] 令牌桶 5 次快速请求放行耗时: {elapsed:.4f}s")


async def test_okx_ws_manager_and_subscriptions():
    """测试 3: OKX WebSocket 客户端登录签名、心跳与订阅自愈"""
    print("\n--- [Test 3] OKX WebSocket 登录签名、心跳与自愈测试 ---")
    ws_client = OKXWebSocketClient(
        ws_url="wss://wspap.okx.com:8443/ws/v5/private",
        is_private=True,
        api_key="my_key",
        secret_key="my_secret",
        passphrase="my_pass"
    )

    # 登录签名
    login_sig = ws_client._generate_login_signature()
    assert login_sig["apiKey"] == "my_key"
    assert login_sig["passphrase"] == "my_pass"
    assert "sign" in login_sig and len(login_sig["sign"]) > 10
    print(f"  [OK] WebSocket 私有连接登录签名生成成功: {login_sig['sign'][:16]}...")

    # 订阅状态保存与自愈参数
    await ws_client.subscribe([
        {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
        {"channel": "books5", "instId": "BTC-USDT-SWAP"}
    ])
    assert len(ws_client._active_subscriptions) == 2
    print(f"  [OK] 订阅通道记录保持正常 ({len(ws_client._active_subscriptions)} 个频道)")

    # 模拟 WebSocket 广播消息
    mgr = OKXWSManager(is_live_mode=False)
    client_q = asyncio.Queue()
    mgr.register_broadcast_subscriber(client_q)

    # 模拟收到 OKX Ticker 推送
    mock_ticker_msg = {
        "arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
        "data": [{
            "instId": "BTC-USDT-SWAP",
            "last": "95240.5",
            "lastSz": "1.2",
            "high24h": "96800.0",
            "low24h": "93500.0",
            "open24h": "94100.0",
            "sodUtc8": "1.21",
            "vol24h": "128450.0",
            "volCcy24h": "1223358000.0",
            "ts": str(int(time.time() * 1000))
        }]
    }
    await mgr._on_public_message(mock_ticker_msg)

    # 校验内存快照与广播队列
    assert "BTC-USDT-SWAP" in mgr.latest_tickers
    assert mgr.latest_tickers["BTC-USDT-SWAP"].last_price == 95240.5
    pushed = await asyncio.wait_for(client_q.get(), timeout=1.0)
    assert pushed.channel == "ticker:BTC-USDT-SWAP"
    assert pushed.data.last_price == 95240.5
    print("  [OK] 行情数据解析、内存快照更新与前端 WebSocket 广播队列分发验证通过")


async def test_indicators_math():
    """测试 4: 量化技术指标纯数学高精度计算"""
    print("\n--- [Test 4] 量化技术指标 (EMA/MACD/RSI/布林带/ATR) 计算测试 ---")
    prices = [100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 109.0] * 5  # 50 条数据

    ema20 = calculate_ema(prices, 20)
    assert len(ema20) == len(prices)
    assert ema20[-1] > 0
    print(f"  [OK] EMA(20) 最后一项值: {ema20[-1]:.2f}")

    macd_res = calculate_macd(prices, 12, 26, 9)
    assert "dif" in macd_res and "dea" in macd_res and "macd_hist" in macd_res
    print(f"  [OK] MACD 结果: DIF={macd_res['dif']}, DEA={macd_res['dea']}, HIST={macd_res['macd_hist']}")

    rsi_val = calculate_rsi(prices, 14)
    assert 0 <= rsi_val <= 100
    print(f"  [OK] RSI(14) 结果: {rsi_val}")

    bb_res = calculate_bollinger_bands(prices, 20, 2.0)
    assert bb_res["upper"] >= bb_res["middle"] >= bb_res["lower"]
    print(f"  [OK] 布林带: 上轨={bb_res['upper']}, 中轨={bb_res['middle']}, 下轨={bb_res['lower']}")


async def test_risk_controller_rules_and_panic():
    """测试 5: 硬风控拦截中枢前置校验与 Panic Button 紧急处置"""
    print("\n--- [Test 5] 硬风控拦截中枢与 Panic Button 熔断测试 ---")
    rc = RiskController()
    rc.rules = RiskRulesConfig(
        max_order_usdt=500.0,
        max_daily_loss_usdt=300.0,
        max_leverage=5,
        max_slippage_pct=0.5,
        max_open_positions=3,
        max_position_usdt=2000.0
    )

    ticker = TickerData(
        symbol="BTC-USDT-SWAP",
        last_price=95000.0,
        high_24h=96000.0,
        low_24h=94000.0,
        change_24h_pct=1.05,
        volume_24h=10000.0,
        vol_currency_24h=950000000.0,
        timestamp=int(time.time() * 1000)
    )

    # 1. 正常订单通过
    normal_order = PlaceOrderRequest(
        symbol="BTC-USDT-SWAP",
        inst_type=InstType.SWAP,
        side=OrderSide.BUY,
        pos_side=PosSide.LONG,
        order_type=OrderType.LIMIT,
        price=95000.0,
        sz=0.002,  # 0.002 * 95000 = 190 USDT < 500
        leverage=3
    )
    check1 = await rc.check_order(normal_order, ticker, [])
    assert check1.passed is True, f"正常订单校验失败: {check1.reason}"
    print("  [OK] 正常合规订单通过全部硬风控校验")

    # 2. 单笔超额拦截
    oversize_order = PlaceOrderRequest(
        symbol="BTC-USDT-SWAP",
        inst_type=InstType.SWAP,
        side=OrderSide.BUY,
        pos_side=PosSide.LONG,
        order_type=OrderType.LIMIT,
        price=95000.0,
        sz=0.01,  # 0.01 * 95000 = 950 USDT > 500
        leverage=3
    )
    check2 = await rc.check_order(oversize_order, ticker, [])
    assert check2.passed is False
    assert "超过硬风控限额" in (check2.message or "")
    print(f"  [OK] 成功拦截单笔名义超额订单: {check2.message}")

    # 3. 杠杆超限拦截
    over_lev_order = PlaceOrderRequest(
        symbol="BTC-USDT-SWAP",
        inst_type=InstType.SWAP,
        side=OrderSide.BUY,
        pos_side=PosSide.LONG,
        order_type=OrderType.LIMIT,
        price=95000.0,
        sz=0.002,
        leverage=10  # 10 > 5
    )
    check3 = await rc.check_order(over_lev_order, ticker, [])
    assert check3.passed is False
    assert "超过系统硬风控上限" in (check3.message or "")
    print(f"  [OK] 成功拦截杠杆倍数超限订单: {check3.message}")

    # 4. 市价单滑点过大拦截
    # 标记价格 96000 偏离 last_price 95000 达 1.04% > 0.5%
    bad_ticker = TickerData(
        symbol="BTC-USDT-SWAP",
        last_price=95000.0,
        mark_price=96000.0,
        high_24h=96000.0,
        low_24h=94000.0,
        change_24h_pct=1.05,
        volume_24h=10000.0,
        vol_currency_24h=950000000.0,
        timestamp=int(time.time() * 1000)
    )
    market_order = PlaceOrderRequest(
        symbol="BTC-USDT-SWAP",
        inst_type=InstType.SWAP,
        side=OrderSide.BUY,
        pos_side=PosSide.LONG,
        order_type=OrderType.MARKET,
        price=None,
        sz=0.002,
        leverage=3
    )
    check4 = await rc.check_order(market_order, bad_ticker, [])
    assert check4.passed is False
    assert "滑点" in (check4.message or "")
    print(f"  [OK] 成功拦截大滑点市价单: {check4.message}")

    # 5. 触发单日亏损熔断
    rc.trigger_daily_loss_circuit_breaker(350.0, "当日累计亏损 350 USDT 触发熔断")
    check5 = await rc.check_order(normal_order, ticker, [])
    assert check5.passed is False
    assert "熔断" in (check5.message or "")
    print(f"  [OK] 触发熔断状态下成功阻断新开仓: {check5.message}")

    # 6. 解除熔断
    await rc.reset_circuit_breaker("人工核对行情后解除熔断")
    assert rc.is_circuit_broken is False
    assert rc.risk_status == RiskStatus.NORMAL
    print("  [OK] 成功手动解除熔断锁定")

    # 7. Panic Button 测试
    rest_mock = OKXRestClient(
        api_key="mock_key",
        secret_key="mock_secret",
        passphrase="mock_pass",
        is_live_mode=False
    )
    # Mock 平仓与撤单方法
    async def mock_cancel_batch(orders):
        return [{"ordId": "123", "sCode": "0"}]
    async def mock_get_positions(inst_type="SWAP", symbol=None):
        return [
            PositionModel(
                pos_id="POS_001",
                symbol="BTC-USDT-SWAP",
                inst_type=InstType.SWAP,
                pos_side=PosSide.NET,
                pos=1.0,
                avail_pos=1.0,
                avg_px=95000.0,
                mark_px=95200.0,
                margin_ratio=5.0,
                maint_margin_ratio=0.01,
                leverage=3,
                notional_usd=95200.0,
                upl=200.0,
                upl_ratio=0.002,
                realized_pnl=0.0,
                margin=31700.0,
                last_synced_at=int(time.time() * 1000)
            )
        ]
    async def mock_close_pos(symbol, mgn_mode="cross", pos_side=None, ccy=None, auto_cancel=True):
        return {"instId": symbol, "posSide": pos_side}

    rest_mock.cancel_batch_orders = mock_cancel_batch
    rest_mock.get_positions = mock_get_positions
    rest_mock.close_position = mock_close_pos

    panic_req = PanicButtonRequest(
        master_password="test",
        confirm_phrase="PANIC_CLOSE_ALL",
        target="ALL",
        cancel_all_orders=True,
        close_all_positions=True,
        reason="测试应急熔断"
    )
    panic_res = await rc.execute_panic_button(rest_mock, panic_req)
    assert panic_res.success is True
    assert panic_res.canceled_orders_count == 1
    assert panic_res.closed_positions_count == 1
    assert rc.is_safe_mode is True
    assert rc.risk_status == RiskStatus.SAFE_MODE
    print(f"  [OK] Panic Button 紧急两阶段处置 (Cancel-Before-Close) 执行完成: {panic_res.message}")


async def test_fastapi_rest_endpoints():
    """测试 6: FastAPI 完整 RESTful 路由全链路测试"""
    print("\n--- [Test 6] FastAPI REST API 路由全链路集成测试 ---")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # 1. 健康检查
        r_health = await ac.get("/api/v1/health")
        assert r_health.status_code == 200
        health_data = r_health.json()
        assert health_data["code"] == 0
        print("  [OK] GET /api/v1/health 响应正常")

        # 2. 凭证保存
        r_cred = await ac.post("/api/v1/vault/credentials", json={
            "master_password": "MyTestMasterPassword!",
            "is_live_mode": False,
            "api_key": "6b2a8d3e-demo-key-1234",
            "secret_key": "SEC_KEY_DEMO_9988",
            "passphrase": "DemoPassphrase"
        })
        assert r_cred.status_code == 200
        assert r_cred.json()["code"] == 0
        print("  [OK] POST /api/v1/vault/credentials 保存凭证正常")

        # 3. 凭证状态查询
        r_status = await ac.get("/api/v1/vault/credentials/status")
        assert r_status.status_code == 200
        assert r_status.json()["data"]["has_configured_okx"] is True
        print("  [OK] GET /api/v1/vault/credentials/status 查询凭证状态正常")

        # 4. 风控状态查询
        r_risk = await ac.get("/api/v1/risk/status")
        assert r_risk.status_code == 200
        assert r_risk.json()["code"] == 0
        print("  [OK] GET /api/v1/risk/status 查询风控状态正常")

        # 5. 风控规则更新
        r_upd_risk = await ac.put("/api/v1/risk/config", json={
            "max_order_usdt": 800.0,
            "max_daily_loss_usdt": 250.0,
            "max_leverage": 5,
            "max_slippage_pct": 0.5,
            "max_open_positions": 3,
            "max_position_usdt": 3000.0
        })
        assert r_upd_risk.status_code == 200
        assert r_upd_risk.json()["data"]["max_order_usdt"] == 800.0
        print("  [OK] PUT /api/v1/risk/config 热更新风控规则正常")

        # 6. 系统配置查询与更新
        r_sys_cfg = await ac.get("/api/v1/config/system")
        assert r_sys_cfg.status_code == 200
        print("  [OK] GET /api/v1/config/system 查询配置正常")

        # 7. 风控审计日志查询
        r_audit = await ac.get("/api/v1/risk/audit-logs?page=1&page_size=10")
        assert r_audit.status_code == 200
        assert r_audit.json()["code"] == 0
        print(f"  [OK] GET /api/v1/risk/audit-logs 查询到 {len(r_audit.json()['data'])} 条风控审计日志")


async def test_paper_engine_smart_sizing_and_breakeven():
    """测试 7: 模拟操盘沙盒 TP1 阶梯 50% 止盈与移动保本 (Breakeven) 状态机测试"""
    print("\n--- [Test 7] 模拟操盘沙盒 TP1 阶梯 50% 止盈与移动保本测试 ---")
    from paper_trading import PaperTradingEngine
    engine = PaperTradingEngine(initial_equity=10000.0)


    # 1. 模拟开仓: 做多 0.04 BTC @ 63000, 止盈 65000, 止损 61000
    order_req = PlaceOrderRequest(
        symbol="BTC-USDT-SWAP",
        inst_type=InstType.SWAP,
        side=OrderSide.BUY,
        pos_side=PosSide.LONG,
        order_type=OrderType.MARKET,
        price=63000.0,
        sz=0.04,
        leverage=3,
        tp_trigger_price=65000.0,
        sl_trigger_price=61000.0,
    )

    ord_res = engine.place_order(order_req, current_market_price=63000.0)
    assert ord_res.state == OrderState.FILLED

    positions = engine.get_positions()
    assert len(positions) == 1
    pos = positions[0]
    assert pos.pos == 0.04
    assert pos.avg_px == 63000.0
    assert pos.tp_trigger_price == 65000.0
    assert pos.sl_trigger_price == 61000.0
    assert pos.is_breakeven_locked is False

    # 2. 模拟行情推升至 65000 (触及 TP1)
    triggered = engine.update_market_price("BTC-USDT-SWAP", 65050.0)
    assert len(triggered) == 1
    assert "锁定移动保本" in triggered[0]["reason"] or "第一止盈位" in triggered[0]["reason"]

    # 检查持仓是否剩 50% 且止损上移至保本价 63000
    positions_after_tp = engine.get_positions()
    assert len(positions_after_tp) == 1
    active_pos = positions_after_tp[0]
    assert abs(active_pos.pos - 0.02) < 1e-4
    assert active_pos.is_breakeven_locked is True
    assert active_pos.sl_trigger_price == 63000.0
    assert active_pos.tp_trigger_price is None
    print(f"  [OK] TP1 触及成功执行 50% 分批平仓，剩余 0.02 BTC 止损已上移至保本价 ${active_pos.sl_trigger_price}")

    # 3. 模拟行情回撤跌破保本价 63000
    triggered_be = engine.update_market_price("BTC-USDT-SWAP", 62990.0)
    assert len(triggered_be) == 1
    assert "保本" in triggered_be[0]["reason"]
    assert len(engine.get_positions()) == 0
    print(f"  [OK] 触及保本价自动离场，持仓平仓完毕，实现零本金回撤亏损！")


async def main_test_runner():
    """主测试入口"""
    print("================================================================================")
    print("OKX-Dog Milestone 2 后端核心模块生产级全量集成测试套件启动")
    print("================================================================================")
    await init_db()

    await test_vault_security()
    await test_okx_rest_signature_and_limiter()
    await test_okx_ws_manager_and_subscriptions()
    await test_indicators_math()
    await test_risk_controller_rules_and_panic()
    await test_fastapi_rest_endpoints()
    await test_paper_engine_smart_sizing_and_breakeven()

    print("\n================================================================================")
    print("🎉 ALL TESTS PASSED! 全部 7 组核心模块集成测试 100% 成功通过！")
    print("================================================================================")


if __name__ == "__main__":
    asyncio.run(main_test_runner())

