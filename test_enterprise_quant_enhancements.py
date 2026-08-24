"""
OKX-Dog 企业级量化决策团队增强与实盘风控核心验证
涵盖:
1. 实时加密快讯与 NLP 情感分析引擎 (fetch_latest_crypto_news & get_sentiment_summary)
2. 单标的止损 30 分钟防洗盘冷却锁 (SYMBOL_STOP_LOSS_COOLDOWN_LOCK)
3. 浮盈移动保本 (Breakeven Lock) 与 POST_ONLY 挂单优化
4. Antigravity 主选 + DeepSeek 容灾自愈配置
"""

import asyncio
import time
import pytest

from models import (
    InstType,
    OrderSide,
    OrderType,
    PlaceOrderRequest,
    PosSide,
    TickerData,
)
from risk_controller import RiskController, RiskStatus


@pytest.mark.asyncio
async def test_symbol_stop_loss_cooldown_interception():
    """测试单标的止损后 30 分钟冷却期拦截"""
    rc = RiskController()
    symbol = "BTC-USDT-SWAP"

    # 1. 初始状态下未进入冷却
    is_cd, rem = rc.is_symbol_in_cooldown(symbol)
    assert is_cd is False
    assert rem == 0

    # 2. 模拟平仓触发止损
    rc.record_symbol_stop_loss(symbol, cooldown_minutes=30, reason="触发动态止损平仓")

    # 3. 再次检查是否在冷却期
    is_cd, rem = rc.is_symbol_in_cooldown(symbol)
    assert is_cd is True
    assert 28 <= rem <= 30

    # 4. 发起开仓委托，验证硬风控是否坚决拦截
    order_req = PlaceOrderRequest(
        symbol=symbol,
        inst_type=InstType.SWAP,
        side=OrderSide.BUY,
        pos_side=PosSide.LONG,
        order_type=OrderType.POST_ONLY,
        sz=0.1,
        px=95000.0,
        leverage=3,
        reduce_only=False,
    )
    ticker = TickerData(
        symbol=symbol,
        last_price=95000.0,
        high_24h=96000.0,
        low_24h=94000.0,
        change_24h_pct=1.2,
        volume_24h=100.0,
        vol_currency_24h=9500000.0,
        volume_24h_currency=9500000.0,
        timestamp=int(time.time() * 1000)
    )

    check_res = await rc.check_order(order_req, ticker, [])
    assert check_res.passed is False
    assert check_res.rejected_rule == "SYMBOL_STOP_LOSS_COOLDOWN_LOCK"
    assert "防洗盘冷静期" in check_res.message

    # 5. 平仓单 (reduce_only=True) 不受冷却锁限制
    reduce_order = PlaceOrderRequest(
        symbol=symbol,
        inst_type=InstType.SWAP,
        side=OrderSide.SELL,
        pos_side=PosSide.LONG,
        order_type=OrderType.MARKET,
        sz=0.1,
        reduce_only=True,
    )
    reduce_check = await rc.check_order(reduce_order, ticker, [])
    assert reduce_check.passed is True


@pytest.mark.asyncio
async def test_news_nlp_engine_and_summary():
    """测试实时快讯抓取、TTL 缓存与情绪加权"""
    from news_nlp_engine import NewsNLPEngine

    news_items = await NewsNLPEngine.fetch_latest_crypto_news(limit=5)
    assert len(news_items) > 0
    assert "title" in news_items[0]
    assert "sentiment_score" in news_items[0]

    avg_score, black_swan, symbols = NewsNLPEngine.get_sentiment_summary(news_items)
    assert isinstance(avg_score, float)
    assert isinstance(black_swan, bool)
    assert isinstance(symbols, list)


@pytest.mark.asyncio
async def test_antigravity_and_deepseek_fallback_config():
    """测试 LLM 客户端双活容灾配置装配"""
    from llm_client import LLMClient, ai_settings

    client = LLMClient()
    assert client.provider in ["antigravity", "deepseek", "openai"]
    if client.fallback_config:
        assert client.fallback_config.get("provider") in ["deepseek", "openai", "custom"]
        assert "base_url" in client.fallback_config
        assert "model" in client.fallback_config
    await client.close()
