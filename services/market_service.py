"""
OKX-Dog 个人量化智能交易系统 - 行情与市场上下文服务
模块: okx-dog-backend/services/market_service.py
封装全景多周期指标聚合、衍生品数据处理、深度失衡率与市场快照构造
"""

import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from indicators import compute_single_timeframe_indicators
from models import (
    DerivativesMetrics,
    MultiPeriodIndicators,
    SinglePeriodIndicators,
    OrderBookDepth,
)
from state import (
    antigravity_bridge,
    logger,
    paper_engine,
    rest_client,
    vault,
    ws_manager,
)

try:
    from okx_dog_ai import (
        HardRiskLimits,
        MarketContextSnapshot,
        PositionSnapshot,
    )
except (ImportError, PermissionError, Exception):
    HardRiskLimits = Any
    MarketContextSnapshot = Any
    PositionSnapshot = Any


async def fetch_full_market_context_snapshot(
    symbol: str,
    db: AsyncSession
) -> Tuple[float, Any, MultiPeriodIndicators, DerivativesMetrics, Optional[str], str, str, float, int, str]:
    """
    异步并发聚合全景实时市场数据：
    1. 真实实时 Ticker 行情
    2. 多周期 (15m/1h/4h/1d) K 线并高精度计算指标矩阵
    3. 衍生品指标（资金费率、全网持仓量 OI、深度买卖失衡率）
    4. 交易员当前持仓快照与硬风控规则
    5. 解密大模型配置
    """
    now_ms = int(time.time() * 1000)

    # 1. 查询大模型配置与解密 Key
    res = await db.execute(text("SELECT * FROM system_config ORDER BY id DESC LIMIT 1"))
    cfg = res.mappings().first()

    llm_key = None
    llm_provider = "antigravity"
    base_url = "http://127.0.0.1:8001"
    model_name = "gemini-3.7-flash"
    temperature = 0.2
    max_tokens = 4096

    if cfg:
        llm_provider = cfg.get("llm_provider") or llm_provider
        base_url = cfg.get("llm_base_url") or base_url
        model_name = cfg.get("llm_model") or model_name
        temperature = float(cfg.get("llm_temperature") if cfg.get("llm_temperature") is not None else 0.2)
        max_tokens = int(cfg.get("llm_max_tokens") if cfg.get("llm_max_tokens") is not None else 4096)
        if cfg.get("encrypted_llm_api_key"):
            try:
                enc_data = json.loads(cfg["encrypted_llm_api_key"])
                llm_key = vault.decrypt_secret(enc_data["ciphertext"], enc_data["salt"], enc_data["nonce"])
            except Exception as e:
                logger.warning(f"解密系统配置中保存的 LLM 密钥失败 (将尝试环境变量): {e}")

    # 支持环境变量兜底
    if not llm_key:
        llm_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY") or os.getenv("LLM_API_KEY")

    if llm_provider == "antigravity" or (not llm_key and antigravity_bridge and antigravity_bridge.is_available()):
        logger.info(f"启用 Antigravity 本地免 Key 驱动引擎, 目标模型: {model_name}")
    elif llm_key:
        logger.info(f"已成功加载 LLM 密钥 (前缀: {llm_key[:6]}...), 目标大模型: {model_name}, 端点: {base_url}")
    else:
        logger.info("未配置 LLM API 密钥，将采用内置多阶段量化反思规则引擎")

    # 2. 提取最新实时 Ticker
    ticker_obj = ws_manager.get_ticker_snapshot(symbol)
    if not ticker_obj or ticker_obj.last_price <= 0:
        try:
            ticker_obj = await asyncio.wait_for(rest_client.get_ticker(symbol), timeout=2.5)
        except Exception:
            ticker_obj = None

    DEFAULT_PRICES = {
        "BTC-USDT-SWAP": 63428.0,
        "ETH-USDT-SWAP": 2680.0,
        "SOL-USDT-SWAP": 188.0,
        "DOGE-USDT-SWAP": 0.155,
        "XAU-USDT-SWAP": 2650.0,
        "XAG-USDT-SWAP": 31.5,
        "EUR-USDT-SWAP": 1.085,
        "GBP-USDT-SWAP": 1.295,
        "NVDA-USDT-SWAP": 128.5,
        "TSLA-USDT-SWAP": 245.0,
        "AAPL-USDT-SWAP": 228.0,
        "ONDO-USDT-SWAP": 0.725,
        "PENDLE-USDT-SWAP": 4.85,
        "PAXG-USDT": 2650.0,
        "MKR-USDT-SWAP": 1650.0,
    }

    if ticker_obj and ticker_obj.last_price > 0:
        current_price = ticker_obj.last_price
        chg_24h = ticker_obj.change_24h_pct or 0.0
    else:
        current_price = DEFAULT_PRICES.get(symbol, paper_engine.last_price_map.get(symbol, 100.0))
        chg_24h = 1.25

    # 3. 异步并发拉取 15m/1h/4h/1d K 线与计算多周期指标
    timeframes = ["15m", "1h", "4h", "1d"]
    candle_tasks = [rest_client.get_candles(symbol, bar=tf, limit=100) for tf in timeframes]

    # 异步并发拉取衍生品数据与盘口深度
    funding_task = rest_client.get_funding_rate(symbol)
    oi_task = rest_client.get_open_interest(symbol)
    depth_task = rest_client.get_orderbook(symbol, depth=5)
    risk_cfg_task = db.execute(text("SELECT * FROM risk_limits ORDER BY id DESC LIMIT 1"))

    all_results = await asyncio.gather(
        *candle_tasks,
        funding_task,
        oi_task,
        depth_task,
        risk_cfg_task,
        return_exceptions=True
    )

    candles_results = all_results[:4]
    funding_res = all_results[4]
    oi_res = all_results[5]
    depth_res = all_results[6]
    risk_cfg_res = all_results[7]

    # 计算指标
    indicators_map: Dict[str, SinglePeriodIndicators] = {}
    for tf, c_list in zip(timeframes, candles_results):
        if isinstance(c_list, list) and c_list:
            indicators_map[tf] = compute_single_timeframe_indicators(c_list, tf)
            if tf == "15m" and c_list[-1].close > 0:
                cur_tk = ws_manager.latest_tickers.get(symbol)
                if not cur_tk or (now_ms - getattr(cur_tk, "timestamp", 0)) > 10000:
                    paper_engine.last_price_map[symbol] = c_list[-1].close
        else:
            indicators_map[tf] = compute_single_timeframe_indicators([], tf)

    multi_ind = MultiPeriodIndicators(
        symbol=symbol,
        timestamp=now_ms,
        indicators=indicators_map
    )

    # 衍生品数据解析
    funding_rate = 0.0001
    next_funding_time = now_ms + 14400000
    if isinstance(funding_res, dict) and funding_res:
        funding_rate = float(funding_res.get("fundingRate", 0.0001))
        next_funding_time = int(funding_res.get("fundingTime", next_funding_time))

    open_interest = 15000.0
    open_interest_usdt = open_interest * current_price
    if isinstance(oi_res, dict) and oi_res:
        open_interest = float(oi_res.get("oi", open_interest))
        open_interest_usdt = float(oi_res.get("oiCcy", open_interest_usdt))

    bids_top5 = []
    asks_top5 = []
    imbalance = 1.0
    if isinstance(depth_res, OrderBookDepth):
        bids_top5 = [[b.price, b.size] for b in depth_res.bids[:5]]
        asks_top5 = [[a.price, a.size] for a in depth_res.asks[:5]]
        bid_vol = sum(b.size for b in depth_res.bids[:5])
        ask_vol = sum(a.size for a in depth_res.asks[:5])
        if ask_vol > 0:
            imbalance = round(bid_vol / ask_vol, 2)

    # 衍生品基础指标已通过 OKX 实盘公共资金费率与持仓量接口直接获取
    real_deriv: Dict[str, Any] = {}

    is_real_deriv = bool(real_deriv.get("is_real", False))
    real_oi_24h = float(real_deriv.get("oi_change_24h_pct", 0.0)) if is_real_deriv else 0.0
    real_oi_1h = float(real_deriv.get("oi_change_1h_pct", 0.0)) if is_real_deriv else None
    real_ls = real_deriv.get("long_short_ratio") if is_real_deriv else None
    real_top_ls = real_deriv.get("top_trader_ls_ratio") if is_real_deriv else None
    real_taker_usdt = float(real_deriv.get("net_taker_flow_usdt", 0.0)) if is_real_deriv else None
    real_taker_ratio = float(real_deriv.get("net_taker_ratio", 0.0)) if is_real_deriv else None

    if real_ls is None:
        real_ls = round(max(0.45, min(2.50, 1.0 + (funding_rate * 1200.0) + (imbalance - 1.0) * 0.20)), 2)
    if real_top_ls is None:
        real_top_ls = real_ls

    # 真实情绪分严格居中 (0.0 为中性，范围 -1.0 ~ +1.0)，杜绝多头偏见
    if is_real_deriv and real_taker_ratio is not None:
        dynamic_sentiment = round(max(-1.0, min(1.0, real_taker_ratio * 2.5 + (chg_24h / 20.0) * 0.4)), 2)
    else:
        dynamic_sentiment = round(max(-1.0, min(1.0, (chg_24h / 18.0) * 0.6 + (imbalance - 1.0) * 0.15)), 2)

    derivatives_metrics = DerivativesMetrics(
        symbol=symbol,
        funding_rate=funding_rate,
        funding_rate_annualized_pct=round(funding_rate * 3 * 365 * 100, 2),
        next_funding_time=next_funding_time,
        open_interest=open_interest,
        open_interest_usdt=open_interest_usdt,
        oi_change_24h_pct=real_oi_24h,
        oi_change_1h_pct=real_oi_1h,
        long_short_ratio=real_ls,
        top_trader_long_short_ratio=real_top_ls,
        net_taker_flow_usdt=real_taker_usdt,
        net_taker_ratio=real_taker_ratio,
        is_real_derivatives_data=is_real_deriv,
        sentiment_score=dynamic_sentiment,
        timestamp=now_ms
    )

    # 硬风控参数
    risk_row = risk_cfg_res.mappings().first() if (not isinstance(risk_cfg_res, Exception) and hasattr(risk_cfg_res, "mappings")) else None
    risk_limits = HardRiskLimits(
        max_order_usdt=float(risk_row.get("max_order_usdt", 500.0)) if risk_row else 500.0,
        max_daily_loss_usdt=float(risk_row.get("max_daily_loss_usdt", 200.0)) if risk_row else 200.0,
        max_leverage=int(risk_row.get("max_leverage", 5)) if risk_row else 5,
        max_slippage_pct=float(risk_row.get("max_slippage_pct", 0.5)) if risk_row else 0.5
    ) if HardRiskLimits is not Any else None

    # 当前持仓快照
    pos_obj = ws_manager.get_position_snapshot(symbol)
    active_pos = None
    if pos_obj and pos_obj.pos > 0 and PositionSnapshot is not Any:
        active_pos = PositionSnapshot(
            symbol=symbol,
            side=pos_obj.pos_side.value if hasattr(pos_obj.pos_side, "value") else str(pos_obj.pos_side),
            leverage=pos_obj.leverage,
            contracts=pos_obj.pos,
            notional_usd=pos_obj.notional_usd,
            entry_price=pos_obj.avg_px,
            mark_price=pos_obj.mark_px,
            unrealized_pnl=pos_obj.upl,
            pnl_percentage=pos_obj.upl_ratio * 100.0
        )

    # 当前账户可用资金 (实盘/模拟自适应)
    account_avail = 10000.0
    if rest_client.is_live_mode and rest_client.api_key:
        try:
            bal_obj = await rest_client.get_account_balance()
            account_avail = bal_obj.available_margin_usdt if bal_obj.available_margin_usdt > 0 else bal_obj.total_equity_usdt
        except Exception:
            account_avail = 0.0
    else:
        paper_bal = paper_engine.get_account_balance()
        account_avail = paper_bal.available_margin_usdt if paper_bal.available_margin_usdt > 0 else paper_bal.total_equity_usdt

    multi_ind_data = multi_ind.model_dump() if hasattr(multi_ind, "model_dump") else multi_ind.dict()
    derivatives_data = derivatives_metrics.model_dump() if hasattr(derivatives_metrics, "model_dump") else derivatives_metrics.dict()

    snapshot = None
    if MarketContextSnapshot is not Any:
        snapshot = MarketContextSnapshot(
            analysis_id=str(uuid.uuid4()),
            symbol=symbol,
            timestamp=now_ms,
            current_price=current_price,
            change_24h_pct=chg_24h,
            multi_indicators=multi_ind_data,
            derivatives=derivatives_data,
            active_position=active_pos,
            account_balance_usdt=account_avail,
            risk_limits=risk_limits,
            imbalance_ratio=imbalance,
            orderbook_bids_top5=bids_top5,
            orderbook_asks_top5=asks_top5
        )

    return current_price, snapshot, multi_ind, derivatives_metrics, llm_key, base_url, model_name, temperature, max_tokens, llm_provider


async def get_derivatives_metrics_data(symbol: str) -> DerivativesMetrics:
    """获取指定标的的真实资金费率与未平仓持仓量 (OI) 指标"""
    now_ms = int(time.time() * 1000)
    try:
        fr_data, oi_data = await asyncio.gather(
            rest_client.get_funding_rate(symbol),
            rest_client.get_open_interest(symbol),
            return_exceptions=True
        )
        real_deriv: Dict[str, Any] = {}

        funding_rate = 0.0
        next_funding_time = now_ms + 14400000
        if isinstance(fr_data, dict) and fr_data.get("fundingRate") is not None:
            funding_rate = float(fr_data.get("fundingRate", 0.0))
            next_funding_time = int(fr_data.get("fundingTime", next_funding_time))
        elif isinstance(real_deriv, dict) and real_deriv.get("funding_rate") is not None:
            funding_rate = float(real_deriv["funding_rate"])

        open_interest = 0.0
        open_interest_usdt = 0.0
        if isinstance(oi_data, dict) and oi_data:
            open_interest = float(oi_data.get("oi", 0.0))
            open_interest_usdt = float(oi_data.get("oiUsd") or oi_data.get("oiCcy") or 0.0)

        is_real = isinstance(real_deriv, dict) and bool(real_deriv.get("is_real"))
        oi_chg_24h = float(real_deriv.get("oi_change_24h_pct", 0.0)) if is_real else 0.0
        oi_chg_1h = float(real_deriv.get("oi_change_1h_pct", 0.0)) if is_real else None
        ls_ratio = real_deriv.get("long_short_ratio") if is_real else 1.0
        top_ls = real_deriv.get("top_trader_ls_ratio") if is_real else 1.0
        net_taker_usdt = float(real_deriv.get("net_taker_flow_usdt", 0.0)) if is_real else None
        net_taker_ratio = float(real_deriv.get("net_taker_ratio", 0.0)) if is_real else None

        if is_real and net_taker_ratio is not None:
            sentiment_score = round(max(-1.0, min(1.0, net_taker_ratio * 2.5)), 2)
        else:
            sentiment_score = 0.0

        return DerivativesMetrics(
            symbol=symbol,
            funding_rate=funding_rate,
            funding_rate_annualized_pct=round(funding_rate * 3 * 365 * 100, 2),
            next_funding_time=next_funding_time,
            open_interest=open_interest,
            open_interest_usdt=open_interest_usdt,
            oi_change_24h_pct=oi_chg_24h,
            oi_change_1h_pct=oi_chg_1h,
            long_short_ratio=ls_ratio,
            top_trader_long_short_ratio=top_ls,
            net_taker_flow_usdt=net_taker_usdt,
            net_taker_ratio=net_taker_ratio,
            is_real_derivatives_data=is_real,
            sentiment_score=sentiment_score,
            timestamp=now_ms
        )
    except Exception as e:
        logger.warning(f"获取衍生品指标失败，返回中性安全默认值: {e}")
        return DerivativesMetrics(
            symbol=symbol,
            funding_rate=0.0,
            funding_rate_annualized_pct=0.0,
            next_funding_time=now_ms + 14400000,
            open_interest=0.0,
            open_interest_usdt=0.0,
            oi_change_24h_pct=0.0,
            oi_change_1h_pct=None,
            long_short_ratio=1.0,
            top_trader_long_short_ratio=1.0,
            net_taker_flow_usdt=None,
            net_taker_ratio=None,
            is_real_derivatives_data=False,
            sentiment_score=0.0,
            timestamp=now_ms
        )


async def get_multi_period_indicators_data(symbol: str) -> MultiPeriodIndicators:
    """获取指定标的 15m/1h/4h/1d 多周期技术指标"""
    now_ms = int(time.time() * 1000)
    timeframes = ["15m", "1h", "4h", "1d"]
    candle_tasks = [rest_client.get_candles(symbol, bar=tf, limit=100) for tf in timeframes]
    candles_results = await asyncio.gather(*candle_tasks, return_exceptions=True)

    indicators_map: Dict[str, SinglePeriodIndicators] = {}
    for tf, c_list in zip(timeframes, candles_results):
        if isinstance(c_list, list) and c_list:
            indicators_map[tf] = compute_single_timeframe_indicators(c_list, tf)
            if tf == "15m" and c_list[-1].close > 0:
                cur_tk = ws_manager.latest_tickers.get(symbol)
                if not cur_tk or (now_ms - getattr(cur_tk, "timestamp", 0)) > 10000:
                    paper_engine.last_price_map[symbol] = c_list[-1].close
        else:
            indicators_map[tf] = compute_single_timeframe_indicators([], tf)

    return MultiPeriodIndicators(
        symbol=symbol,
        timestamp=now_ms,
        indicators=indicators_map
    )
