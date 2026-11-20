"""
OKX-Dog 个人量化智能交易系统 - OKX v5 REST API 客户端模块
模块: okx-dog-backend/okx_rest.py
实现 OKX v5 严格签名 (HMAC-SHA256)、令牌桶平滑限流、429 自动指数退避重试、全量交易与行情接口封装
"""

import asyncio
import base64
import datetime
import hashlib
import hmac
import json
import logging
import secrets
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import httpx

from config import settings
from models import (
    InstType,
    OrderSide,
    PosSide,
    OrderType,
    OrderState,
    TickerData,
    KlineCandle,
    OrderBookDepth,
    OrderBookLevel,
    PositionModel,
    AccountBalanceModel,
    OrderModel
)

logger = logging.getLogger("okx_dog.rest")


class OKXApiException(Exception):
    """OKX 官方 API 业务异常"""
    def __init__(self, code: str, msg: str, data: Optional[Any] = None):
        self.code = code
        self.msg = msg
        self.data = data
        super().__init__(f"OKX API 异常 [code={code}]: {msg}")


class OKXRateLimitException(OKXApiException):
    """OKX 429 限频异常"""
    pass


class TokenBucketRateLimiter:
    """
    令牌桶平滑限流器 (Token Bucket Rate Limiter)
    针对 OKX 不同 API 端点组进行高精度频控保护，杜绝 429 拦截
    """

    def __init__(self, rate_limits: Optional[Dict[str, Tuple[float, float]]] = None):
        """
        :param rate_limits: 字典，格式为 {分类名: (每秒补充令牌数 rate, 桶容量 capacity)}
        """
        # 默认 OKX 频控规则：
        # public: 20 次/2s -> 10/s, cap=20
        # trade: 60 次/2s -> 30/s, cap=60
        # account: 20 次/2s -> 10/s, cap=20
        # default: 10 次/s, cap=20
        self._limits: Dict[str, Tuple[float, float]] = rate_limits or {
            "public": (10.0, 20.0),
            "trade": (30.0, 60.0),
            "account": (10.0, 20.0),
            "default": (10.0, 20.0)
        }
        self._tokens: Dict[str, float] = {}
        self._last_refill: Dict[str, float] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

        now = time.monotonic()
        for cat, (rate, cap) in self._limits.items():
            self._tokens[cat] = cap
            self._last_refill[cat] = now
            self._locks[cat] = asyncio.Lock()

    async def acquire(self, category: str = "default", tokens: float = 1.0):
        """异步获取令牌，若不足则自适应休眠平滑放行"""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if getattr(self, "_limiter_loop", None) != loop:
            self._locks.clear()
            self._limiter_loop = loop

        if category not in self._limits:
            category = "default"

        if category not in self._locks:
            self._locks[category] = asyncio.Lock()
            self._tokens[category] = self._limits[category][1]
            self._last_refill[category] = time.monotonic()

        rate, capacity = self._limits[category]

        async with self._locks[category]:
            now = time.monotonic()
            elapsed = now - self._last_refill[category]
            self._last_refill[category] = now

            # 补充令牌
            self._tokens[category] = min(capacity, self._tokens[category] + elapsed * rate)

            if self._tokens[category] < tokens:
                # 令牌不足，计算需要等待的时间
                deficit = tokens - self._tokens[category]
                wait_time = deficit / rate
                logger.debug(f"[RateLimiter] 端点 [{category}] 令牌不足，平滑等待 {wait_time:.3f} 秒")
                await asyncio.sleep(wait_time)
                self._tokens[category] = 0.0
                self._last_refill[category] = time.monotonic()
            else:
                self._tokens[category] -= tokens


class OKXRestClient:
    """
    OKX v5 REST 异步客户端
    - 严格 HMAC-SHA256 签名与时间戳格式化
    - 支持实盘与模拟盘模式自动路由
    - 集成令牌桶平滑限频器与 429 自动指数退避重试
    - 封装全量行情、资产、持仓与交易执行接口
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        passphrase: Optional[str] = None,
        is_live_mode: bool = False,
        base_url: Optional[str] = None,
        timeout: float = 10.0
    ):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.is_live_mode = is_live_mode

        if base_url:
            self.base_url = base_url.rstrip("/")
        else:
            self.base_url = settings.OKX_LIVE_REST_URL.rstrip("/") if is_live_mode else settings.OKX_DEMO_REST_URL.rstrip("/")

        self.timeout = timeout
        self.rate_limiter = TokenBucketRateLimiter()
        self._client: Optional[httpx.AsyncClient] = None

    def update_credentials(self, api_key: str, secret_key: str, passphrase: str, is_live_mode: Optional[bool] = None):
        """热更新凭证信息"""
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        if is_live_mode is not None:
            self.is_live_mode = is_live_mode
            self.base_url = settings.OKX_LIVE_REST_URL.rstrip("/") if is_live_mode else settings.OKX_DEMO_REST_URL.rstrip("/")
        self._account_pos_mode = None

    async def _get_client(self) -> httpx.AsyncClient:
        """获取或创建单例 HTTPX 异步客户端 (自适应当前 Event Loop)"""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if (
            self._client is None
            or self._client.is_closed
            or getattr(self, "_client_loop", None) != loop
        ):
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout, connect=5.0),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
                headers={"Accept": "application/json"}
            )
            self._client_loop = loop
        return self._client

    async def close(self):
        """关闭 HTTP 连接池"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    def _generate_signature(
        self,
        timestamp: str,
        method: str,
        request_path: str,
        body_str: str = ""
    ) -> str:
        """
        计算 OKX v5 HMAC-SHA256 签名
        待签名字符串: timestamp + method.upper() + requestPath + (body if body else "")
        """
        if not self.secret_key:
            return ""

        prehash = f"{timestamp}{method.upper()}{request_path}{body_str}"
        mac = hmac.new(
            self.secret_key.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256
        )
        return base64.b64encode(mac.digest()).decode("utf-8")

    def _get_headers(
        self,
        method: str,
        request_path: str,
        body_str: str = ""
    ) -> Dict[str, str]:
        """组装完整的 OKX v5 请求头"""
        headers = {
            "Content-Type": "application/json"
        }

        # Stage 0: 当开启实盘公共行情开关时，公共行情/市场统计接口统一访问 OKX 实盘公共端点
        # 不附带 x-simulated-trading 也不附带私有密钥，彻底消除模拟盘脏 tick 与过期 API Key 导致的公共行情报错
        is_public_endpoint = self._categorize_endpoint(request_path) == "public"
        if is_public_endpoint and settings.USE_LIVE_PUBLIC_MARKET_DATA:
            return headers

        # 模拟盘私有与下单请求必须包含 x-simulated-trading 标头
        if not self.is_live_mode:
            headers["x-simulated-trading"] = "1"

        if self.api_key and self.secret_key and self.passphrase:
            # OKX 规范 ISO8601 UTC 毫秒时间戳: 2026-08-15T01:48:21.123Z
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            timestamp = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

            sign = self._generate_signature(timestamp, method, request_path, body_str)

            headers.update({
                "OK-ACCESS-KEY": self.api_key,
                "OK-ACCESS-SIGN": sign,
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": self.passphrase,
            })

        return headers

    def _categorize_endpoint(self, path: str) -> str:
        """根据请求路径分类以进行针对性限流"""
        if "/api/v5/market/" in path or "/api/v5/public/" in path or "/api/v5/rubik/" in path:
            return "public"
        elif "/api/v5/trade/" in path:
            return "trade"
        elif "/api/v5/account/" in path:
            return "account"
        return "default"

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        max_retries: int = 3
    ) -> List[Dict[str, Any]]:
        """
        发送底层 HTTP 请求并进行限流、签名校验与重试处理
        """
        category = self._categorize_endpoint(path)
        client = await self._get_client()

        # 处理 URL query 拼接
        query_str = ""
        if params:
            clean_params = {k: v for k, v in params.items() if v is not None}
            if clean_params:
                # 编码查询字符串
                query_str = "?" + "&".join([f"{k}={v}" for k, v in clean_params.items()])

        full_request_path = path + query_str
        body_str = json.dumps(json_data) if json_data else ""

        attempt = 0
        last_exception: Optional[Exception] = None

        while attempt < max_retries:
            attempt += 1
            # 令牌桶平滑限频
            await self.rate_limiter.acquire(category)

            # 动态生成当前瞬时时间戳与签名 Headers
            headers = self._get_headers(method, full_request_path, body_str)

            try:
                if method.upper() == "GET":
                    response = await client.get(full_request_path, headers=headers)
                elif method.upper() == "POST":
                    response = await client.post(
                        full_request_path,
                        headers=headers,
                        content=body_str if body_str else None
                    )
                elif method.upper() == "DELETE":
                    response = await client.delete(full_request_path, headers=headers)
                else:
                    raise ValueError(f"不支持的 HTTP 请求方法: {method}")

                # 429 拦截自愈
                if response.status_code == 429:
                    backoff = (2 ** attempt) * 0.5
                    logger.warning(f"触发 OKX 429 Too Many Requests, 将在 {backoff:.1f} 秒后重试 (第 {attempt} 次)...")
                    await asyncio.sleep(backoff)
                    continue

                if response.status_code >= 500:
                    logger.warning(f"OKX 服务器网关错误 [{response.status_code}], 正在重试...")
                    await asyncio.sleep(1.0 * attempt)
                    continue

                res_json = response.json()
                code = str(res_json.get("code", ""))
                msg = res_json.get("msg", "")

                if code == "0":
                    return res_json.get("data", [])
                elif code == "50004" or "Too many requests" in msg:
                    # OKX 业务级 429 报错
                    backoff = (2 ** attempt) * 0.5
                    logger.warning(f"触发 OKX 业务级限流 (code={code}, msg={msg}), 等待 {backoff}s 重试")
                    await asyncio.sleep(backoff)
                    continue
                else:
                    logger.error(f"OKX API 返回业务失败: code={code}, msg={msg}, path={full_request_path}")
                    raise OKXApiException(code=code, msg=msg, data=res_json.get("data"))

            except (httpx.RequestError, httpx.TimeoutException) as net_err:
                last_exception = net_err
                logger.warning(f"OKX 网络请求异常 [{type(net_err).__name__}]: {net_err}, 正在重试 (第 {attempt}/{max_retries} 次)...")
                await asyncio.sleep(0.5 * attempt)
            except OKXApiException:
                raise
            except Exception as e:
                last_exception = e
                logger.error(f"执行 REST 请求发生非预期异常: {e}", exc_info=True)
                raise

        raise OKXApiException(
            code="500",
            msg=f"请求 OKX 达到最大重试次数失败: {last_exception}"
        )

    # =========================================================================
    # 1. 行情与公共市场接口 (Market Data APIs)
    # =========================================================================

    async def get_ticker(self, symbol: str) -> TickerData:
        """
        获取单一标的最新 Ticker 行情
        GET /api/v5/market/ticker
        """
        data = await self._request("GET", "/api/v5/market/ticker", params={"instId": symbol})
        if not data:
            raise OKXApiException("404", f"未查询到标的 {symbol} 的 Ticker 行情")

        item = data[0]
        now_ms = int(time.time() * 1000)
        last_px = float(item.get("last") or 0.0)
        bid_px = float(item.get("bidPx") or 0.0) if item.get("bidPx") else last_px
        ask_px = float(item.get("askPx") or 0.0) if item.get("askPx") else last_px
        open_24h = float(item.get("open24h") or last_px)
        chg_pct = ((last_px - open_24h) / open_24h * 100.0) if open_24h > 0 else 0.0
        return TickerData(
            symbol=item.get("instId", symbol),
            last_price=last_px,
            last_sz=float(item.get("lastSz") or 0.0),
            bid_price=bid_px if bid_px > 0 else None,
            ask_price=ask_px if ask_px > 0 else None,
            high_24h=float(item.get("high24h") or last_px),
            low_24h=float(item.get("low24h") or last_px),
            open_24h=open_24h,
            change_24h_pct=round(chg_pct, 2),
            volume_24h=float(item.get("vol24h") or 0.0),
            vol_currency_24h=float(item.get("volCcy24h") or 0.0),
            timestamp=int(item.get("ts") or now_ms)
        )

    async def get_candles(
        self,
        symbol: str,
        bar: str = "1H",
        limit: int = 100,
        after: Optional[str] = None,
        before: Optional[str] = None
    ) -> List[KlineCandle]:
        """
        获取 K 线数据
        GET /api/v5/market/candles
        :param bar: 时间周期 (如 1m, 5m, 15m, 1H, 4H, 1D)
        :param limit: 获取条数 (最大 300)
        """
        # OKX 周期大小写适配
        bar_norm = bar
        if bar.endswith("h") or bar.endswith("d"):
            bar_norm = bar.upper()

        params = {
            "instId": symbol,
            "bar": bar_norm,
            "limit": str(min(limit, 300)),
            "after": after,
            "before": before
        }
        raw_list = await self._request("GET", "/api/v5/market/candles", params=params)
        candles: List[KlineCandle] = []
        for k in raw_list:
            # k format: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
            candles.append(KlineCandle(
                timestamp=int(k[0]),
                open=float(k[1]),
                high=float(k[2]),
                low=float(k[3]),
                close=float(k[4]),
                volume=float(k[5]),
                vol_currency=float(k[6]) if len(k) > 6 else None,
                is_closed=bool(int(k[8])) if len(k) > 8 else True
            ))
        # 按时间正序排列 (从过去到最新)
        candles.sort(key=lambda x: x.timestamp)
        return candles

    async def get_kline_history(
        self,
        symbol: str,
        bar: str = "4H",
        limit: int = 100,
        after: Optional[str] = None,
        before: Optional[str] = None
    ) -> List[KlineCandle]:
        """
        获取深历史 K 线数据 (支持跨多年全量回溯)
        GET /api/v5/market/history-candles
        :param bar: 周期 (15m, 1H, 4H, 1Dutc 等)
        :param limit: 单页条数 (OKX history-candles 最大 100)
        :param after: 请求早于该毫秒时间戳的更旧数据
        :param before: 请求晚于该毫秒时间戳的更新数据
        :return: 按时间戳升序排列的 KlineCandle 列表 (candles[0] 为本页最旧一根)
        """
        bar_map = {
            "1h": "1H",
            "4h": "4H",
            "1d": "1Dutc",
            "1D": "1Dutc",
        }
        bar_norm = bar_map.get(bar, bar)
        params: Dict[str, Any] = {
            "instId": symbol,
            "bar": bar_norm,
            "limit": str(min(max(1, limit), 100)),
        }
        if after:
            params["after"] = str(after)
        if before:
            params["before"] = str(before)

        raw_list = await self._request("GET", "/api/v5/market/history-candles", params=params)
        candles: List[KlineCandle] = []
        for k in raw_list:
            candles.append(KlineCandle(
                timestamp=int(k[0]),
                open=float(k[1]),
                high=float(k[2]),
                low=float(k[3]),
                close=float(k[4]),
                volume=float(k[5]),
                vol_currency=float(k[6]) if len(k) > 6 else None,
                is_closed=bool(int(k[8])) if len(k) > 8 else True
            ))
        candles.sort(key=lambda x: x.timestamp)
        return candles

    async def get_orderbook(self, symbol: str, depth: int = 20) -> OrderBookDepth:
        """
        获取订单簿深度盘口
        GET /api/v5/market/books
        """
        params = {"instId": symbol, "sz": str(depth)}
        data = await self._request("GET", "/api/v5/market/books", params=params)
        if not data:
            return OrderBookDepth(
                symbol=symbol,
                timestamp=int(time.time() * 1000),
                bids=[],
                asks=[]
            )

        item = data[0]
        now_ms = int(time.time() * 1000)
        bids = [
            OrderBookLevel(price=float(b[0]), size=float(b[1]), order_count=int(b[3]) if len(b) > 3 else 1)
            for b in item.get("bids", [])
        ]
        asks = [
            OrderBookLevel(price=float(a[0]), size=float(a[1]), order_count=int(a[3]) if len(a) > 3 else 1)
            for a in item.get("asks", [])
        ]
        return OrderBookDepth(
            symbol=symbol,
            timestamp=int(item.get("ts", now_ms)),
            bids=bids,
            asks=asks
        )

    async def get_instruments(self, inst_type: str = "SWAP", symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取交易产品列表 GET /api/v5/public/instruments"""
        params = {"instType": inst_type}
        if symbol:
            params["instId"] = symbol
        return await self._request("GET", "/api/v5/public/instruments", params=params)

    async def get_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """获取当前资金费率 GET /api/v5/public/funding-rate"""
        data = await self._request("GET", "/api/v5/public/funding-rate", params={"instId": symbol})
        return data[0] if data else {}

    async def get_funding_rate_history(
        self,
        symbol: str,
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """获取历史资金费率 GET /api/v5/public/funding-rate-history"""
        params: Dict[str, Any] = {"instId": symbol, "limit": str(limit)}
        if before:
            params["before"] = before
        if after:
            params["after"] = after
        data = await self._request("GET", "/api/v5/public/funding-rate-history", params=params)
        return data or []

    async def get_funding_rate_history_range(
        self,
        symbol: str,
        start_ts_ms: int,
        max_pages: int = 45
    ) -> Dict[int, float]:
        """
        分页拉取指定起始时间以来的全量真实 8 小时历史资金费率
        :return: {funding_time_ms: funding_rate} 字典
        """
        funding_map: Dict[int, float] = {}
        cursor_after: Optional[str] = None

        for _ in range(max_pages):
            batch = await self.get_funding_rate_history(symbol=symbol, limit=100, after=cursor_after)
            if not batch:
                break

            oldest_ts: Optional[int] = None
            for item in batch:
                f_ts = int(item.get("fundingTime") or 0)
                f_rate = float(item.get("realizedRate") or item.get("fundingRate") or 0.0)
                if f_ts > 0:
                    funding_map[f_ts] = f_rate
                    if oldest_ts is None or f_ts < oldest_ts:
                        oldest_ts = f_ts

            if oldest_ts is None or oldest_ts <= start_ts_ms or len(batch) < 100:
                break

            cursor_after = str(oldest_ts)
            await asyncio.sleep(0.06)

        return funding_map

    async def get_open_interest(self, symbol: str, inst_type: str = "SWAP") -> Dict[str, Any]:
        """获取合约全网持仓量 GET /api/v5/public/open-interest"""
        params = {"instType": inst_type, "instId": symbol}
        data = await self._request("GET", "/api/v5/public/open-interest", params=params)
        return data[0] if data else {}

    async def get_all_tickers(self, inst_type: str = "SWAP") -> List[Dict[str, Any]]:
        """
        获取全市场指定产品类型的所有最新 Ticker 快照（单次轻量请求覆盖全网 150+ 永续合约）
        GET /api/v5/market/tickers
        """
        data = await self._request("GET", "/api/v5/market/tickers", params={"instType": inst_type})
        return data or []

    async def get_all_open_interest(self, inst_type: str = "SWAP") -> List[Dict[str, Any]]:
        """
        批量获取全市场合约当前全网持仓量 (OI)
        GET /api/v5/public/open-interest
        """
        data = await self._request("GET", "/api/v5/public/open-interest", params={"instType": inst_type})
        return data or []

    @staticmethod
    def _extract_base_ccy(symbol: str) -> str:
        """从标的代码提取基础币种，如 TAO-USDT-SWAP -> TAO"""
        return symbol.strip().upper().split("-")[0]

    async def get_open_interest_volume(
        self,
        symbol_or_ccy: str,
        period: str = "1H"
    ) -> List[List[str]]:
        """
        获取 OKX 真实合约持仓量与交易量历史序列（用于计算真实 OI Delta）
        GET /api/v5/rubik/stat/contracts/open-interest-volume
        返回元素格式: [ts, oi_usd, vol_usd] (按时间倒序)
        """
        ccy = self._extract_base_ccy(symbol_or_ccy)
        data = await self._request(
            "GET",
            "/api/v5/rubik/stat/contracts/open-interest-volume",
            params={"ccy": ccy, "period": period}
        )
        return data or []

    async def get_taker_volume(
        self,
        symbol_or_ccy: str,
        inst_type: str = "CONTRACTS",
        period: str = "1H"
    ) -> List[List[str]]:
        """
        获取 OKX 真实主动买入/卖出成交量（Taker Buy vs Taker Sell / CVD 核心数据源）
        GET /api/v5/rubik/stat/taker-volume
        返回元素格式: [ts, sellVol, buyVol] (按时间倒序)
        """
        ccy = self._extract_base_ccy(symbol_or_ccy)
        data = await self._request(
            "GET",
            "/api/v5/rubik/stat/taker-volume",
            params={"ccy": ccy, "instType": inst_type, "period": period}
        )
        return data or []

    async def get_long_short_account_ratio(
        self,
        symbol_or_ccy: str,
        period: str = "1H"
    ) -> List[List[str]]:
        """
        获取 OKX 真实合约多空持仓人数比
        GET /api/v5/rubik/stat/contracts/long-short-account-ratio
        返回元素格式: [ts, ratio] (按时间倒序)
        """
        ccy = self._extract_base_ccy(symbol_or_ccy)
        data = await self._request(
            "GET",
            "/api/v5/rubik/stat/contracts/long-short-account-ratio",
            params={"ccy": ccy, "period": period}
        )
        return data or []

    async def get_top_trader_long_short_ratio(
        self,
        symbol: str,
        period: str = "1H"
    ) -> List[List[str]]:
        """
        获取 OKX 真实精英大户合约多空持仓比
        GET /api/v5/rubik/stat/contracts/long-short-account-ratio-contract-top-trader
        返回元素格式: [ts, ratio] (按时间倒序)
        """
        data = await self._request(
            "GET",
            "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract-top-trader",
            params={"instId": symbol, "period": period}
        )
        return data or []

    # =========================================================================
    # 2. 账户与资产接口 (Account & Balance APIs)
    # =========================================================================

    async def get_account_balance(self, ccy: Optional[str] = None) -> AccountBalanceModel:
        """
        获取账户总资产全景与保证金数据
        GET /api/v5/account/balance
        """
        params = {"ccy": ccy} if ccy else None
        data = await self._request("GET", "/api/v5/account/balance", params=params)

        now_ms = int(time.time() * 1000)
        if not data:
            return AccountBalanceModel(
                total_equity_usdt=0.0,
                total_equity_btc=0.0,
                available_margin_usdt=0.0,
                used_margin_usdt=0.0,
                frozen_balance_usdt=0.0,
                unrealized_pnl_usdt=0.0,
                today_realized_pnl_usdt=0.0,
                today_pnl_ratio=0.0,
                margin_ratio=100.0,
                is_warning_margin=False,
                timestamp=now_ms
            )

        item = data[0]
        total_eq = float(item.get("totalEq") or 0.0)
        adj_eq = float(item.get("adjEq") or 0.0)
        mgn_ratio_str = item.get("mgnRatio", "")
        mgn_ratio = float(mgn_ratio_str) if mgn_ratio_str else 999.0

        details = item.get("details", [])
        avail_margin = float(item.get("availEq") or item.get("availMgn") or 0.0)
        used_margin = float(item.get("imr") or 0.0)
        frozen_bal = float(item.get("ordFzn") or 0.0)
        upl = float(item.get("upl") or 0.0)

        # 从各币种资产细节汇总 (兼容统一账户单币种/跨币种/组合保证金模式及空字符串回退)
        if details:
            details_eq_sum = 0.0
            details_avail_sum = 0.0
            details_upl_sum = 0.0
            details_frozen_sum = 0.0
            for d in details:
                eq_usd = float(d.get("eqUsd") or 0.0)
                eq_native = float(d.get("eq") or d.get("cashBal") or 0.0)
                details_eq_sum += eq_usd if eq_usd > 0 else (eq_native if d.get("ccy") == "USDT" else 0.0)
                details_upl_sum += float(d.get("upl") or 0.0)
                details_frozen_sum += float(d.get("ordFrozen") or d.get("frozenBal") or 0.0)
                if d.get("ccy") in ("USDT", "USDC", "USD"):
                    avail_val = float(
                        d.get("availEq")
                        or d.get("availBal")
                        or d.get("availMgn")
                        or d.get("cashBal")
                        or d.get("eq")
                        or 0.0
                    )
                    details_avail_sum += avail_val

            if total_eq <= 0.0 and details_eq_sum > 0.0:
                total_eq = details_eq_sum
            if avail_margin <= 0.0 and details_avail_sum > 0.0:
                avail_margin = details_avail_sum
            if upl == 0.0 and details_upl_sum != 0.0:
                upl = details_upl_sum
            if frozen_bal <= 0.0 and details_frozen_sum > 0.0:
                frozen_bal = details_frozen_sum

        # 终极兜底：若统一账户跨币种模式仅返回了 adjEq 或 totalEq，按权益扣除已用与冻结保证金推算可用保证金
        if avail_margin <= 0.0:
            base_eq = adj_eq if adj_eq > 0 else total_eq
            if base_eq > 0:
                avail_margin = max(0.0, base_eq - used_margin - frozen_bal)

        # 计算今日已实现盈亏与收益率 (从 OKX bills 账单流水中汇总当日平仓及资金费盈亏)
        today_realized_pnl = 0.0
        try:
            today_start_dt = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            today_start_ms = int(today_start_dt.timestamp() * 1000)
            bills_data = await self._request("GET", "/api/v5/account/bills", params={"begin": str(today_start_ms), "limit": "100"})
            if bills_data:
                for b in bills_data:
                    today_realized_pnl += float(b.get("pnl") or 0.0)
        except Exception as bill_err:
            logger.debug(f"拉取今日账单流水盈亏异常 (非阻塞): {bill_err}")

        today_combined_pnl = today_realized_pnl + upl
        today_pnl_ratio = today_combined_pnl / max(100.0, total_eq) if total_eq > 0 else 0.0

        return AccountBalanceModel(
            total_equity_usdt=round(total_eq, 4),
            total_equity_btc=0.0,
            available_margin_usdt=round(avail_margin, 4),
            used_margin_usdt=round(used_margin, 4),
            frozen_balance_usdt=round(frozen_bal, 4),
            unrealized_pnl_usdt=round(upl, 4),
            today_realized_pnl_usdt=round(today_realized_pnl, 4),
            today_pnl_ratio=round(today_pnl_ratio, 4),
            margin_ratio=mgn_ratio,
            is_warning_margin=mgn_ratio < 1.5 and mgn_ratio > 0,
            timestamp=now_ms
        )

    def set_credentials(self, api_key: str, secret_key: str, passphrase: str, is_live_mode: Optional[bool] = None):
        """设置凭证信息 (update_credentials 别名)"""
        self.update_credentials(api_key, secret_key, passphrase, is_live_mode)

    def get_instrument_spec(self, symbol: str) -> Tuple[float, float, float, float]:
        """
        查询标的精确交易规格: (ct_val, tick_sz, lot_sz, min_sz)
        优先从全量缓存 InstrumentManager 获取，兜底采用内建字典
        """
        try:
            from services.instrument_service import instrument_manager
            inst = instrument_manager.get_by_symbol(symbol)
            if inst:
                return (
                    float(inst.ct_val or 1.0),
                    float(inst.tick_sz or 0.01),
                    float(inst.lot_sz or 0.01),
                    float(inst.min_sz or 0.01),
                )
        except Exception:
            pass

        ct_val_map = {
            "BTC": 0.01,
            "ETH": 0.1,
            "SOL": 1.0,
            "DOGE": 1000.0,
            "PEPE": 10000000.0,
            "XRP": 100.0,
            "SUI": 1.0,
            "BNB": 0.01,
            "XAU": 0.001,
            "PAXG": 0.001,
            "XAG": 0.1,
            "NVDA": 0.1,
            "TSLA": 0.01,
            "AAPL": 0.01,
            "EUR": 100.0,
            "GBP": 100.0,
            "ONDO": 10.0,
            "PENDLE": 1.0,
            "MKR": 0.001,
        }
        ct_val = 1.0
        if "SWAP" in symbol or "FUTURES" in symbol:
            for k, v in ct_val_map.items():
                if symbol.upper().startswith(k + "-") or k in symbol.upper():
                    ct_val = v
                    break
        tick_sz = 0.1 if "BTC" in symbol else (0.01 if any(x in symbol for x in ("ETH", "SOL", "BNB", "XAU")) else 0.0001)
        return (ct_val, tick_sz, 0.01, 0.01)

    def get_contract_value(self, symbol: str) -> float:
        """获取单张合约对应的基础币面值 (ctVal)"""
        if "SWAP" in symbol or "FUTURES" in symbol:
            ct_val, _, _, _ = self.get_instrument_spec(symbol)
            return ct_val
        return 1.0

    async def get_positions(
        self,
        inst_type: Optional[str] = None,
        symbol: Optional[str] = None,
        include_spot: bool = True
    ) -> List[PositionModel]:
        """
        获取当前持仓列表 (包括 SWAP 永续合约、MARGIN 币币杠杆、FUTURES 交割以及 SPOT 现货持有资产)
        统一将 SWAP 合约张数转换为币数 (Coins = Contracts * ctVal)，确保与模拟盘及全系统单位 100% 对齐
        """
        params = {}
        if inst_type:
            params["instType"] = inst_type
        if symbol:
            params["instId"] = symbol

        now_ms = int(time.time() * 1000)
        positions: List[PositionModel] = []

        try:
            data = await self._request("GET", "/api/v5/account/positions", params=params)
            for p in data:
                raw_pos_contracts = float(p.get("pos") or 0.0)
                if raw_pos_contracts == 0.0:
                    continue

                inst_id = p.get("instId", "")
                raw_inst = str(p.get("instType", "SWAP")).upper()
                if "MARGIN" in raw_inst:
                    inst_enum = InstType.MARGIN
                elif "SPOT" in raw_inst:
                    inst_enum = InstType.SPOT
                elif "FUTURES" in raw_inst:
                    inst_enum = InstType.FUTURES
                else:
                    inst_enum = InstType.SWAP

                raw_side = str(p.get("posSide", "net")).lower()
                if raw_side == "long":
                    side_enum = PosSide.LONG
                elif raw_side == "short":
                    side_enum = PosSide.SHORT
                else:
                    # net_mode 单向持仓模式下，正数为多头，为空头
                    side_enum = PosSide.LONG if raw_pos_contracts > 0 else PosSide.SHORT

                ct_val = self.get_contract_value(inst_id) if inst_enum in (InstType.SWAP, InstType.FUTURES) else 1.0
                pos_coins = round(abs(raw_pos_contracts) * ct_val, 6)
                raw_avail = float(p.get("availPos") or abs(raw_pos_contracts))
                avail_coins = round(abs(raw_avail) * ct_val, 6)

                pos_model = PositionModel(
                    pos_id=p.get("posId", ""),
                    symbol=inst_id,
                    inst_type=inst_enum,
                    pos_side=side_enum,
                    pos=pos_coins,
                    avail_pos=avail_coins,
                    avg_px=float(p.get("avgPx") or 0.0),
                    mark_px=float(p.get("markPx") or 0.0),
                    liq_px=float(p.get("liqPx") or 0.0) if p.get("liqPx") else None,
                    margin_ratio=float(p.get("mgnRatio") or 0.0),
                    maint_margin_ratio=float(p.get("mmr") or 0.01),
                    leverage=int(float(p.get("lever") or 1)),
                    notional_usd=float(p.get("notionalUsd") or 0.0),
                    upl=float(p.get("upl") or 0.0),
                    upl_ratio=float(p.get("uplRatio") or 0.0),
                    realized_pnl=float(p.get("realizedPnl") or 0.0),
                    margin=float(p.get("margin") or p.get("imr") or 0.0),
                    last_synced_at=int(p.get("uTime") or now_ms)
                )
                positions.append(pos_model)
        except Exception as e:
            logger.warning(f"获取 OKX 合约/杠杆持仓列表异常: {e}")

        # 若需要包含现货资产 (SPOT)
        if include_spot and not symbol:
            try:
                bal_data = await self._request("GET", "/api/v5/account/balance")
                if bal_data and len(bal_data) > 0:
                    details = bal_data[0].get("details", [])
                    for d in details:
                        ccy = str(d.get("ccy", "")).upper()
                        if ccy in ("USDT", "USDC", "USD"):
                            continue
                        pos_sz = float(d.get("eq") or 0.0)
                        eq_usd = float(d.get("eqUsd") or 0.0)
                        if pos_sz > 0 and eq_usd >= 0.5:
                            unit_px = (eq_usd / pos_sz) if pos_sz > 0 else 0.0
                            avail_sz = float(d.get("availBal") or d.get("availEq") or pos_sz)
                            spot_symbol = f"{ccy}-USDT"
                            avg_cost_px = round(unit_px, 6)
                            raw_upl = float(d.get("upl") or 0.0)

                            spot_pos = PositionModel(
                                pos_id=f"spot_{ccy.lower()}",
                                symbol=spot_symbol,
                                inst_type=InstType.SPOT,
                                pos_side=PosSide.NET,
                                pos=pos_sz,
                                pos_size=pos_sz,
                                avail_pos=avail_sz,
                                avg_px=avg_cost_px,
                                avg_price=avg_cost_px,
                                mark_px=round(unit_px, 6),
                                mark_price=round(unit_px, 6),
                                liq_px=None,
                                liquidation_price=None,
                                margin_ratio=0.0,
                                maint_margin_ratio=0.0,
                                leverage=1,
                                notional_usd=round(eq_usd, 2),
                                upl=raw_upl,
                                upl_ratio=0.0,
                                realized_pnl=0.0,
                                margin=round(eq_usd, 2),
                                last_synced_at=int(d.get("uTime") or now_ms)
                            )
                            positions.append(spot_pos)
            except Exception as spot_err:
                logger.warning(f"获取 OKX 现货资产明细转换为持仓异常: {spot_err}")

        return positions

    async def set_leverage(
        self,
        symbol: str,
        leverage: int,
        mgn_mode: str = "cross",
        pos_side: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        设置合约杠杆倍数
        POST /api/v5/account/set-leverage
        """
        body: Dict[str, Any] = {
            "instId": symbol,
            "lever": str(leverage),
            "mgnMode": mgn_mode
        }
        if pos_side:
            body["posSide"] = pos_side

        res = await self._request("POST", "/api/v5/account/set-leverage", json_data=body)
        logger.info(f"已设置标的 {symbol} 杠杆为 {leverage}x (模式: {mgn_mode})")
        return res[0] if res else {}

    async def set_position_mode(self, pos_mode: str = "long_short_mode") -> Dict[str, Any]:
        """设置持仓模式: long_short_mode (双向持仓) / net_mode (单向持仓)"""
        body = {"posMode": pos_mode}
        res = await self._request("POST", "/api/v5/account/set-position-mode", json_data=body)
        return res[0] if res else {}

    async def get_account_pos_mode(self) -> str:
        """获取当前账户持仓模式: net_mode (单向/买卖) vs long_short_mode (双向/开平)"""
        if getattr(self, "_account_pos_mode", None):
            return self._account_pos_mode
        try:
            cfg_data = await self._request("GET", "/api/v5/account/config")
            if cfg_data and isinstance(cfg_data, list):
                self._account_pos_mode = cfg_data[0].get("posMode", "long_short_mode")
                return self._account_pos_mode
        except Exception as e:
            logger.debug(f"获取持仓模式异常，默认采用 long_short_mode: {e}")
        return "long_short_mode"

    def _format_order_sz(self, symbol: str, sz: float) -> str:
        """
        将委托币数 (Coins) 自适应转换为 OKX 交易所要求的标准张数 (Contracts) 或现货数量:
        - 永续/交割 SWAP/FUTURES: Contracts = sz / ctVal，按 lotSz 步长与 minSz 下限精准对齐 (支持 0.01 张小数合约)
        - 现货 SPOT: 按 lotSz 步长对齐
        """
        from decimal import Decimal, ROUND_DOWN

        ct_val, _, lot_sz, min_sz = self.get_instrument_spec(symbol)
        if "SWAP" in symbol or "FUTURES" in symbol:
            raw_contracts = float(sz) / max(ct_val, 1e-12)
            d_contracts = Decimal(str(max(min_sz, raw_contracts)))
            d_lot = Decimal(str(max(lot_sz, 1e-8)))
            d_min = Decimal(str(max(min_sz, d_lot)))
            steps = (d_contracts / d_lot).quantize(Decimal("1"), rounding=ROUND_DOWN)
            if steps < 1:
                steps = Decimal("1")
            aligned = max(d_min, steps * d_lot)
            formatted = format(aligned.normalize(), "f")
            return formatted if formatted != "" else str(min_sz)
        else:
            d_sz = Decimal(str(max(min_sz, float(sz))))
            d_lot = Decimal(str(max(lot_sz, 1e-8)))
            steps = (d_sz / d_lot).quantize(Decimal("1"), rounding=ROUND_DOWN)
            aligned = max(Decimal(str(min_sz)), steps * d_lot)
            return format(aligned.normalize(), "f")

    def _format_order_px(self, symbol: str, px: float) -> str:
        """将委托价格或触发价格按 OKX 标的 tickSz 最小步长对齐，杜绝 51006/51000 价格精度拒单"""
        from decimal import Decimal, ROUND_HALF_UP

        if px is None or px <= 0:
            return "0"
        _, tick_sz, _, _ = self.get_instrument_spec(symbol)
        d_px = Decimal(str(float(px)))
        d_tick = Decimal(str(max(tick_sz, 1e-10)))
        steps = (d_px / d_tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        aligned_px = max(d_tick, steps * d_tick)
        return format(aligned_px.normalize(), "f")

    # =========================================================================
    # 3. 交易委托与订单管理接口 (Trading & Order APIs)
    # =========================================================================

    async def place_order(
        self,
        symbol: str,
        side: Union[OrderSide, str],
        pos_side: Union[PosSide, str] = "net",
        order_type: Union[OrderType, str] = "limit",
        sz: float = 1.0,
        price: Optional[float] = None,
        trade_mode: str = "cross",
        leverage: int = 1,
        client_order_id: Optional[str] = None,
        reduce_only: bool = False,
        tp_trigger_price: Optional[float] = None,
        tp_order_price: Optional[float] = None,
        sl_trigger_price: Optional[float] = None,
        sl_order_price: Optional[float] = None,
        size: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        提交委托下单 (支持现货/永续、市价/限价、附带 TP/SL 委托、自动对齐账户持仓模式与合约张数)
        POST /api/v5/trade/order
        """
        if size is not None and size > 0:
            sz = size

        side_val = side.value if isinstance(side, OrderSide) else str(side).lower()
        pos_side_val = pos_side.value if isinstance(pos_side, PosSide) else str(pos_side).lower()
        ord_type_val = order_type.value if isinstance(order_type, OrderType) else str(order_type).lower()
        if ord_type_val == "optimal_limit_ioc":
            ord_type_val = "optimal_limit_ioc"

        # 1. 自动对齐 OKX 账户持仓模式 (net_mode 单向 vs long_short_mode 双向)
        acc_mode = "long_short_mode"
        if "SWAP" in symbol or "FUTURES" in symbol:
            try:
                acc_mode = await self.get_account_pos_mode()
                if acc_mode == "net_mode":
                    pos_side_val = "net"
                else:
                    if pos_side_val == "net":
                        if reduce_only:
                            pos_side_val = "long" if side_val == "sell" else "short"
                        else:
                            pos_side_val = "long" if side_val == "buy" else "short"
            except Exception as mode_err:
                logger.debug(f"自适应持仓模式检测提示: {mode_err}")

        # 2. 数量转换为 OKX 交易所标准张数或小数
        formatted_sz = self._format_order_sz(symbol, float(sz))

        # 3. 如果没有指定 client_order_id，自动生成金融级防碰撞全局唯一 ID
        if not client_order_id:
            client_order_id = f"OKXD{int(time.time()*1000)}{secrets.token_hex(4)}"

        body: Dict[str, Any] = {
            "instId": symbol,
            "tdMode": trade_mode,
            "side": side_val,
            "posSide": pos_side_val,
            "ordType": ord_type_val,
            "sz": formatted_sz,
            "clOrdId": client_order_id,
        }
        # 在双向持仓模式 (long_short_mode) 下，OKX 根据 side + posSide 自动区分开平仓，仅在 net_mode 或减仓时传 reduceOnly
        if reduce_only and acc_mode == "net_mode":
            body["reduceOnly"] = True

        # 限价单必须包含按 tickSz 对齐的价格
        if ord_type_val in ("limit", "post_only", "fok", "ioc") and price is not None and price > 0:
            body["px"] = self._format_order_px(symbol, price)

        # 附带止盈止损 (attachAlgoOrds)
        attach_algos: List[Dict[str, Any]] = []
        if tp_trigger_price is not None and tp_trigger_price > 0 and not reduce_only:
            tp_ord_px_str = self._format_order_px(symbol, tp_order_price) if (tp_order_price and tp_order_price > 0) else "-1"
            attach_algos.append({
                "attachAlgoId": "",
                "tpTriggerPx": self._format_order_px(symbol, tp_trigger_price),
                "tpOrdPx": tp_ord_px_str,
                "tpTriggerPxType": "last"
            })

        if sl_trigger_price is not None and sl_trigger_price > 0 and not reduce_only:
            sl_ord_px_str = self._format_order_px(symbol, sl_order_price) if (sl_order_price and sl_order_price > 0) else "-1"
            attach_algos.append({
                "attachAlgoId": "",
                "slTriggerPx": self._format_order_px(symbol, sl_trigger_price),
                "slOrdPx": sl_ord_px_str,
                "slTriggerPxType": "mark"
            })

        if attach_algos:
            tp_obj = next((a for a in attach_algos if "tpTriggerPx" in a), None)
            sl_obj = next((a for a in attach_algos if "slTriggerPx" in a), None)

            attach_data: Dict[str, Any] = {}
            if tp_obj:
                attach_data["tpTriggerPx"] = tp_obj["tpTriggerPx"]
                attach_data["tpOrdPx"] = tp_obj["tpOrdPx"]
                attach_data["tpTriggerPxType"] = tp_obj.get("tpTriggerPxType", "last")
            if sl_obj:
                attach_data["slTriggerPx"] = sl_obj["slTriggerPx"]
                attach_data["slOrdPx"] = sl_obj["slOrdPx"]
                attach_data["slTriggerPxType"] = sl_obj.get("slTriggerPxType", "mark")

            body["attachAlgoOrds"] = [attach_data]

        # 4. 开仓前自适应更新杠杆
        if leverage and leverage >= 1 and "SWAP" in symbol and not reduce_only:
            try:
                await self.set_leverage(
                    symbol=symbol,
                    leverage=leverage,
                    mgn_mode=trade_mode,
                    pos_side=pos_side_val if pos_side_val != "net" else None
                )
            except Exception as lev_err:
                logger.debug(f"前置设置杠杆倍数提示 (非致命): {lev_err}")

        logger.info(f"正在提交 OKX 订单: {body}")
        try:
            res = await self._request("POST", "/api/v5/trade/order", json_data=body)
        except OKXApiException as okx_err:
            # Stage 0 (T0.1) 安全红线：附带硬止损校验失败时严禁降级提交裸单，直接拒单并推送紧急告警
            if "attachAlgoOrds" in body and body["attachAlgoOrds"]:
                alert_msg = (
                    f"🚨【安全阻断-禁止无止损裸单】标的 {symbol} 附带 TP/SL 委托校验未通过 ({okx_err})，"
                    f"系统已强制拒绝本次开仓，杜绝产生交易所侧无止损裸仓！"
                )
                logger.critical(alert_msg)
                try:
                    from state import telegram_bot_service
                    asyncio.create_task(telegram_bot_service.send_message(alert_msg))
                except Exception:
                    pass
            raise

        if not res:
            raise OKXApiException("500", "OKX 订单提交未返回数据")

        item = res[0]
        s_code = item.get("sCode", "0")
        s_msg = item.get("sMsg", "")
        if s_code != "0":
            raise OKXApiException(s_code, f"订单提交失败: {s_msg}", data=item)

        return item

    async def get_pending_orders(
        self,
        symbol: Optional[str] = None,
        inst_type: str = "SWAP"
    ) -> List[Dict[str, Any]]:
        """
        获取当前未成交活跃挂单
        GET /api/v5/trade/orders-pending
        """
        params: Dict[str, Any] = {"instType": inst_type}
        if symbol:
            params["instId"] = symbol
        res = await self._request("GET", "/api/v5/trade/orders-pending", params=params)
        return res or []

    async def get_orders(
        self,
        symbol: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 50
    ) -> List[OrderModel]:
        """
        查询实盘当前挂单与历史订单并转换为标准 OrderModel 列表
        """
        now_ms = int(time.time() * 1000)
        raw_orders: List[Dict[str, Any]] = []
        try:
            pending = await self.get_pending_orders(symbol=symbol, inst_type="SWAP")
            raw_orders.extend(pending)
        except Exception as e:
            logger.debug(f"拉取实盘活跃挂单提示: {e}")

        if state != "live":
            try:
                hist_params: Dict[str, Any] = {"instType": "SWAP", "limit": str(min(limit, 50))}
                if symbol:
                    hist_params["instId"] = symbol
                hist = await self._request("GET", "/api/v5/trade/orders-history", params=hist_params)
                if hist:
                    raw_orders.extend(hist)
            except Exception as e:
                logger.debug(f"拉取实盘历史订单提示: {e}")

        result: List[OrderModel] = []
        for o in raw_orders[:limit]:
            inst_id = o.get("instId", symbol or "BTC-USDT-SWAP")
            ct_val = self.get_contract_value(inst_id)
            raw_sz = float(o.get("sz") or 0.0)
            raw_fill_sz = float(o.get("accFillSz") or o.get("fillSz") or 0.0)
            coins_sz = round(raw_sz * ct_val, 6)
            coins_fill = round(raw_fill_sz * ct_val, 6)
            avg_px = float(o.get("avgPx") or 0.0)
            px = float(o.get("px") or 0.0) if o.get("px") else None
            ref_px = avg_px if avg_px > 0 else (px or 0.0)
            notional = round(coins_sz * ref_px, 2)

            try:
                result.append(OrderModel(
                    client_order_id=o.get("clOrdId") or o.get("ordId") or f"OKX_{now_ms}",
                    okx_order_id=o.get("ordId", ""),
                    symbol=inst_id,
                    inst_type=InstType.SWAP,
                    side=OrderSide.BUY if str(o.get("side", "buy")).lower() == "buy" else OrderSide.SELL,
                    pos_side=PosSide(str(o.get("posSide", "net")).lower()) if str(o.get("posSide", "net")).lower() in ("long", "short", "net") else PosSide.NET,
                    order_type=OrderType(str(o.get("ordType", "limit")).lower()) if str(o.get("ordType", "limit")).lower() in ("market", "limit", "post_only", "fok", "ioc") else OrderType.LIMIT,
                    price=px,
                    sz=coins_sz,
                    notional_usdt=notional,
                    filled_sz=coins_fill,
                    avg_price=avg_px,
                    state=OrderState(str(o.get("state", "live")).lower()) if str(o.get("state", "live")).lower() in ("live", "partially_filled", "filled", "canceled") else OrderState.LIVE,
                    leverage=int(float(o.get("lever") or 1)),
                    fee=float(o.get("fee") or 0.0),
                    pnl=float(o.get("pnl") or 0.0),
                    tp_trigger_price=float(o.get("tpTriggerPx")) if o.get("tpTriggerPx") else None,
                    sl_trigger_price=float(o.get("slTriggerPx")) if o.get("slTriggerPx") else None,
                    created_at=int(o.get("cTime") or now_ms),
                    updated_at=int(o.get("uTime") or now_ms)
                ))
            except Exception:
                continue
        return result

    async def get_pending_algo_orders(
        self,
        symbol: Optional[str] = None,
        inst_type: str = "SWAP",
        ord_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        获取当前未完成策略/条件挂单 (如止盈止损单)
        GET /api/v5/trade/orders-algo-pending
        """
        params: Dict[str, Any] = {"instType": inst_type}
        if symbol:
            params["instId"] = symbol
        if ord_type:
            params["ordType"] = ord_type
        else:
            params["ordType"] = "conditional"
        res = await self._request("GET", "/api/v5/trade/orders-algo-pending", params=params)
        return res or []

    async def cancel_batch_algo_orders(
        self,
        orders: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        批量撤销策略/条件挂单 (单次最多 10 笔)
        POST /api/v5/trade/cancel-algos
        :param orders: [{"algoId": "...", "instId": "..."}, ...]
        """
        if not orders:
            return []
        results: List[Dict[str, Any]] = []
        for i in range(0, len(orders), 10):
            batch = orders[i:i + 10]
            try:
                res = await self._request("POST", "/api/v5/trade/cancel-algos", json_data=batch)
                if res:
                    results.extend(res)
            except Exception as e:
                logger.error(f"批量撤销策略委托失败 batch={batch}: {e}")
        return results

    async def cancel_order(
        self,
        symbol: str,
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        单笔撤单
        POST /api/v5/trade/cancel-order
        """
        if not order_id and not client_order_id:
            raise ValueError("撤单必须提供 order_id 或 client_order_id")

        body: Dict[str, Any] = {"instId": symbol}
        if order_id:
            body["ordId"] = order_id
        if client_order_id:
            body["clOrdId"] = client_order_id

        res = await self._request("POST", "/api/v5/trade/cancel-order", json_data=body)
        if not res:
            raise OKXApiException("500", "撤单响应为空")

        item = res[0]
        s_code = item.get("sCode", "0")
        if s_code != "0":
            raise OKXApiException(s_code, f"撤单失败: {item.get('sMsg', '')}", data=item)

        return item

    async def cancel_batch_orders(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        批量撤单 (单次最多 20 笔)
        POST /api/v5/trade/cancel-batch-orders
        :param orders: [{"instId": "BTC-USDT-SWAP", "ordId": "..."}, ...]
        """
        if not orders:
            return []

        results: List[Dict[str, Any]] = []
        for i in range(0, len(orders), 20):
            batch = orders[i:i + 20]
            res = await self._request("POST", "/api/v5/trade/cancel-batch-orders", json_data=batch)
            results.extend(res)

        return results

    async def cancel_all_orders(
        self,
        symbol: Optional[str] = None,
        inst_type: str = "SWAP"
    ) -> List[Dict[str, Any]]:
        """
        撤销指定标的（或全部标的）的所有普通活跃挂单与策略条件挂单 (Cancel-Before-Close)
        """
        canceled: List[Dict[str, Any]] = []
        try:
            pending = await self.get_pending_orders(symbol=symbol, inst_type=inst_type)
            if pending:
                to_cancel = [
                    {"instId": o["instId"], "ordId": o["ordId"]}
                    for o in pending if "instId" in o and "ordId" in o
                ]
                if to_cancel:
                    res = await self.cancel_batch_orders(to_cancel)
                    canceled.extend(res)
        except Exception as e:
            logger.debug(f"撤销普通挂单异常 (非阻塞): {e}")

        for ord_tp in ("conditional", "oco", "trigger"):
            try:
                algos = await self.get_pending_algo_orders(symbol=symbol, inst_type=inst_type, ord_type=ord_tp)
                if algos:
                    to_cancel_algo = [
                        {"instId": a["instId"], "algoId": a["algoId"]}
                        for a in algos if "instId" in a and "algoId" in a
                    ]
                    if to_cancel_algo:
                        res_algo = await self.cancel_batch_algo_orders(to_cancel_algo)
                        canceled.extend(res_algo)
            except Exception:
                pass
        return canceled

    async def place_tpsl_algo_order(
        self,
        symbol: str,
        pos_side: Union[PosSide, str],
        sz: float,
        tp_trigger_price: Optional[float] = None,
        sl_trigger_price: Optional[float] = None,
        trade_mode: str = "cross"
    ) -> Dict[str, Any]:
        """
        为现有实盘持仓挂载/更新止盈止损策略委托单
        POST /api/v5/trade/order-algo
        """
        pos_side_val = pos_side.value if isinstance(pos_side, PosSide) else str(pos_side).lower()
        side_val = "sell" if pos_side_val == "long" else "buy"
        acc_mode = await self.get_account_pos_mode()

        body: Dict[str, Any] = {
            "instId": symbol,
            "tdMode": trade_mode,
            "side": side_val,
            "posSide": "net" if acc_mode == "net_mode" else pos_side_val,
            "ordType": "oco" if (tp_trigger_price and sl_trigger_price) else "conditional",
            "sz": self._format_order_sz(symbol, sz),
            "reduceOnly": True
        }
        if tp_trigger_price and tp_trigger_price > 0:
            body["tpTriggerPx"] = self._format_order_px(symbol, tp_trigger_price)
            body["tpOrdPx"] = "-1"
            body["tpTriggerPxType"] = "last"
        if sl_trigger_price and sl_trigger_price > 0:
            body["slTriggerPx"] = self._format_order_px(symbol, sl_trigger_price)
            body["slOrdPx"] = "-1"
            body["slTriggerPxType"] = "mark"

        res = await self._request("POST", "/api/v5/trade/order-algo", json_data=body)
        item = res[0] if res else {}
        s_code = str(item.get("sCode", "0"))
        if s_code != "0":
            raise OKXApiException(s_code, f"止盈止损策略单挂载失败: {item.get('sMsg', '未知错误')}", data=item)
        return item

    async def replace_position_stop_loss_safely(
        self,
        symbol: str,
        pos_side: Union[PosSide, str],
        sz: float,
        sl_trigger_price: float,
        tp_trigger_price: Optional[float] = None,
        trade_mode: str = "cross",
        max_retries: int = 2
    ) -> Dict[str, Any]:
        """
        Stage 0 (T0.2) 零裸仓窗口安全改损协议 (Mount-Before-Cancel)：
        1. 先快照当前标的已有的条件止损单 algoId 列表；
        2. 优先向交易所挂载新的止损策略单并校验回执 (sCode == '0' 且返回 algoId)，失败最多重试 max_retries 次；
        3. 仅在新止损确认生效后，才撤销旧的条件止损单，彻底消除“先撤单再挂单”导致的裸仓真空期；
        4. 若挂载失败，保留旧止损单完好无损，提升至 ERROR 日志并推送 Telegram 紧急告警。
        """
        old_algos: List[Dict[str, str]] = []
        inst_type = "SWAP" if "SWAP" in symbol else ("SPOT" if "-" in symbol else "SWAP")
        for ord_tp in ("conditional", "oco", "trigger"):
            try:
                pending = await self.get_pending_algo_orders(symbol=symbol, inst_type=inst_type, ord_type=ord_tp)
                for a in pending:
                    if a.get("instId") == symbol and a.get("algoId"):
                        old_algos.append({"instId": a["instId"], "algoId": a["algoId"]})
            except Exception as q_err:
                logger.warning(f"[安全改损] 查询旧条件单 ({ord_tp}) 异常 {symbol}: {q_err}")

        new_algo_res: Optional[Dict[str, Any]] = None
        last_err: Optional[Exception] = None
        for attempt in range(1, max_retries + 2):
            try:
                res = await self.place_tpsl_algo_order(
                    symbol=symbol,
                    pos_side=pos_side,
                    sz=sz,
                    tp_trigger_price=tp_trigger_price,
                    sl_trigger_price=sl_trigger_price,
                    trade_mode=trade_mode,
                )
                if res and res.get("algoId"):
                    new_algo_res = res
                    break
                raise OKXApiException("500", f"新止损单未返回有效 algoId: {res}")
            except Exception as e:
                last_err = e
                logger.error(f"[安全改损] {symbol} 新止损单挂载第 {attempt} 次失败 (SL=${sl_trigger_price}): {e}")
                if attempt <= max_retries:
                    await asyncio.sleep(0.5 * attempt)

        if not new_algo_res:
            alert_msg = (
                f"🚨【安全改损失败-已保留旧止损】标的 {symbol} 尝试挂载新止损价 `${sl_trigger_price}` 连续失败: {last_err}。"
                f"系统已取消撤销旧条件单以避免裸仓，请立即人工检查交易所止损状态！"
            )
            logger.error(alert_msg)
            try:
                from state import telegram_bot_service
                asyncio.create_task(telegram_bot_service.send_message(alert_msg))
            except Exception:
                pass
            raise OKXApiException("SL_REPLACE_FAILED", str(last_err or "新止损单挂载失败"))

        # 新止损已确认生效，再安全撤销旧止损条件单
        new_algo_id = new_algo_res.get("algoId")
        to_cancel = [a for a in old_algos if a.get("algoId") != new_algo_id]
        if to_cancel:
            try:
                await self.cancel_batch_algo_orders(to_cancel)
                logger.info(f"[安全改损] {symbol} 新止损 ({new_algo_id}) 已生效，成功清理 {len(to_cancel)} 笔旧条件单")
            except Exception as cxl_err:
                logger.warning(f"[安全改损] {symbol} 新止损已生效，但清理旧条件单出现轻微异常 (不影响防护): {cxl_err}")

        return new_algo_res

    async def close_position(
        self,
        symbol: str,
        mgn_mode: str = "cross",
        pos_side: Optional[Union[PosSide, str]] = None,
        ccy: Optional[str] = None,
        auto_cancel: bool = True,
        order_type: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        快捷市价平仓 (兼容 PosSide 枚举及 order_type 冗余参数，自动对齐账户持仓模式)
        POST /api/v5/trade/close-position
        """
        body: Dict[str, Any] = {
            "instId": symbol,
            "mgnMode": mgn_mode,
            "autoCxl": auto_cancel
        }
        if pos_side is not None:
            pos_side_str = pos_side.value if isinstance(pos_side, PosSide) else str(pos_side).lower()
            try:
                acc_mode = await self.get_account_pos_mode()
                if acc_mode == "net_mode":
                    pos_side_str = "net"
            except Exception:
                pass
            body["posSide"] = pos_side_str
        if ccy:
            body["ccy"] = ccy

        logger.info(f"正在发起市价平仓: {body}")
        res = await self._request("POST", "/api/v5/trade/close-position", json_data=body)
        return res[0] if res else {}

    # =========================================================================
    # 4. 连通性测试与权限探测 (Diagnostic & Health)
    # =========================================================================

    async def test_credentials(self) -> Tuple[bool, int, List[str], Optional[str]]:
        """
        测试当前 API 凭证连通性、测算网络延迟并真实解析 OKX 账户权限列表 (perm)
        :return: (is_connected, latency_ms, permissions, error_message)
        """
        start_time = time.monotonic()
        try:
            data = await self._request("GET", "/api/v5/account/config")
            latency = int((time.monotonic() - start_time) * 1000)

            permissions: List[str] = []
            warning_msg: Optional[str] = None
            if data and isinstance(data, list):
                cfg = data[0]
                self._account_pos_mode = cfg.get("posMode", "long_short_mode")
                raw_perm = str(cfg.get("perm", "")).lower()
                if "read_only" in raw_perm or "read" in raw_perm or not raw_perm:
                    permissions.append("read")
                if "trade" in raw_perm or not raw_perm:
                    permissions.append("trade")
                if "withdraw" in raw_perm:
                    permissions.append("withdraw")
                if raw_perm and "trade" not in raw_perm:
                    warning_msg = "当前 OKX API Key 仅开启了只读 (read_only) 权限，未勾选交易 (trade) 权限，请在欧易后台开启交易权限！"
            else:
                permissions = ["read", "trade"]

            return True, latency, permissions, warning_msg
        except OKXApiException as okx_err:
            latency = int((time.monotonic() - start_time) * 1000)
            return False, latency, [], str(okx_err)
        except Exception as e:
            latency = int((time.monotonic() - start_time) * 1000)
            return False, latency, [], f"网络连接失败: {str(e)}"
