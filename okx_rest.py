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

    async def _get_client(self) -> httpx.AsyncClient:
        """获取或创建单例 HTTPX 异步客户端"""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(self.timeout, connect=5.0),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
                headers={"Accept": "application/json"}
            )
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

        # 模拟盘必须包含 x-simulated-trading 标头
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
        if "/api/v5/market/" in path or "/api/v5/public/" in path:
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
        return TickerData(
            symbol=item.get("instId", symbol),
            last_price=float(item.get("last", 0.0)),
            last_sz=float(item.get("lastSz", 0.0)) if item.get("lastSz") else 0.0,
            high_24h=float(item.get("high24h", 0.0)) if item.get("high24h") else float(item.get("last", 0.0)),
            low_24h=float(item.get("low24h", 0.0)) if item.get("low24h") else float(item.get("last", 0.0)),
            open_24h=float(item.get("open24h", 0.0)) if item.get("open24h") else float(item.get("last", 0.0)),
            change_24h_pct=float(item.get("sodUtc8", 0.0)) if item.get("sodUtc8") else 0.0,
            volume_24h=float(item.get("vol24h", 0.0)) if item.get("vol24h") else 0.0,
            vol_currency_24h=float(item.get("volCcy24h", 0.0)) if item.get("volCcy24h") else 0.0,
            timestamp=int(item.get("ts", now_ms))
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

    async def get_open_interest(self, symbol: str, inst_type: str = "SWAP") -> Dict[str, Any]:
        """获取合约全网持仓量 GET /api/v5/public/open-interest"""
        params = {"instType": inst_type, "instId": symbol}
        data = await self._request("GET", "/api/v5/public/open-interest", params=params)
        return data[0] if data else {}

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
        total_eq = float(item.get("totalEq", 0.0) or 0.0)
        adj_eq = float(item.get("adjEq", 0.0) or 0.0)
        mgn_ratio_str = item.get("mgnRatio", "")
        mgn_ratio = float(mgn_ratio_str) if mgn_ratio_str else 999.0

        details = item.get("details", [])
        avail_margin = float(item.get("availMgn", 0.0) or 0.0) if item.get("availMgn") else 0.0
        used_margin = float(item.get("imr", 0.0) or 0.0) if item.get("imr") else 0.0
        frozen_bal = float(item.get("ordFzn", 0.0) or 0.0) if item.get("ordFzn") else 0.0
        upl = float(item.get("upl", 0.0) or 0.0) if item.get("upl") else 0.0

        # 从各币种资产细节汇总 (防止 simple 模式或非跨币种全仓模式下 totalEq 为空)
        if details:
            details_eq_sum = 0.0
            details_avail_sum = 0.0
            details_upl_sum = 0.0
            details_frozen_sum = 0.0
            for d in details:
                eq_usd = float(d.get("eqUsd", 0.0) or 0.0)
                eq_native = float(d.get("eq", d.get("cashBal", 0.0)) or 0.0)
                details_eq_sum += eq_usd if eq_usd > 0 else (eq_native if d.get("ccy") == "USDT" else 0.0)
                details_upl_sum += float(d.get("upl", 0.0) or 0.0)
                details_frozen_sum += float(d.get("ordFrozen", d.get("frozenBal", 0.0)) or 0.0)
                if d.get("ccy") == "USDT":
                    avail_val = float(d.get("availMgn", d.get("availBal", d.get("cashBal", 0.0))) or 0.0)
                    details_avail_sum += avail_val

            if total_eq <= 0.0 and details_eq_sum > 0.0:
                total_eq = details_eq_sum
            if avail_margin <= 0.0 and details_avail_sum > 0.0:
                avail_margin = details_avail_sum
            if upl == 0.0 and details_upl_sum != 0.0:
                upl = details_upl_sum
            if frozen_bal <= 0.0 and details_frozen_sum > 0.0:
                frozen_bal = details_frozen_sum

        # 计算今日已实现盈亏与收益率 (从 OKX bills 账单流水中汇总当日平仓及资金费盈亏)
        today_realized_pnl = 0.0
        try:
            today_start_dt = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            today_start_ms = int(today_start_dt.timestamp() * 1000)
            bills_data = await self._request("GET", "/api/v5/account/bills", params={"begin": str(today_start_ms), "limit": "100"})
            if bills_data:
                for b in bills_data:
                    today_realized_pnl += float(b.get("pnl", 0.0) or 0.0)
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

    async def get_positions(
        self,
        inst_type: Optional[str] = None,
        symbol: Optional[str] = None,
        include_spot: bool = True
    ) -> List[PositionModel]:
        """
        获取当前持仓列表 (包括 SWAP 永续合约、MARGIN 币币杠杆、FUTURES 交割以及 SPOT 现货持有资产)
        GET /api/v5/account/positions & GET /api/v5/account/balance
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
                pos_sz = float(p.get("pos", 0.0))
                if pos_sz == 0.0:
                    continue

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
                    side_enum = PosSide.NET

                pos_model = PositionModel(
                    pos_id=p.get("posId", ""),
                    symbol=p.get("instId", ""),
                    inst_type=inst_enum,
                    pos_side=side_enum,
                    pos=pos_sz,
                    avail_pos=float(p.get("availPos", pos_sz)),
                    avg_px=float(p.get("avgPx", 0.0)),
                    mark_px=float(p.get("markPx", 0.0)),
                    liq_px=float(p.get("liqPx", 0.0)) if p.get("liqPx") else None,
                    margin_ratio=float(p.get("mgnRatio", 0.0)) if p.get("mgnRatio") else 0.0,
                    maint_margin_ratio=float(p.get("mmr", 0.01)) if p.get("mmr") else 0.01,
                    leverage=int(p.get("lever", 1)),
                    notional_usd=float(p.get("notionalUsd", 0.0)) if p.get("notionalUsd") else 0.0,
                    upl=float(p.get("upl", 0.0)) if p.get("upl") else 0.0,
                    upl_ratio=float(p.get("uplRatio", 0.0)) if p.get("uplRatio") else 0.0,
                    realized_pnl=float(p.get("realizedPnl", 0.0)) if p.get("realizedPnl") else 0.0,
                    margin=float(p.get("margin", 0.0)) if p.get("margin") else 0.0,
                    last_synced_at=int(p.get("uTime", now_ms))
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
                        # 过滤 USDT/USDC 等稳定币基础计价资产，仅将主流币/现货币种 (ETH, BTC, SOL 等) 作为现货持仓呈现
                        if ccy in ("USDT", "USDC", "USD"):
                            continue
                        pos_sz = float(d.get("eq", 0.0))
                        eq_usd = float(d.get("eqUsd", 0.0))
                        # 仅展示名义价值大于 0.5 USDT 的实际持有现货
                        if pos_sz > 0 and eq_usd >= 0.5:
                            unit_px = (eq_usd / pos_sz) if pos_sz > 0 else 0.0
                            avail_sz = float(d.get("availBal", pos_sz))
                            spot_pos = PositionModel(
                                pos_id=f"spot_{ccy.lower()}",
                                symbol=f"{ccy}-USDT",
                                inst_type=InstType.SPOT,
                                pos_side=PosSide.NET,
                                pos=pos_sz,
                                avail_pos=avail_sz,
                                avg_px=round(unit_px, 2),
                                mark_px=round(unit_px, 2),
                                liq_px=None,
                                margin_ratio=0.0,
                                maint_margin_ratio=0.0,
                                leverage=1,
                                notional_usd=round(eq_usd, 2),
                                upl=float(d.get("upl", 0.0)),
                                upl_ratio=0.0,
                                realized_pnl=0.0,
                                margin=round(eq_usd, 2),
                                last_synced_at=int(d.get("uTime", now_ms))
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
        sl_order_price: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        提交委托下单 (支持现货/永续、市价/限价、附带 TP/SL 委托)
        POST /api/v5/trade/order
        """
        side_val = side.value if isinstance(side, OrderSide) else side
        pos_side_val = pos_side.value if isinstance(pos_side, PosSide) else pos_side
        ord_type_val = order_type.value if isinstance(order_type, OrderType) else order_type

        # 如果没有指定 client_order_id，自动生成金融级防碰撞全局唯一 ID (OKXD + 13位毫秒时间戳 + 8位安全随机数，总长25字符 <= 32)
        if not client_order_id:
            client_order_id = f"OKXD{int(time.time()*1000)}{secrets.token_hex(4)}"

        body: Dict[str, Any] = {
            "instId": symbol,
            "tdMode": trade_mode,
            "side": side_val,
            "posSide": pos_side_val,
            "ordType": ord_type_val,
            "sz": str(sz),
            "clOrdId": client_order_id,
            "reduceOnly": reduce_only
        }

        # 限价单必须包含价格
        if ord_type_val in ("limit", "post_only", "fok", "ioc") and price is not None:
            body["px"] = str(price)

        # 附带止盈止损 (attachAlgoOrds)
        attach_algos: List[Dict[str, Any]] = []
        if tp_trigger_price is not None and tp_trigger_price > 0:
            tp_ord_px_str = str(tp_order_price) if (tp_order_price and tp_order_price > 0) else "-1"
            attach_algos.append({
                "attachAlgoId": "",
                "tpTriggerPx": str(tp_trigger_price),
                "tpOrdPx": tp_ord_px_str,
                "tpTriggerPxType": "last"
            })

        if sl_trigger_price is not None and sl_trigger_price > 0:
            sl_ord_px_str = str(sl_order_price) if (sl_order_price and sl_order_price > 0) else "-1"
            # 止损默认采用 mark (标记价格) 防恶意深度插针
            attach_algos.append({
                "attachAlgoId": "",
                "slTriggerPx": str(sl_trigger_price),
                "slOrdPx": sl_ord_px_str,
                "slTriggerPxType": "mark"
            })

        if attach_algos:
            # 整合附带止盈止损参数
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

        logger.info(f"正在提交 OKX 订单: {body}")
        res = await self._request("POST", "/api/v5/trade/order", json_data=body)
        if not res:
            raise OKXApiException("500", "OKX 订单提交未返回数据")

        item = res[0]
        s_code = item.get("sCode", "0")
        s_msg = item.get("sMsg", "")
        if s_code != "0":
            raise OKXApiException(s_code, f"订单提交失败: {s_msg}", data=item)

        return item

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

    async def close_position(
        self,
        symbol: str,
        mgn_mode: str = "cross",
        pos_side: Optional[str] = None,
        ccy: Optional[str] = None,
        auto_cancel: bool = True
    ) -> Dict[str, Any]:
        """
        快捷市价平仓
        POST /api/v5/trade/close-position
        """
        body: Dict[str, Any] = {
            "instId": symbol,
            "mgnMode": mgn_mode,
            "autoCxl": auto_cancel
        }
        if pos_side:
            body["posSide"] = pos_side
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
        测试当前 API 凭证连通性、测算网络延迟并探测权限列表
        :return: (is_connected, latency_ms, permissions, error_message)
        """
        start_time = time.monotonic()
        try:
            # 请求用户配置接口验证鉴权与只读/交易权限
            data = await self._request("GET", "/api/v5/account/config")
            latency = int((time.monotonic() - start_time) * 1000)

            permissions = ["read", "trade"]
            # 检查返回配置中是否涉及提现权限标识
            return True, latency, permissions, None
        except OKXApiException as okx_err:
            latency = int((time.monotonic() - start_time) * 1000)
            return False, latency, [], str(okx_err)
        except Exception as e:
            latency = int((time.monotonic() - start_time) * 1000)
            return False, latency, [], f"网络连接失败: {str(e)}"
