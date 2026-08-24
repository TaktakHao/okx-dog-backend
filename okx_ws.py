"""
OKX-Dog 个人量化智能交易系统 - OKX v5 WebSocket 异步长连接管理模块
模块: okx-dog-backend/okx_ws.py
实现 OKX v5 公有与私有长连接池、自动鉴权、20s 定时心跳、指数退避重连、订阅状态自愈与异步消息分发
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set, Union
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from config import settings
from models import (
    WSServerEvent,
    WSPushPayload,
    TickerData,
    KlineCandle,
    OrderBookDepth,
    OrderBookLevel,
    PositionModel,
    OrderModel
)

logger = logging.getLogger("okx_dog.ws")

# 消息处理回调函数类型定义
MessageHandler = Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]


class OKXWebSocketClient:
    """
    单个 OKX v5 WebSocket 连接客户端
    - 管理单一长连接生命周期（公有或私有）
    - 20s 自动心跳 ping / pong
    - 心跳超时熔断检测（30s 无响应自动断线）
    - 指数退避自动重连（1s, 2s, 4s, 8s, 16s, 30s）
    - 连接重连成功后的订阅自愈与自动重新鉴权
    """

    RECONNECT_DELAYS = [1, 2, 4, 8, 16, 30]

    def __init__(
        self,
        ws_url: str,
        is_private: bool = False,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        passphrase: Optional[str] = None,
        name: str = "OKX-WS"
    ):
        self.ws_url = ws_url
        self.is_private = is_private
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.name = name

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._is_running = False
        self._is_authenticated = False
        self._last_heartbeat_time = 0.0
        self._last_message_time = 0.0
        self._reconnect_attempt = 0

        # 保存所有当前活跃订阅的通道参数字典集合 (用于断线重连自愈)
        # 每个 item 是 {"channel": "tickers", "instId": "BTC-USDT-SWAP"} 字典
        self._active_subscriptions: Dict[str, Dict[str, Any]] = {}

        # 消息监听器集合：key 为 channel 标识或 "*" (通配所有消息)
        self._listeners: Dict[str, Set[MessageHandler]] = {}

        # 异步队列，供消费协程读取
        self.message_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=10000)

        # 内部后台任务
        self._main_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None

    @property
    def is_connected(self) -> bool:
        """检查底层 WebSocket 连接是否处于打开状态 (兼容 websockets 14/15+ ClientConnection)"""
        if self._ws is None:
            return False
        if hasattr(self._ws, "state"):
            try:
                return str(self._ws.state).endswith("OPEN") or self._ws.state.name == "OPEN"
            except Exception:
                pass
        if hasattr(self._ws, "closed"):
            return not self._ws.closed
        if hasattr(self._ws, "open"):
            return bool(self._ws.open)
        return True

    @property
    def is_authenticated(self) -> bool:
        """检查私有连接是否已完成鉴权"""
        return self._is_authenticated

    def set_credentials(self, api_key: str, secret_key: str, passphrase: str):
        """动态更新 API 凭证"""
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def add_listener(self, channel: str, handler: MessageHandler):
        """注册特定频道的消息回调处理器"""
        if channel not in self._listeners:
            self._listeners[channel] = set()
        self._listeners[channel].add(handler)

    def remove_listener(self, channel: str, handler: MessageHandler):
        """移除特定频道的消息回调处理器"""
        if channel in self._listeners:
            self._listeners[channel].discard(handler)
            if not self._listeners[channel]:
                del self._listeners[channel]

    async def start(self):
        """启动 WebSocket 客户端主循环与心跳协程"""
        if self._is_running:
            logger.warning(f"[{self.name}] 客户端已在运行中")
            return

        self._is_running = True
        self._main_task = asyncio.create_task(self._connection_loop(), name=f"{self.name}-loop")
        logger.info(f"[{self.name}] WebSocket 客户端已启动，连接目标: {self.ws_url}")

    async def stop(self):
        """优雅关闭 WebSocket 连接并释放资源"""
        self._is_running = False
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()

        if self.is_connected and self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            logger.info(f"[{self.name}] 底层 WebSocket 已断开")

        if self._main_task and not self._main_task.done():
            self._main_task.cancel()

        logger.info(f"[{self.name}] WebSocket 客户端已停止")

    async def subscribe(self, args: List[Dict[str, Any]]):
        """
        订阅 OKX 频道
        :param args: OKX 订阅参数列表，例如 [{"channel": "tickers", "instId": "BTC-USDT-SWAP"}]
        """
        for arg in args:
            key = self._arg_to_key(arg)
            self._active_subscriptions[key] = arg

        if self.is_connected and (not self.is_private or self._is_authenticated):
            payload = {
                "op": "subscribe",
                "args": args
            }
            await self._send_json(payload)
            logger.info(f"[{self.name}] 已发送订阅请求: {args}")

    async def unsubscribe(self, args: List[Dict[str, Any]]):
        """
        取消订阅 OKX 频道
        :param args: OKX 订阅参数列表
        """
        for arg in args:
            key = self._arg_to_key(arg)
            self._active_subscriptions.pop(key, None)

        if self.is_connected:
            payload = {
                "op": "unsubscribe",
                "args": args
            }
            await self._send_json(payload)
            logger.info(f"[{self.name}] 已发送取消订阅请求: {args}")

    async def _send_json(self, data: Any):
        """向 WebSocket 连接发送 JSON 报文"""
        if self.is_connected and self._ws:
            try:
                msg_str = json.dumps(data)
                await self._ws.send(msg_str)
            except Exception as e:
                logger.error(f"[{self.name}] 发送数据异常: {e}")

    async def _send_text(self, text_data: str):
        """向 WebSocket 连接发送纯文本（用于心跳 ping）"""
        if self.is_connected and self._ws:
            try:
                await self._ws.send(text_data)
            except Exception as e:
                logger.error(f"[{self.name}] 发送文本帧异常: {e}")

    def _arg_to_key(self, arg: Dict[str, Any]) -> str:
        """将订阅参数转换为唯一标识 key"""
        channel = arg.get("channel", "")
        inst_id = arg.get("instId") or arg.get("instType") or ""
        return f"{channel}:{inst_id}"

    def _generate_login_signature(self) -> Dict[str, Any]:
        """
        生成 OKX v5 私有频道登录签名
        OKX 规则: timestamp = str(int(time.time())), prehash = timestamp + 'GET' + '/users/self/verify'
        """
        if not self.api_key or not self.secret_key or not self.passphrase:
            raise ValueError(f"[{self.name}] 私有通道连接必须配置 API Key, Secret 和 Passphrase")

        timestamp = str(int(time.time()))
        message = timestamp + "GET" + "/users/self/verify"
        mac = hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256
        )
        sign = base64.b64encode(mac.digest()).decode("utf-8")

        return {
            "apiKey": self.api_key,
            "passphrase": self.passphrase,
            "timestamp": timestamp,
            "sign": sign
        }

    async def _login(self):
        """执行私有连接登录认证"""
        try:
            sign_arg = self._generate_login_signature()
            payload = {
                "op": "login",
                "args": [sign_arg]
            }
            await self._send_json(payload)
            logger.info(f"[{self.name}] 已发送私有通道登录鉴权请求")
        except Exception as e:
            logger.error(f"[{self.name}] 生成登录签名失败: {e}")

    async def _connection_loop(self):
        """长连接维护主循环，处理重连与异常捕获"""
        while self._is_running:
            try:
                logger.info(f"[{self.name}] 正在连接 OKX WebSocket: {self.ws_url} ...")
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=None,  # OKX v5 必须使用文本帧 ping，禁用 websockets 自带的 opcode ping
                    ping_timeout=None,
                    close_timeout=10,
                    max_size=20 * 1024 * 1024  # 支持大深度盘口
                ) as ws:
                    self._ws = ws
                    self._reconnect_attempt = 0
                    self._last_heartbeat_time = time.time()
                    self._last_message_time = time.time()
                    self._is_authenticated = False
                    logger.info(f"[{self.name}] WebSocket 握手成功建立！")

                    # 启动定时心跳协程
                    if self._ping_task and not self._ping_task.done():
                        self._ping_task.cancel()
                    self._ping_task = asyncio.create_task(self._heartbeat_loop(), name=f"{self.name}-ping")

                    # 若为私有连接，首先发送登录
                    if self.is_private:
                        await self._login()
                    else:
                        # 公有连接直接恢复之前的订阅
                        await self._resubscribe_all()

                    # 接收并分发消息循环
                    async for raw_msg in ws:
                        self._last_message_time = time.time()
                        await self._handle_raw_message(raw_msg)

            except asyncio.CancelledError:
                logger.info(f"[{self.name}] 连接循环协程被取消")
                break
            except (ConnectionClosed, WebSocketException, Exception) as e:
                logger.warning(f"[{self.name}] WebSocket 连接断开或发生异常: {e}")
            finally:
                self._ws = None
                self._is_authenticated = False
                if self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()

            if self._is_running:
                delay = self.RECONNECT_DELAYS[min(self._reconnect_attempt, len(self.RECONNECT_DELAYS) - 1)]
                self._reconnect_attempt += 1
                logger.info(f"[{self.name}] 将在 {delay} 秒后尝试第 {self._reconnect_attempt} 次自动重连...")
                await asyncio.sleep(delay)

    async def _heartbeat_loop(self):
        """20 秒定时发送 ping 心跳文本帧，并检测 30 秒超时断开"""
        while self._is_running and self.is_connected:
            try:
                await asyncio.sleep(20)
                if not self.is_connected:
                    break

                # 发送 OKX v5 规定的 'ping' 字符串
                await self._send_text("ping")
                self._last_heartbeat_time = time.time()

                # 超时检测：如果超过 35 秒未收到任何服务器下行数据，强行主动关闭连接触发重连
                if time.time() - self._last_message_time > 35:
                    logger.warning(f"[{self.name}] 心跳超时（超过 35s 未收到任何服务端消息），主动断开重连")
                    if self.is_connected and self._ws:
                        try:
                            await self._ws.close()
                        except Exception:
                            pass
                    break

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.name}] 心跳检测协程异常: {e}")
                break

    async def _resubscribe_all(self):
        """连接恢复后重新订阅所有活跃通道"""
        if not self._active_subscriptions:
            return

        all_args = list(self._active_subscriptions.values())
        logger.info(f"[{self.name}] 触发订阅自愈，正在重新订阅 {len(all_args)} 个通道: {all_args}")
        # 分批订阅，每批最多 10 个
        for i in range(0, len(all_args), 10):
            batch = all_args[i:i + 10]
            payload = {
                "op": "subscribe",
                "args": batch
            }
            await self._send_json(payload)
            await asyncio.sleep(0.05)

    async def _handle_raw_message(self, raw_msg: Union[str, bytes]):
        """处理 OKX WebSocket 收到的原始数据"""
        if isinstance(raw_msg, bytes):
            raw_msg = raw_msg.decode("utf-8")

        # 处理心跳 pong 响应
        if raw_msg == "pong":
            logger.debug(f"[{self.name}] 收到服务端 pong 心跳响应")
            return

        try:
            msg_dict = json.loads(raw_msg)
        except Exception as e:
            logger.error(f"[{self.name}] 解析 JSON 消息失败: {raw_msg[:100]}..., 错误: {e}")
            return

        # 检查是否为事件响应 (event)
        event = msg_dict.get("event")
        if event:
            if event == "login":
                code = msg_dict.get("code", "")
                if code == "0":
                    self._is_authenticated = True
                    logger.info(f"[{self.name}] 私有通道登录鉴权成功！")
                    # 登录成功后自愈私有订阅
                    await self._resubscribe_all()
                else:
                    self._is_authenticated = False
                    logger.error(f"[{self.name}] 私有通道登录鉴权失败: {msg_dict.get('msg')} (code={code})")
            elif event == "subscribe":
                logger.info(f"[{self.name}] 成功订阅通道: {msg_dict.get('arg')}")
            elif event == "unsubscribe":
                logger.info(f"[{self.name}] 成功取消订阅: {msg_dict.get('arg')}")
            elif event == "error":
                logger.error(f"[{self.name}] OKX 报错事件: {msg_dict.get('msg')} (code={msg_dict.get('code')})")
            return

        # 放入队列供全局轮询读取
        try:
            self.message_queue.put_nowait(msg_dict)
        except asyncio.QueueFull:
            # 队列满则丢弃最老的一条
            try:
                _ = self.message_queue.get_nowait()
                self.message_queue.put_nowait(msg_dict)
            except Exception:
                pass

        # 触发监听回调分发
        arg = msg_dict.get("arg", {})
        channel = arg.get("channel", "")
        inst_id = arg.get("instId", "")

        specific_key = f"{channel}:{inst_id}"
        channel_key = channel
        wildcard_key = "*"

        # 收集目标处理器
        targets: Set[MessageHandler] = set()
        if specific_key in self._listeners:
            targets.update(self._listeners[specific_key])
        if channel_key in self._listeners:
            targets.update(self._listeners[channel_key])
        if wildcard_key in self._listeners:
            targets.update(self._listeners[wildcard_key])

        for handler in targets:
            try:
                asyncio.create_task(handler(msg_dict))
            except Exception as e:
                logger.error(f"[{self.name}] 回调分发执行异常: {e}")


class OKXWSManager:
    """
    OKX WebSocket 全局长连接管理中枢
    - 统筹管理公有连接 (Public WS) 与私有连接 (Private WS)
    - 针对实盘/模拟盘模式自动切换端点
    - 提供行情通道 (tickers, books5, candle*, trades) 的统一订阅与多播分发
    - 提供私有账户 (orders, positions, account) 的统一订阅与状态同步
    - 维护当前各标的的最新内存快照
    """

    def __init__(self, is_live_mode: bool = False):
        self.is_live_mode = is_live_mode

        # 选择对应端点
        if is_live_mode:
            self.public_url = settings.OKX_LIVE_WS_PUBLIC
            self.private_url = settings.OKX_LIVE_WS_PRIVATE
        else:
            self.public_url = settings.OKX_DEMO_WS_PUBLIC
            self.private_url = settings.OKX_DEMO_WS_PRIVATE

        self.public_client = OKXWebSocketClient(
            ws_url=self.public_url,
            is_private=False,
            name="OKX-Public"
        )
        self.private_client = OKXWebSocketClient(
            ws_url=self.private_url,
            is_private=True,
            name="OKX-Private"
        )

        # 最新内存快照缓存 (Symbol -> Data)
        self.latest_tickers: Dict[str, TickerData] = {}
        self.latest_depths: Dict[str, OrderBookDepth] = {}
        self.latest_klines: Dict[str, Dict[str, KlineCandle]] = {}  # symbol -> {timeframe: KlineCandle}
        self.latest_positions: Dict[str, PositionModel] = {}
        self.latest_orders: Dict[str, OrderModel] = {}

        # 外部广播分发队列（供与前端 WebSocket 桥接）
        self.broadcast_subscribers: Set[asyncio.Queue] = set()

        # 内部处理任务
        self._consumer_task: Optional[asyncio.Task] = None
        self._is_running = False

    def set_live_mode(self, is_live_mode: bool):
        """设置实盘/模拟运行模式"""
        self.is_live_mode = is_live_mode

    def update_credentials(self, api_key: str, secret_key: str, passphrase: str):
        """更新私有连接凭证"""
        self.private_client.set_credentials(api_key, secret_key, passphrase)

    def register_broadcast_subscriber(self, q: asyncio.Queue):

        """注册一个前端 WebSocket 广播订阅队列"""
        self.broadcast_subscribers.add(q)

    def unregister_broadcast_subscriber(self, q: asyncio.Queue):
        """注销一个前端 WebSocket 广播订阅队列"""
        self.broadcast_subscribers.discard(q)

    async def broadcast_to_subscribers(self, payload: WSPushPayload):
        """向所有注册的前端客户端异步广播推送数据"""
        if not self.broadcast_subscribers:
            return

        dead_subscribers = set()
        for q in list(self.broadcast_subscribers):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                try:
                    _ = q.get_nowait()
                    q.put_nowait(payload)
                except Exception:
                    dead_subscribers.add(q)
            except Exception:
                dead_subscribers.add(q)

        for q in dead_subscribers:
            self.broadcast_subscribers.discard(q)

    async def start(self):
        """启动公共与私有长连接中枢"""
        if self._is_running:
            return

        self._is_running = True
        logger.info(f"正在启动 OKX WebSocket 管理中枢 (实盘模式={self.is_live_mode})...")

        # 绑定消息解析处理器
        self.public_client.add_listener("*", self._on_public_message)
        self.private_client.add_listener("*", self._on_private_message)

        await self.public_client.start()
        await self.private_client.start()

    async def stop(self):
        """优雅停止 WebSocket 管理中枢"""
        self._is_running = False
        await self.public_client.stop()
        await self.private_client.stop()
        logger.info("OKX WebSocket 管理中枢已停止")

    async def subscribe_market_symbol(self, symbol: str):
        """
        快捷订阅指定标的的完整行情通道：
        1. tickers (行情)
        2. books5 (买卖前5档深度)
        3. trades (逐笔成交)
        4. funding-rate (资金费率)
        5. open-interest (持仓总量)
        """
        args = [
            {"channel": "tickers", "instId": symbol},
            {"channel": "books5", "instId": symbol},
            {"channel": "trades", "instId": symbol},
            {"channel": "funding-rate", "instId": symbol},
            {"channel": "open-interest", "instId": symbol},
        ]
        await self.public_client.subscribe(args)
        logger.info(f"已为标的 {symbol} 注册全套公共行情 WebSocket 订阅")

    async def subscribe_kline(self, symbol: str, timeframe: str = "1h"):
        """动态订阅指定标的与周期的 K 线频道 (candle15m / candle1H / candle4H / candle1D)"""
        tf_map = {
            "1m": "candle1m",
            "5m": "candle5m",
            "15m": "candle15m",
            "30m": "candle30m",
            "1h": "candle1H",
            "4h": "candle4H",
            "1d": "candle1D",
            "1D": "candle1D",
        }
        ch = tf_map.get(timeframe.lower(), "candle1H")
        args = [{"channel": ch, "instId": symbol}]
        await self.public_client.subscribe(args)
        logger.info(f"已为标的 {symbol} 注册 {ch} K 线 WebSocket 订阅")

    async def subscribe_private_channels(self, inst_type: str = "SWAP"):
        """快捷订阅私有通道：orders, positions, account"""
        args = [
            {"channel": "orders", "instType": inst_type},
            {"channel": "positions", "instType": inst_type},
            {"channel": "account"}
        ]
        await self.private_client.subscribe(args)
        logger.info(f"已为产品类型 {inst_type} 注册私有账户与订单持仓 WebSocket 订阅")

    async def _on_public_message(self, msg: Dict[str, Any]):
        """解析公有行情消息并广播"""
        arg = msg.get("arg", {})
        channel = arg.get("channel", "")
        inst_id = arg.get("instId", "")
        data_list = msg.get("data", [])

        if not data_list:
            return

        now_ms = int(time.time() * 1000)

        # 1. Tickers 频道
        if channel == "tickers":
            for item in data_list:
                ticker = TickerData(
                    symbol=item.get("instId", inst_id),
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
                self.latest_tickers[ticker.symbol] = ticker

                # 驱动模拟操盘沙盒实时盯市与 TP/SL 触发
                try:
                    from paper_trading import paper_engine
                    triggered_events = paper_engine.update_market_price(ticker.symbol, ticker.last_price)
                    if paper_engine.positions or triggered_events:
                        await self.broadcast_to_subscribers(WSPushPayload[List[PositionModel]](
                            channel="positions",
                            event=WSServerEvent.SNAPSHOT,
                            data=paper_engine.get_positions(),
                            timestamp=now_ms
                        ))
                        await self.broadcast_to_subscribers(WSPushPayload[AccountBalanceModel](
                            channel="account",
                            event=WSServerEvent.UPDATE,
                            data=paper_engine.get_account_balance(),
                            timestamp=now_ms
                        ))
                except Exception:
                    pass


                push_msg = WSPushPayload[TickerData](
                    channel=f"ticker:{ticker.symbol}",
                    event=WSServerEvent.UPDATE,
                    data=ticker,
                    timestamp=now_ms,
                    arg=arg
                )
                await self.broadcast_to_subscribers(push_msg)

        # 2. 深度盘口频道 (books / books5)
        elif channel in ("books", "books5", "bbo-tbt"):
            for item in data_list:
                bids_raw = item.get("bids", [])
                asks_raw = item.get("asks", [])
                bids = [
                    OrderBookLevel(price=float(b[0]), size=float(b[1]), order_count=int(b[3]) if len(b) > 3 else 1)
                    for b in bids_raw
                ]
                asks = [
                    OrderBookLevel(price=float(a[0]), size=float(a[1]), order_count=int(a[3]) if len(a) > 3 else 1)
                    for a in asks_raw
                ]
                depth_obj = OrderBookDepth(
                    symbol=inst_id,
                    timestamp=int(item.get("ts", now_ms)),
                    bids=bids,
                    asks=asks
                )
                self.latest_depths[inst_id] = depth_obj

                push_msg = WSPushPayload[OrderBookDepth](
                    channel=f"depth20:{inst_id}",
                    event=WSServerEvent.UPDATE,
                    data=depth_obj,
                    timestamp=now_ms,
                    arg=arg
                )
                await self.broadcast_to_subscribers(push_msg)

        # 3. K 线频道 (candle*)
        elif channel.startswith("candle"):
            timeframe_code = channel.replace("candle", "").lower()  # 15m, 1h, 4h, 1d
            for k in data_list:
                # [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
                candle = KlineCandle(
                    timestamp=int(k[0]),
                    open=float(k[1]),
                    high=float(k[2]),
                    low=float(k[3]),
                    close=float(k[4]),
                    volume=float(k[5]),
                    vol_currency=float(k[6]) if len(k) > 6 else None,
                    is_closed=bool(int(k[8])) if len(k) > 8 else False
                )
                if inst_id not in self.latest_klines:
                    self.latest_klines[inst_id] = {}
                self.latest_klines[inst_id][timeframe_code] = candle

                push_msg = WSPushPayload[KlineCandle](
                    channel=f"kline:{inst_id}:{timeframe_code}",
                    event=WSServerEvent.UPDATE,
                    data=candle,
                    timestamp=now_ms,
                    arg=arg
                )
                await self.broadcast_to_subscribers(push_msg)

    async def _on_private_message(self, msg: Dict[str, Any]):
        """解析私有通道消息（订单、持仓、账户资产）并广播"""
        arg = msg.get("arg", {})
        channel = arg.get("channel", "")
        data_list = msg.get("data", [])

        if not data_list:
            return

        now_ms = int(time.time() * 1000)

        # 1. 订单更新 (orders)
        if channel == "orders":
            for ord_data in data_list:
                order_item = OrderModel(
                    client_order_id=ord_data.get("clOrdId", ""),
                    okx_order_id=ord_data.get("ordId", ""),
                    symbol=ord_data.get("instId", ""),
                    inst_type=ord_data.get("instType", "SWAP"),
                    side=ord_data.get("side", "buy"),
                    pos_side=ord_data.get("posSide", "net"),
                    order_type=ord_data.get("ordType", "limit"),
                    price=float(ord_data.get("px", 0.0)) if ord_data.get("px") else None,
                    sz=float(ord_data.get("sz", 0.0)),
                    notional_usdt=float(ord_data.get("notionalUsd", 0.0)) if ord_data.get("notionalUsd") else 0.0,
                    filled_sz=float(ord_data.get("accFillSz", 0.0)),
                    avg_price=float(ord_data.get("avgPx", 0.0)) if ord_data.get("avgPx") else 0.0,
                    state=ord_data.get("state", "live"),
                    leverage=int(ord_data.get("lever", 1)) if ord_data.get("lever") else 1,
                    fee=float(ord_data.get("fee", 0.0)) if ord_data.get("fee") else 0.0,
                    fee_currency=ord_data.get("feeCcy", "USDT"),
                    pnl=float(ord_data.get("pnl", 0.0)) if ord_data.get("pnl") else 0.0,
                    tp_trigger_price=float(ord_data.get("tpTriggerPx", 0.0)) if ord_data.get("tpTriggerPx") else None,
                    sl_trigger_price=float(ord_data.get("slTriggerPx", 0.0)) if ord_data.get("slTriggerPx") else None,
                    created_at=int(ord_data.get("cTime", now_ms)),
                    updated_at=int(ord_data.get("uTime", now_ms))
                )
                self.latest_orders[order_item.client_order_id] = order_item

                push_msg = WSPushPayload[OrderModel](
                    channel="orders",
                    event=WSServerEvent.UPDATE,
                    data=order_item,
                    timestamp=now_ms,
                    arg=arg
                )
                await self.broadcast_to_subscribers(push_msg)

        # 2. 持仓更新 (positions)
        elif channel == "positions":
            pos_list: List[PositionModel] = []
            for p in data_list:
                pos_model = PositionModel(
                    pos_id=p.get("posId", ""),
                    symbol=p.get("instId", ""),
                    inst_type=p.get("instType", "SWAP"),
                    pos_side=p.get("posSide", "net"),
                    pos=float(p.get("pos", 0.0)),
                    avail_pos=float(p.get("availPos", 0.0)) if p.get("availPos") else 0.0,
                    avg_px=float(p.get("avgPx", 0.0)) if p.get("avgPx") else 0.0,
                    mark_px=float(p.get("markPx", 0.0)) if p.get("markPx") else 0.0,
                    liq_px=float(p.get("liqPx", 0.0)) if p.get("liqPx") else None,
                    margin_ratio=float(p.get("mgnRatio", 0.0)) if p.get("mgnRatio") else 0.0,
                    maint_margin_ratio=float(p.get("mmr", 0.01)) if p.get("mmr") else 0.01,
                    leverage=int(p.get("lever", 1)) if p.get("lever") else 1,
                    notional_usd=float(p.get("notionalUsd", 0.0)) if p.get("notionalUsd") else 0.0,
                    upl=float(p.get("upl", 0.0)) if p.get("upl") else 0.0,
                    upl_ratio=float(p.get("uplRatio", 0.0)) if p.get("uplRatio") else 0.0,
                    realized_pnl=float(p.get("realizedPnl", 0.0)) if p.get("realizedPnl") else 0.0,
                    margin=float(p.get("margin", 0.0)) if p.get("margin") else 0.0,
                    last_synced_at=int(p.get("uTime", now_ms))
                )
                self.latest_positions[f"{pos_model.symbol}:{pos_model.pos_side}"] = pos_model
                pos_list.append(pos_model)

            push_msg = WSPushPayload[List[PositionModel]](
                channel="positions",
                event=WSServerEvent.UPDATE,
                data=pos_list,
                timestamp=now_ms,
                arg=arg
            )
            await self.broadcast_to_subscribers(push_msg)

    def get_ticker_snapshot(self, symbol: str) -> Optional[TickerData]:
        """获取指定标的的最新 Ticker 内存快照"""
        return self.latest_tickers.get(symbol)

    def get_depth_snapshot(self, symbol: str) -> Optional[OrderBookDepth]:
        """获取指定标的的最新盘口深度内存快照"""
        return self.latest_depths.get(symbol)

    def get_position_snapshot(self, symbol: str, pos_side: str = "net") -> Optional[PositionModel]:
        """获取指定标的和方向的持仓内存快照"""
        key = f"{symbol}:{pos_side}"
        return self.latest_positions.get(key)


# 单例 WS 管理器实例
ws_manager = OKXWSManager(is_live_mode=False)

