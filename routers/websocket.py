"""
OKX-Dog 个人量化智能交易系统 - WebSocket 实时数据流通道
模块: okx-dog-backend/routers/websocket.py
处理前端全双工 WebSocket 长连接、行情与订单持仓广播、心跳 ping/pong 以及 Antigravity 推理通道
"""

import asyncio
import json
import logging
import time
from typing import Any
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from models import (
    WSPongMessage,
    WSPushPayload,
    WSServerEvent,
)
from state import (
    antigravity_bridge,
    logger,
    ws_manager,
)

router = APIRouter(tags=["WebSocket 实时数据流"])


@router.websocket("/ws/v1/stream")
@router.websocket("/ws/v5/stream")
async def websocket_stream_endpoint(websocket: WebSocket):
    """
    前端与后端全双工 WebSocket 实时数据流通道
    - 毫秒级广播行情 (Ticker, Depth, Kline)、持仓 (Positions)、订单 (Orders)、风控事件 (Risk Events)
    - 响应客户端心跳 ping / pong
    - 动态解析前端订阅 channels 并自愈向 OKX WS 订阅
    """
    await websocket.accept()
    client_queue: asyncio.Queue[WSPushPayload] = asyncio.Queue(maxsize=1000)
    ws_manager.register_broadcast_subscriber(client_queue)
    logger.info("前端 WebSocket 客户端已建立连接")

    # 协程 1: 下行数据推送协程
    async def push_loop():
        try:
            while True:
                payload = await client_queue.get()
                if payload is None:
                    break
                await websocket.send_text(payload.model_dump_json(by_alias=True))
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        except Exception as e:
            logger.debug(f"WS 推送协程异常退出: {e}")

    # 协程 2: 上行数据接收与指令解析协程
    async def receive_loop():
        try:
            while True:
                raw_text = await websocket.receive_text()
                now_ms = int(time.time() * 1000)

                # 处理纯文本 ping
                if raw_text == "ping":
                    pong_msg = WSPongMessage(timestamp=now_ms)
                    await websocket.send_text(pong_msg.model_dump_json())
                    continue

                try:
                    data = json.loads(raw_text)
                except Exception:
                    continue

                action = data.get("action") or data.get("op")

                # 心跳 ping 包
                if action == "ping":
                    pong_msg = WSPongMessage(timestamp=now_ms)
                    await websocket.send_text(pong_msg.model_dump_json())

                # 订阅频道
                elif action == "subscribe":
                    channels = data.get("channels", [])
                    args = data.get("args", [])

                    # 解析标准 channel 格式，如 "ticker:BTC-USDT-SWAP"
                    for ch in channels:
                        parts = ch.split(":")
                        if len(parts) >= 2:
                            ch_type = parts[0]
                            symbol = parts[1]
                            # 动态注册至 OKX WebSocket
                            await ws_manager.subscribe_market_symbol(symbol)

                            # 立即回送当前最新快照
                            if ch_type == "ticker" and symbol in ws_manager.latest_tickers:
                                snap_payload = WSPushPayload(
                                    channel=ch,
                                    event=WSServerEvent.SNAPSHOT,
                                    data=ws_manager.latest_tickers[symbol],
                                    timestamp=now_ms
                                )
                                await websocket.send_text(snap_payload.model_dump_json(by_alias=True))
                            elif ch_type.startswith("depth") and symbol in ws_manager.latest_depths:
                                snap_payload = WSPushPayload(
                                    channel=ch,
                                    event=WSServerEvent.SNAPSHOT,
                                    data=ws_manager.latest_depths[symbol],
                                    timestamp=now_ms
                                )
                                await websocket.send_text(snap_payload.model_dump_json(by_alias=True))

                        # 私有频道快照
                        if ch == "positions" and ws_manager.latest_positions:
                            pos_list = list(ws_manager.latest_positions.values())
                            snap_payload = WSPushPayload(
                                channel="positions",
                                event=WSServerEvent.SNAPSHOT,
                                data=pos_list,
                                timestamp=now_ms
                            )
                            await websocket.send_text(snap_payload.model_dump_json(by_alias=True))

                        # 发送订阅成功确认
                        ack_payload = WSPushPayload(
                            channel=ch,
                            event=WSServerEvent.SUBSCRIBED,
                            data={"status": "subscribed", "channel": ch},
                            timestamp=now_ms
                        )
                        await websocket.send_text(ack_payload.model_dump_json(by_alias=True))

                    # 兼容 OKX 格式 args
                    if args:
                        await ws_manager.public_client.subscribe(args)

                # 鉴权指令
                elif action == "auth":
                    auth_ack = WSPushPayload(
                        channel="auth",
                        event=WSServerEvent.AUTH_SUCCESS,
                        data={"authenticated": True},
                        timestamp=now_ms
                    )
                    await websocket.send_text(auth_ack.model_dump_json(by_alias=True))

        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        except Exception as e:
            logger.debug(f"WS 接收协程异常: {e}")

    push_task = asyncio.create_task(push_loop())
    receive_task = asyncio.create_task(receive_loop())

    done, pending = await asyncio.wait(
        [push_task, receive_task],
        return_when=asyncio.FIRST_COMPLETED
    )
    for t in pending:
        t.cancel()

    ws_manager.unregister_broadcast_subscriber(client_queue)
    logger.info("前端 WebSocket 客户端连接已断开并释放队列")


@router.websocket("/ws/antigravity/stream")
async def websocket_antigravity_stream(websocket: WebSocket):
    """
    全双工 WebSocket Antigravity 推理通道
    """
    await websocket.accept()
    logger.info("[WebSocket:Antigravity] 客户端已成功建立流式连接")

    if not (antigravity_bridge and antigravity_bridge.is_available()):
        await websocket.send_json({"type": "error", "message": "Antigravity CLI 驱动未就绪"})
        await websocket.close()
        return

    try:
        while True:
            raw_msg = await websocket.receive_text()
            try:
                msg_data = json.loads(raw_msg)
            except json.JSONDecodeError:
                await websocket.send_json({"type": "error", "message": "非法的 JSON 数据"})
                continue

            prompt = msg_data.get("prompt")
            if not prompt:
                await websocket.send_json({"type": "error", "message": "prompt 不能为空"})
                continue

            schema = msg_data.get("schema")
            effort = msg_data.get("effort", "medium")
            model = msg_data.get("model")

            t0 = time.time()
            try:
                async for text_delta, think_delta in antigravity_bridge.generate_stream(
                    prompt=prompt,
                    response_schema=schema,
                    effort=effort,
                    model=model,
                ):
                    if text_delta or think_delta:
                        await websocket.send_json({
                            "type": "delta",
                            "text": text_delta,
                            "thinking": think_delta,
                        })

                latency_ms = int((time.time() - t0) * 1000)
                await websocket.send_json({
                    "type": "done",
                    "latency_ms": latency_ms,
                })
            except Exception as exc:
                logger.error(f"[WebSocket:Antigravity] 推理异常: {exc}")
                await websocket.send_json({"type": "error", "message": str(exc)})

    except WebSocketDisconnect:
        logger.info("[WebSocket:Antigravity] 客户端已断开")
    except Exception as exc:
        logger.warning(f"[WebSocket:Antigravity] 异常: {exc}")
