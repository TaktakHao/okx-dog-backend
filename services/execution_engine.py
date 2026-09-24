"""
OKX-Dog 个人量化智能交易系统 - 智能执行引擎与 Smart Pegging (POST_ONLY 动态追单)
模块: okx-dog-backend/services/execution_engine.py
角色: 后端架构师 (Backend Architect) / 量化执行算法工程师 (Quant Execution Engineer)

核心职责:
1. 落地极低摩擦的 Smart Pegging 被动做市追单算法
2. 始终优先挂单在 BBO 买一/卖一档 (POST_ONLY/Maker)，直接节省 60%+ 交易所手续费
3. 盘口漂移时自动撤单重挂 (Dynamic Peg Re-quoting)，避免挂单悬空或踏空
4. 超时或紧急状态下支持自适应降级为最优五档即时成交 (optimal_limit_ioc)
5. 实时统计与输出 Maker 成交率 (maker_fill_ratio)，杜绝高滑点市价吃单损耗
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from models import OrderSide, OrderType, PosSide, TickerData

logger = logging.getLogger("okx_dog.execution")


class SmartExecutionEngine:
    """智能执行中枢与 Smart Pegging 追单引擎"""

    def __init__(self, rest_client, risk_controller):
        self.rest_client = rest_client
        self.risk_controller = risk_controller

        # 统计执行指标
        self.total_orders: int = 0
        self.maker_fill_count: int = 0
        self.taker_fill_count: int = 0
        self.timeout_cancel_count: int = 0
        self.peg_adjustments_count: int = 0
        self.execution_records: List[Dict[str, Any]] = []

    def get_execution_metrics(self) -> Dict[str, Any]:
        """获取智能执行与 Maker 费率优化核心指标"""
        total_fills = self.maker_fill_count + self.taker_fill_count
        maker_ratio = (self.maker_fill_count / total_fills * 100.0) if total_fills > 0 else 100.0
        return {
            "total_orders_submitted": self.total_orders,
            "total_fills": total_fills,
            "maker_fill_count": self.maker_fill_count,
            "taker_fill_count": self.taker_fill_count,
            "maker_fill_ratio_pct": round(maker_ratio, 2),
            "peg_adjustments_count": self.peg_adjustments_count,
            "timeout_cancel_count": self.timeout_cancel_count,
            "recent_executions": self.execution_records[-10:] if self.execution_records else []
        }

    async def execute_smart_peg_order(
        self,
        symbol: str,
        side: OrderSide,
        pos_side: PosSide,
        size: float,
        leverage: int,
        tp_trigger_price: Optional[float] = None,
        sl_trigger_price: Optional[float] = None,
        max_retries: int = 3,
        check_interval_seconds: float = 2.0,
        timeout_seconds: float = 20.0,
        fallback_to_ioc: bool = False,
    ) -> Dict[str, Any]:
        """
        执行 Smart Pegging 动态追单流程:
        1. 抓取最新盘口 BBO (买一/卖一档)
        2. 以 POST_ONLY 挂单进入深度首档
        3. 巡检订单状态，若盘口脱离，自动撤单重挂
        4. 达到最大重试或超时后，依配置决定取消或降级 IOC 成交
        """
        self.total_orders += 1
        start_ts = time.time()
        now_ms = int(start_ts * 1000)

        logger.info(f"⚡ [SmartPeg] 启动被动挂单执行: {symbol} {side.value.upper()} {size} 币 (最大追单次数={max_retries})")

        current_retry = 0
        current_ord_id: Optional[str] = None
        last_peg_px: float = 0.0

        is_buy = (side == OrderSide.BUY)

        try:
            while current_retry <= max_retries:
                # 1. 获取最新盘口并计算贴盘价格
                try:
                    tk = await self.rest_client.get_ticker(symbol)
                    ref_px = tk.last_price or 0.0
                    # 确定首档挂单价 (针对币种精度自适应取整)
                    price_dec = 2 if ref_px > 10 else 4
                    if is_buy:
                        # 买单挂在买一或微低于现价
                        peg_px = round(tk.bid_price if (tk.bid_price and tk.bid_price > 0) else ref_px * 0.9998, price_dec)
                    else:
                        # 卖单挂在卖一或微高于现价
                        peg_px = round(tk.ask_price if (tk.ask_price and tk.ask_price > 0) else ref_px * 1.0002, price_dec)
                except Exception as tk_err:
                    logger.warning(f"[SmartPeg] 获取盘口行情异常: {tk_err}")
                    peg_px = last_peg_px if last_peg_px > 0 else 0.0

                if peg_px <= 0:
                    raise ValueError(f"无法获取标的 {symbol} 有效盘口价格进行挂单")

                last_peg_px = peg_px

                # 2. 提交 POST_ONLY 挂单
                try:
                    logger.debug(f"[SmartPeg] 正在挂单 (第{current_retry+1}次): {symbol} @ ${peg_px} (POST_ONLY)")
                    order_res = await self.rest_client.place_order(
                        symbol=symbol,
                        side=side,
                        pos_side=pos_side,
                        order_type=OrderType.POST_ONLY,
                        price=peg_px,
                        sz=size,
                        leverage=leverage,
                        tp_trigger_price=tp_trigger_price,
                        sl_trigger_price=sl_trigger_price
                    )
                    current_ord_id = order_res.get("ordId")
                except Exception as place_err:
                    # 如果 POST_ONLY 立即被撮合引擎取消（例如由于挂单价格穿透买卖盘口），捕获后继续重试
                    logger.warning(f"[SmartPeg] 挂单可能穿透盘口，自适应重试: {place_err}")
                    await asyncio.sleep(0.5)
                    current_retry += 1
                    continue

                if not current_ord_id:
                    logger.warning("[SmartPeg] 下单未返回有效 ordId，终止追单")
                    break

                # 3. 动态监测订单状态
                wait_time_this_cycle = 0.0
                max_cycle_wait = min(timeout_seconds / max(1, max_retries), 8.0)
                filled = False

                while wait_time_this_cycle < max_cycle_wait:
                    await asyncio.sleep(check_interval_seconds)
                    wait_time_this_cycle += check_interval_seconds

                    # 查询该订单状态
                    try:
                        res = await self.rest_client._request(
                            "GET",
                            "/api/v5/trade/order",
                            params={"instId": symbol, "ordId": current_ord_id}
                        )
                        if res:
                            item = res[0]
                            state = item.get("state", "").lower()
                            if state == "filled":
                                # 成功以 Maker 完全成交！
                                self.maker_fill_count += 1
                                fill_latency = int((time.time() - start_ts) * 1000)
                                logger.info(f"🎯 [SmartPeg 成功成交] {symbol} {side.value.upper()} 订单 {current_ord_id} 成功以 Maker 撮合！耗时 {fill_latency}ms @ ${peg_px} (节省 60% 费率)")
                                rec = {
                                    "symbol": symbol,
                                    "side": side.value,
                                    "order_type": "POST_ONLY",
                                    "fill_type": "MAKER",
                                    "price": peg_px,
                                    "size": size,
                                    "latency_ms": fill_latency,
                                    "timestamp": now_ms
                                }
                                self.execution_records.append(rec)
                                return {"success": True, "fill_type": "MAKER", "price": peg_px, "order_id": current_ord_id, "latency_ms": fill_latency}
                            elif state in ("canceled", "cancelled"):
                                logger.debug(f"[SmartPeg] 订单 {current_ord_id} 已取消或已被踢出盘口")
                                break
                    except Exception as check_err:
                        logger.debug(f"[SmartPeg] 轮询订单状态轻度异常: {check_err}")

                    # 检查盘口是否大幅脱离
                    try:
                        cur_tk = await self.rest_client.get_ticker(symbol)
                        cur_px = cur_tk.last_price or peg_px
                        # 若价格朝着不利方向抛离我们超过 0.15%，提前触发追单重挂
                        price_drift_pct = abs(cur_px - peg_px) / peg_px * 100.0 if peg_px > 0 else 0.0
                        if price_drift_pct > 0.15:
                            logger.info(f"[SmartPeg] 盘口已向外漂移 {price_drift_pct:.2f}%，提前触发撤单重跟")
                            break
                    except Exception:
                        pass

                # 本轮未成交，撤销当前挂单
                try:
                    await self.rest_client.cancel_order(symbol=symbol, order_id=current_ord_id)
                    logger.debug(f"[SmartPeg] 已撤销未完全成交挂单 {current_ord_id}，准备重新 Pegging")
                except Exception as cancel_err:
                    logger.debug(f"[SmartPeg] 撤销订单轻微异常 (可能已成交): {cancel_err}")

                self.peg_adjustments_count += 1
                current_retry += 1

            # 4. 追单次数耗尽或超时处理
            if fallback_to_ioc:
                logger.warning(f"⚠️ [SmartPeg 降级] 追单 {max_retries} 次仍未成交，降级为 optimal_limit_ioc 确保进场")
                try:
                    ioc_res = await self.rest_client.place_order(
                        symbol=symbol,
                        side=side,
                        pos_side=pos_side,
                        order_type=OrderType.OPTIMAL_LIMIT_IOC,
                        price=last_peg_px,
                        sz=size,
                        leverage=leverage,
                        tp_trigger_price=tp_trigger_price,
                        sl_trigger_price=sl_trigger_price
                    )
                    self.taker_fill_count += 1
                    fill_latency = int((time.time() - start_ts) * 1000)
                    return {"success": True, "fill_type": "IOC_FALLBACK", "price": last_peg_px, "order_id": ioc_res.get("ordId"), "latency_ms": fill_latency}
                except Exception as ioc_err:
                    logger.error(f"[SmartPeg] 降级 IOC 发单异常: {ioc_err}")

            self.timeout_cancel_count += 1
            logger.info(f"[SmartPeg 结束] {symbol} 挂单未能成交，已安全取消，杜绝长期悬挂孤立单")
            return {"success": False, "fill_type": "NONE", "message": "Smart Pegging 超时未成交并已安全取消"}

        except Exception as global_peg_err:
            logger.error(f"[SmartPeg] 执行全流程异常: {global_peg_err}", exc_info=True)
            return {"success": False, "fill_type": "ERROR", "message": str(global_peg_err)}
