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
        max_retries: int = 2,
        check_interval_seconds: float = 1.5,
        timeout_seconds: float = 10.0,
        fallback_to_ioc: bool = True,
    ) -> Dict[str, Any]:
        """
        执行 Smart Pegging 动态追单流程:
        1. 抓取最新盘口 BBO (买一/卖一档)
        2. 以 POST_ONLY 挂单进入深度首档
        3. 巡检订单状态，若盘口脱离，自动撤单重挂
        4. 达到最大重试或超时后，依配置自动降级为市价/IOC 确保实盘可靠进场
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
                # 1. 获取最新盘口并按 OKX tickSz 精度计算贴盘价格
                try:
                    tk = await self.rest_client.get_ticker(symbol)
                    ref_px = tk.last_price or 0.0
                    _, tick_sz, _, _ = self.rest_client.get_instrument_spec(symbol)
                    if is_buy:
                        raw_peg = tk.bid_price if (tk.bid_price and tk.bid_price > 0) else (ref_px - tick_sz)
                    else:
                        raw_peg = tk.ask_price if (tk.ask_price and tk.ask_price > 0) else (ref_px + tick_sz)
                    peg_px = float(self.rest_client._format_order_px(symbol, raw_peg))
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

            # 4. 追单次数耗尽或超时处理 -> 自动降级市价单 (MARKET) 确保实盘信号不漏单
            if fallback_to_ioc:
                logger.warning(f"⚠️ [SmartPeg 降级] 追单 {max_retries} 次仍未以 Maker 成交，自动降级为 MARKET 市价单确保可靠进场")
                try:
                    ioc_res = await self.rest_client.place_order(
                        symbol=symbol,
                        side=side,
                        pos_side=pos_side,
                        order_type=OrderType.MARKET,
                        price=None,
                        sz=size,
                        leverage=leverage,
                        tp_trigger_price=tp_trigger_price,
                        sl_trigger_price=sl_trigger_price
                    )
                    self.taker_fill_count += 1
                    fill_latency = int((time.time() - start_ts) * 1000)
                    return {"success": True, "fill_type": "MARKET_FALLBACK", "price": last_peg_px, "order_id": ioc_res.get("ordId"), "latency_ms": fill_latency}
                except Exception as ioc_err:
                    logger.error(f"[SmartPeg] 降级市价发单异常: {ioc_err}")

            self.timeout_cancel_count += 1
            logger.info(f"[SmartPeg 结束] {symbol} 挂单未能成交，已安全取消，杜绝长期悬挂孤立单")
            return {"success": False, "fill_type": "NONE", "message": "Smart Pegging 超时未成交并已安全取消"}

        except Exception as global_peg_err:
            logger.error(f"[SmartPeg] 执行全流程异常: {global_peg_err}", exc_info=True)
            return {"success": False, "fill_type": "ERROR", "message": str(global_peg_err)}

    async def execute_atomic_dual_leg(
        self,
        swap_symbol: str,
        spot_symbol: str,
        action: str,  # "OPEN_CARRY" | "CLOSE_CARRY"
        coin_size: float,
        max_drift_pct: float = 0.15,
        is_paper: bool = True,
    ) -> Dict[str, Any]:
        """
        Stage 2B 原子化双腿防跛腿执行器 (Atomic Dual-Leg Executor for Cash-and-Carry):
        - OPEN_CARRY: 卖出 1x 永续合约 (SHORT) + 买入等量现货 (SPOT) -> Delta 中性建仓
        - CLOSE_CARRY: 买入平空永续合约 + 卖出等量现货 -> Delta 中性平仓
        防跛腿状态机:
          1. 优先以 POST_ONLY Maker 挂单执行 Leg 1 (永续腿)，未成交直接安全撤单 (零敞口)；
          2. Leg 1 确认成交后，立即对 Leg 2 (现货腿) 执行等量对冲 (先试 3 秒 Maker 贴盘，漂移/超时自动市价补齐锁死 Delta=0)；
          3. 若 Leg 2 极端异常失败，立即市价回滚平掉 Leg 1 并推送 Telegram 紧急告警，杜绝单边敞口裸露。
        """
        if coin_size <= 0:
            raise ValueError(f"双腿执行币数必须大于 0: {coin_size}")

        action_up = action.upper()
        if action_up not in ("OPEN_CARRY", "CLOSE_CARRY"):
            raise ValueError(f"不支持的双腿动作类型: {action}")

        start_ts = time.time()
        swap_side = OrderSide.SELL if action_up == "OPEN_CARRY" else OrderSide.BUY
        spot_side = OrderSide.BUY if action_up == "OPEN_CARRY" else OrderSide.SELL
        is_close = (action_up == "CLOSE_CARRY")

        if is_paper:
            # 影子/模拟盘严格按实盘公共盘口价格与双腿 Maker 费率 (2bps/腿) 核算
            swap_tk = await self.rest_client.get_ticker(swap_symbol)
            spot_tk = await self.rest_client.get_ticker(spot_symbol)
            swap_px = float(swap_tk.last_price or 0.0)
            spot_px = float(spot_tk.last_price or 0.0)
            if swap_px <= 0 or spot_px <= 0:
                raise ValueError(f"无法获取有效双腿公共盘口价: {swap_symbol}={swap_px}, {spot_symbol}={spot_px}")
            basis_bps = ((swap_px - spot_px) / spot_px) * 10000.0
            notional = spot_px * coin_size
            est_fee = round(notional * 0.0004, 4)  # 双腿 Maker 0.02% * 2
            return {
                "success": True,
                "mode": "PAPER_SHADOW",
                "action": action_up,
                "swap_symbol": swap_symbol,
                "spot_symbol": spot_symbol,
                "coin_size": coin_size,
                "swap_fill_px": swap_px,
                "spot_fill_px": spot_px,
                "basis_bps": round(basis_bps, 2),
                "notional_usdt": round(notional, 2),
                "est_fee_usdt": est_fee,
                "hedged_delta": 0.0,
                "latency_ms": int((time.time() - start_ts) * 1000),
            }

        # 实盘原子化双腿流程
        logger.info(f"⚡ [AtomicDualLeg] 启动实盘 {action_up}: {swap_symbol} + {spot_symbol}, 数量={coin_size}")

        # Step 1: 永续腿先挂 Maker (不强行市价降级，未成交则零敞口退出)
        leg1_res = await self.execute_smart_peg_order(
            symbol=swap_symbol,
            side=swap_side,
            pos_side=PosSide.SHORT,
            size=coin_size,
            leverage=1,
            max_retries=2,
            check_interval_seconds=1.0,
            timeout_seconds=6.0,
            fallback_to_ioc=False,
        )
        if not leg1_res.get("success"):
            return {
                "success": False,
                "stage": "LEG1_NOT_FILLED",
                "action": action_up,
                "hedged_delta": 0.0,
                "message": f"永续腿 {swap_symbol} Maker 未成交已安全撤单，零单边敞口",
            }

        # Step 2: 永续腿已成交，立即执行等量现货腿对冲 (3 秒快速 Maker + 超时自动市价锁定 Delta=0)
        try:
            leg2_res = await self.execute_smart_peg_order(
                symbol=spot_symbol,
                side=spot_side,
                pos_side=PosSide.NET,
                size=coin_size,
                leverage=1,
                max_retries=1,
                check_interval_seconds=1.0,
                timeout_seconds=3.0,
                fallback_to_ioc=True,
            )
            if not leg2_res.get("success"):
                raise RuntimeError(leg2_res.get("message", "现货腿对冲未能成交"))
        except Exception as leg2_err:
            # Step 3: 紧急回滚 Leg 1，彻底防止跛腿裸露
            rollback_side = OrderSide.BUY if swap_side == OrderSide.SELL else OrderSide.SELL
            alert_msg = (
                f"🚨【原子双腿跛腿回滚】{swap_symbol} 已成交但现货腿 {spot_symbol} 对冲失败 ({leg2_err})！"
                f"正在立即市价回滚永续腿以消除单边敞口！"
            )
            logger.critical(alert_msg)
            try:
                await self.rest_client.place_order(
                    symbol=swap_symbol,
                    side=rollback_side,
                    pos_side=PosSide.SHORT,
                    order_type=OrderType.MARKET,
                    sz=coin_size,
                    leverage=1,
                    reduce_only=(not is_close),
                )
            except Exception as rb_err:
                logger.critical(f"🚨【极高危】双腿回滚失败，请立即人工接管 {swap_symbol}: {rb_err}")
                alert_msg += f"\n❌ 自动回滚异常: {rb_err}，请立即人工平仓！"
            try:
                from state import telegram_bot_service
                asyncio.create_task(telegram_bot_service.send_message(alert_msg))
            except Exception:
                pass
            return {
                "success": False,
                "stage": "LEG2_FAILED_ROLLED_BACK",
                "action": action_up,
                "message": str(leg2_err),
            }

        swap_px = float(leg1_res.get("price") or 0.0)
        spot_px = float(leg2_res.get("price") or 0.0)
        basis_bps = ((swap_px - spot_px) / spot_px * 10000.0) if spot_px > 0 else 0.0
        return {
            "success": True,
            "mode": "LIVE_ATOMIC",
            "action": action_up,
            "swap_symbol": swap_symbol,
            "spot_symbol": spot_symbol,
            "coin_size": coin_size,
            "swap_fill_px": swap_px,
            "spot_fill_px": spot_px,
            "swap_fill_type": leg1_res.get("fill_type"),
            "spot_fill_type": leg2_res.get("fill_type"),
            "basis_bps": round(basis_bps, 2),
            "hedged_delta": 0.0,
            "latency_ms": int((time.time() - start_ts) * 1000),
        }

    @staticmethod
    def check_cross_margin_health(
        total_equity_usdt: float,
        carry_notional_usdt: float,
        margin_ratio: float,
        min_safe_margin_ratio: float = 3.0,
    ) -> Dict[str, Any]:
        """
        跨币种保证金安全健康度巡检：
        在 1x 期现套利模式下，当有效杠杆 > 1.2x 或交易所维持保证金率逼近警戒线时发出减仓指令。
        """
        effective_lev = (carry_notional_usdt / total_equity_usdt) if total_equity_usdt > 0 else 0.0
        is_healthy = True
        action = "OK"
        reason = "跨币种保证金安全健康"

        if margin_ratio > 0 and margin_ratio < min_safe_margin_ratio:
            is_healthy = False
            action = "DELEVERAGE_50PCT"
            reason = f"跨币种保证金率 ({margin_ratio:.2f}) 低于安全阈值 ({min_safe_margin_ratio:.2f})，触发等比例双腿减仓 50%"
        elif effective_lev > 1.25:
            is_healthy = False
            action = "CAP_NOTIONAL"
            reason = f"期现套利名义杠杆 ({effective_lev:.2f}x) 超过 1.25x 安全上限"

        return {
            "is_healthy": is_healthy,
            "recommended_action": action,
            "effective_leverage": round(effective_lev, 3),
            "margin_ratio": round(margin_ratio, 3),
            "reason": reason,
        }
