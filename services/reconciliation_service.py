"""
OKX-Dog 个人量化智能交易系统 - 60秒常驻自动化对账中枢 (Reconciliation Engine)
模块: okx-dog-backend/services/reconciliation_service.py
角色: 后端架构师 (Backend Architect) / 交易风控安全审计师 (Trading Security Auditor)

核心职责:
1. 确立交易所为唯一权威事实源 (Remote SSOT)
2. 每 60 秒常驻静默核对远端持仓、挂单与资金 vs 本地账本
3. 发现持仓漂移时自动执行闭环自愈：
   - 外部注入开仓自动接管并纳入风控跟踪
   - 远端已平仓/强平/止损自动注销本地记录并同步损失
4. 远端真实累计亏损超限自动触发硬风控单日熔断
5. 维护全生命周期对账健康状态与历史流水供前端大屏展示
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("okx_dog.reconciliation")


class ReconciliationService:
    """自动化对账与状态自愈中枢"""

    def __init__(self, rest_client, paper_engine, auto_pilot_manager, risk_controller):
        self.rest_client = rest_client
        self.paper_engine = paper_engine
        self.auto_pilot_manager = auto_pilot_manager
        self.risk_controller = risk_controller

        self.is_running: bool = False
        self.last_reconciled_at: Optional[int] = None
        self.is_consistent: bool = True
        self.total_reconcile_runs: int = 0
        self.total_drift_incidents: int = 0
        self.last_drifts: List[str] = []
        self.reconciliation_history: List[Dict[str, Any]] = []

        self._lock = asyncio.Lock()

    async def reconcile_once(self) -> Dict[str, Any]:
        """执行单次全量对账与自愈同步"""
        now_ms = int(time.time() * 1000)
        self.total_reconcile_runs += 1
        drifts: List[str] = []
        heal_actions: List[str] = []

        async with self._lock:
            is_live = getattr(self.rest_client, "is_live_mode", False) and bool(getattr(self.rest_client, "api_key", None))

            if is_live:
                try:
                    # 1. 从 OKX 远端拉取权威事实
                    remote_positions = await self.rest_client.get_positions(inst_type="SWAP")
                    remote_balance = await self.rest_client.get_account_balance()
                    remote_orders = await self.rest_client.get_pending_orders(inst_type="SWAP")

                    # 构建远端有效持仓字典 (排除持仓量为 0 的记录)
                    remote_pos_map = {
                        p.symbol: p for p in remote_positions if p.pos != 0
                    }

                    # 本地自动操盘状态
                    live_state = self.auto_pilot_manager.live_state

                    # 2. 校验持仓一致性
                    # 2.1 检查远端持仓 -> 本地状态
                    for sym, r_pos in remote_pos_map.items():
                        local_scale = live_state.position_scale_records.get(sym, 0)
                        if local_scale <= 0:
                            # 远端有持仓，本地未记录（用户在 App 手动开仓或外部介入）
                            drift_msg = f"标的 {sym} 远端持有 {r_pos.pos_side.value.upper() if hasattr(r_pos.pos_side, 'value') else r_pos.pos_side} {r_pos.pos} 币，本地操盘账本未登记"
                            drifts.append(drift_msg)
                            
                            # 自动接管自愈
                            live_state.position_scale_records[sym] = 1
                            live_state.position_peaks[sym] = {
                                "peak_price": r_pos.last_price or r_pos.avg_px or 0.0,
                                "peak_profit_ratio": 0.0
                            }
                            heal_msg = f"本地已自动接管并登记标的 {sym} 外部持仓，纳入保本与移动止损巡检池"
                            heal_actions.append(heal_msg)
                            logger.warning(f"【对账中枢-自愈】{heal_msg}")

                    # 2.2 检查本地状态 -> 远端持仓
                    current_local_symbols = list(live_state.position_scale_records.keys())
                    for sym in current_local_symbols:
                        if sym not in remote_pos_map:
                            # 本地记录仍在持仓，但远端已无持仓（交易所强平、被动止损成交或 App 手动全平）
                            drift_msg = f"标的 {sym} 本地记录处于持仓中，但 OKX 远端持仓已归零"
                            drifts.append(drift_msg)

                            # 自动注销自愈
                            live_state.position_scale_records.pop(sym, None)
                            live_state.last_closed_timestamps[sym] = now_ms
                            heal_msg = f"本地已自动注销标的 {sym} 仓位记录，触发平仓冷静期"
                            heal_actions.append(heal_msg)
                            logger.warning(f"【对账中枢-自愈】{heal_msg}")

                    # 3. 校验资金与当日亏损一致性
                    if remote_balance and hasattr(remote_balance, "today_realized_pnl_usdt"):
                        real_pnl = float(remote_balance.today_realized_pnl_usdt)
                        if real_pnl < 0:
                            real_loss = abs(real_pnl)
                            if real_loss > self.risk_controller.today_realized_loss:
                                diff = real_loss - self.risk_controller.today_realized_loss
                                drifts.append(f"远端累计亏损 ({real_loss:.2f} USDT) 大于本地风控统计 ({self.risk_controller.today_realized_loss:.2f} USDT)，存在外部磨损偏差: {diff:.2f} USDT")
                                self.risk_controller.today_realized_loss = real_loss
                                heal_actions.append(f"硬风控已同步更新当日累计亏损至 {real_loss:.2f} USDT")
                                
                                # 触发熔断检查
                                if real_loss >= self.risk_controller.rules.max_daily_loss_usdt:
                                    self.risk_controller.trigger_daily_loss_circuit_breaker(
                                        real_loss,
                                        f"对账中枢同步 OKX 远端当日实际累计亏损 ({real_loss:.2f} USDT) 触发硬风控熔断"
                                    )

                except Exception as live_err:
                    err_msg = f"实盘对账异常: {str(live_err)}"
                    logger.error(err_msg, exc_info=True)
                    drifts.append(err_msg)

            else:
                # 模拟盘状态自检
                try:
                    paper_bal = self.paper_engine.get_account_balance()
                    paper_pos = self.paper_engine.get_positions(inst_type="SWAP")
                    paper_state = self.auto_pilot_manager.paper_state
                    paper_syms = {p.symbol for p in paper_pos if p.pos != 0}
                    local_syms = set(paper_state.position_scale_records.keys())

                    diff_syms = paper_syms.symmetric_difference(local_syms)
                    if diff_syms:
                        drifts.append(f"模拟盘撮合引擎与操盘记录存在标的差异: {diff_syms}")
                        # 自动以模拟撮合引擎为准
                        paper_state.position_scale_records = {s: 1 for s in paper_syms}
                        heal_actions.append("模拟盘账本已以 PaperEngine 底层持仓为准自动对齐")
                except Exception as paper_err:
                    drifts.append(f"模拟盘自检异常: {str(paper_err)}")

        # 4. 汇总对账结论
        is_synced = len(drifts) == 0
        if not is_synced:
            self.total_drift_incidents += 1

        self.last_reconciled_at = now_ms
        self.is_consistent = is_synced
        self.last_drifts = drifts

        record = {
            "timestamp": now_ms,
            "status": "SYNCED" if is_synced else ("HEALED" if heal_actions else "DRIFT_DETECTED"),
            "drift_count": len(drifts),
            "drifts": drifts,
            "heal_actions": heal_actions,
            "is_live_mode": is_live
        }

        self.reconciliation_history.append(record)
        if len(self.reconciliation_history) > 50:
            self.reconciliation_history.pop(0)

        if not is_synced:
            logger.warning(f"【自动化对账完成】检测到 {len(drifts)} 项偏差，已实施 {len(heal_actions)} 项自愈动作: {drifts}")
        else:
            logger.debug(f"【自动化对账完成】账本 100% 吻合对齐 (时间戳={now_ms})")

        return record

    def get_status(self) -> Dict[str, Any]:
        """获取对账中枢最新实时健康状态"""
        return {
            "is_running": self.is_running,
            "last_reconciled_at": self.last_reconciled_at,
            "is_consistent": self.is_consistent,
            "total_reconcile_runs": self.total_reconcile_runs,
            "total_drift_incidents": self.total_drift_incidents,
            "last_drifts": self.last_drifts,
            "recent_history": self.reconciliation_history[-10:] if self.reconciliation_history else []
        }

    async def run_forever(self):
        """后台常驻守护协程 (每 60 秒触发)"""
        self.is_running = True
        logger.info("【自动化对账中枢】60秒常驻静默巡检守护协程已启动")
        while self.is_running:
            try:
                await asyncio.sleep(60)
                await self.reconcile_once()
            except asyncio.CancelledError:
                self.is_running = False
                logger.info("【自动化对账中枢】守护协程接收到终止信号")
                break
            except Exception as e:
                logger.error(f"【自动化对账中枢】巡检周期发生未捕获异常: {e}", exc_info=True)
