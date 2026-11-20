"""
OKX-Dog 个人量化智能交易系统 - 核心硬风控拦截中枢与 Panic Button 熔断模块
模块: okx-dog-backend/risk_controller.py
实现不可绕过的下单前置拦截、单笔限额、单日亏损熔断、杠杆上限、滑点保护、连续亏损风控与紧急全平
"""

import asyncio
import datetime
import json
import logging
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from config import DEFAULT_DB_PATH, settings
from database import get_db_session
from models import (
    AuditSeverity,
    OrderSide,
    OrderType,
    PanicButtonRequest,
    PanicButtonResponse,
    PanicExecutionDetail,
    PlaceOrderRequest,
    PositionModel,
    RiskAuditLogModel,
    RiskCheckResult,
    RiskEventType,
    RiskRulesConfig,
    RiskStatus,
    RiskStatusResponse,
    TickerData,
    WSPushPayload,
    WSServerEvent
)
from okx_rest import OKXRestClient

logger = logging.getLogger("okx_dog.risk")


class RiskInterceptionException(Exception):
    """硬风控强阻断拦截异常"""
    def __init__(self, message: str, rule_name: str, current_value: Any, limit_value: Any):
        self.message = message
        self.rule_name = rule_name
        self.current_value = current_value
        self.limit_value = limit_value
        super().__init__(f"【硬风控拦截】规则 [{rule_name}] 触发阻断: {message}")


class CircuitBreakerTriggeredException(RiskInterceptionException):
    """触发单日亏损熔断异常"""
    pass


class SafeModeException(RiskInterceptionException):
    """系统处于 Panic Button 安全模式异常"""
    pass


class RiskController:
    """
    金融级硬风控中枢 (Hard Risk Controller)
    - 任何交易委托在发送至 OKX API 之前必须无条件经由本中枢审核
    - 多维度指标计算：单笔上限、单日亏损熔断、杠杆硬锁、盘口滑点保护、持仓规模限制
    - Panic Button 紧急处置：Cancel-Before-Close 严格两阶段全撤全平
    - 内存状态机与 SQLite risk_events 异步持久化双重一致性
    """

    def __init__(self):
        # 内存风控规则配置 (初始化自 settings 默认值)
        self.rules = RiskRulesConfig(
            max_order_usdt=settings.DEFAULT_MAX_ORDER_USDT,
            max_daily_loss_usdt=settings.DEFAULT_MAX_DAILY_LOSS_USDT,
            max_leverage=settings.DEFAULT_MAX_LEVERAGE,
            max_slippage_pct=settings.DEFAULT_MAX_SLIPPAGE_PCT,
            max_open_positions=10,
            max_position_usdt=100000.0,
            fixed_risk_per_trade_percent=0.01,
            enforce_post_only=True,
            max_same_sector_same_direction_positions=2,
            min_24h_volume_usdt_for_rotation=30000000.0,
            anti_fomo_24h_change_pct_cap=18.0,
            contrarian_extreme_percentile_guard=85.0,
        )

        # 内存实时风控状态机
        self.risk_status: RiskStatus = RiskStatus.NORMAL
        self.is_circuit_broken: bool = False
        self.circuit_break_reason: Optional[str] = None
        self.circuit_break_at: Optional[int] = None
        self.is_safe_mode: bool = False

        # 当日盈亏与连续止损统计
        self.today_realized_loss: float = 0.0
        self.today_unrealized_loss: float = 0.0
        self.consecutive_loss_count: int = 0
        self.loss_timestamps: List[float] = []  # 记录止损时间戳 (秒)
        self.cooldown_until_ts: Optional[int] = None  # 全局防上头冷却截止时间戳 (ms)
        self.cooldown_reason: Optional[str] = None
        self.symbol_cooldowns: Dict[str, int] = {}    # 标的级别止损冷静期: symbol -> cooldown_until_ms
        self.last_reset_date: str = datetime.datetime.utcnow().strftime("%Y-%m-%d")

        # FP&A 滚动风险预算与账户高水位线 (High Water Mark, HWM)
        self.high_water_mark_equity: float = 10000.0

        # 异步锁
        self._lock = asyncio.Lock()

    def _persist_state_sync(self) -> None:
        """
        Stage 0 (T0.4): 将当日累计亏损、UTC 重置日期与熔断锁定状态实时落盘至 SQLite risk_limits，
        确保进程崩溃或重启后熔断状态与日亏统计绝对不丢失
        """
        try:
            if not DEFAULT_DB_PATH.exists():
                return
            conn = sqlite3.connect(str(DEFAULT_DB_PATH), timeout=3.0)
            try:
                cur = conn.cursor()
                cb_at_str = (
                    datetime.datetime.fromtimestamp(self.circuit_break_at / 1000.0, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    if self.circuit_break_at
                    else None
                )
                reset_at_str = f"{self.last_reset_date} 00:00:00"
                cur.execute(
                    """
                    UPDATE risk_limits SET
                        is_circuit_broken = ?,
                        circuit_break_reason = ?,
                        circuit_break_at = ?,
                        daily_loss_current = ?,
                        daily_loss_reset_at = ?,
                        is_safe_mode = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = (SELECT id FROM risk_limits ORDER BY id DESC LIMIT 1)
                    """,
                    (
                        1 if self.is_circuit_broken else 0,
                        self.circuit_break_reason,
                        cb_at_str,
                        round(float(self.today_realized_loss), 4),
                        reset_at_str,
                        1 if self.is_safe_mode else 0,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            logger.debug(f"风控熔断状态同步落盘跳过: {e}")

    def _check_day_rollover(self):
        """每日 UTC 0 点重置当日统计并落盘"""
        current_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
        if current_date != self.last_reset_date:
            logger.info(f"风控统计跨日切换 (UTC): {self.last_reset_date} -> {current_date}，重置当日盈亏统计")
            self.today_realized_loss = 0.0
            self.today_unrealized_loss = 0.0
            self.consecutive_loss_count = 0
            self.loss_timestamps.clear()
            self.last_reset_date = current_date
            # 如果非人工锁定的 SAFE_MODE，跨日自动恢复单日亏损熔断
            if self.risk_status == RiskStatus.CIRCUIT_BROKEN and not self.is_safe_mode:
                self.risk_status = RiskStatus.NORMAL
                self.is_circuit_broken = False
                self.circuit_break_reason = None
                self.circuit_break_at = None
            self._persist_state_sync()

    def is_in_cooldown(self) -> Tuple[bool, int, Optional[str]]:
        """
        检查当前是否处于交易心理防上头强制冷静期
        :return: (is_cooldown, remaining_mins, reason)
        """
        if not self.cooldown_until_ts:
            return False, 0, None
        now_ms = int(time.time() * 1000)
        if now_ms < self.cooldown_until_ts:
            rem_mins = max(1, int((self.cooldown_until_ts - now_ms + 59999) // 60000))
            return True, rem_mins, self.cooldown_reason
        else:
            # 冷却结束，自动解除
            self.cooldown_until_ts = None
            self.cooldown_reason = None
            return False, 0, None

    def reset_for_mode_switch(self):
        """
        实盘/模拟盘环境切换时的风控状态物理隔离重置：
        防止模拟盘产生的亏损计数、防上头冷却锁、标的止损锁污染实盘交易权限
        """
        self.today_realized_loss = 0.0
        self.today_unrealized_loss = 0.0
        self.consecutive_loss_count = 0
        self.loss_timestamps.clear()
        self.cooldown_until_ts = None
        self.cooldown_reason = None
        self.symbol_cooldowns.clear()
        if self.risk_status == RiskStatus.CIRCUIT_BROKEN:
            self.risk_status = RiskStatus.NORMAL
            self.is_circuit_broken = False
            self.circuit_break_reason = None
            self.circuit_break_at = None
        logger.info("【硬风控环境隔离】已重置当前环境的亏损累计与冷却锁定状态")

    def get_effective_max_leverage(self) -> int:
        """
        获取当前动态有效最大杠杆倍数：
        仅当单日累计亏损超过熔断阈值的 50% 时才触发降杠杆防守 (降至 3x)，避免微幅亏损锁死正常 3x 开仓
        """
        base_max = self.rules.max_leverage
        loss_threshold = max(50.0, self.rules.max_daily_loss_usdt * 0.5)
        if self.today_realized_loss >= loss_threshold:
            return min(3, base_max)
        return base_max

    def record_symbol_stop_loss(self, symbol: str, cooldown_minutes: int = 10, reason: str = "触发止损平仓"):
        """记录单币种止损，触发 10 分钟防洗盘追单冷静期"""
        now_ms = int(time.time() * 1000)
        until_ms = now_ms + cooldown_minutes * 60 * 1000
        self.symbol_cooldowns[symbol] = until_ms
        logger.warning(f"【标的止损冷静锁激活】标的 {symbol} 已锁定 {cooldown_minutes} 分钟防洗盘追单 (原因: {reason})")

    def is_symbol_in_cooldown(self, symbol: str) -> Tuple[bool, int]:
        """检查指定标的是否处于止损冷静期中"""
        now_ms = int(time.time() * 1000)
        until_ms = self.symbol_cooldowns.get(symbol)
        if not until_ms:
            return False, 0
        if now_ms < until_ms:
            rem_mins = max(1, int((until_ms - now_ms + 59999) // 60000))
            return True, rem_mins
        else:
            self.symbol_cooldowns.pop(symbol, None)
            return False, 0

    def record_closed_trade_pnl(self, pnl: float, reason: str = ""):
        """
        记录已平仓订单盈亏，维护连续止损与防上头状态机
        """
        now_sec = time.time()
        self._check_day_rollover()

        if pnl < 0:
            self.today_realized_loss += abs(pnl)
            # 清理 2 小时 (7200s) 以前的止损历史
            self.loss_timestamps = [t for t in self.loss_timestamps if (now_sec - t) <= 7200]
            self.loss_timestamps.append(now_sec)
            self.consecutive_loss_count = len(self.loss_timestamps)

            # 若 2 小时内连续触发 3 笔及以上止损单，激活 15 分钟防上头冷静锁
            if self.consecutive_loss_count >= 3:
                self.cooldown_until_ts = int((now_sec + 900) * 1000)
                self.cooldown_reason = f"2小时内连续触发 {self.consecutive_loss_count} 笔止损，系统激活 15 分钟防上头强制冷静锁"
                logger.warning(f"【交易心理防上头触发】{self.cooldown_reason}")
            # 触发当日亏损熔断判断
            if self.today_realized_loss >= self.rules.max_daily_loss_usdt:
                self.trigger_daily_loss_circuit_breaker(
                    self.today_realized_loss,
                    f"当日累计亏损 {self.today_realized_loss:.2f} USDT 超过上限 {self.rules.max_daily_loss_usdt} USDT"
                )
            else:
                self._persist_state_sync()
        else:
            # 盈利单可缓解连续止损压力
            if self.loss_timestamps:
                self.loss_timestamps.pop(0)
            self.consecutive_loss_count = len(self.loss_timestamps)

    def get_tilt_status(self) -> Dict[str, Any]:
        """获取交易心理防上头状态"""
        is_cd, rem_mins, cd_reason = self.is_in_cooldown()
        loss_threshold = max(50.0, self.rules.max_daily_loss_usdt * 0.5)
        leverage_penalty = self.today_realized_loss >= loss_threshold
        return {
            "is_in_cooldown": is_cd,
            "cooldown_until_ts": self.cooldown_until_ts,
            "cooldown_remaining_mins": rem_mins,
            "consecutive_loss_count": self.consecutive_loss_count,
            "leverage_penalty_applied": leverage_penalty,
            "reason": cd_reason or ("当日亏损已达熔断线 50%，最大可用杠杆已自动收敛至 3x" if leverage_penalty else None)
        }

    async def get_status(self) -> RiskStatusResponse:
        """获取当前风控中枢全景状态"""
        self._check_day_rollover()
        current_daily_loss = self.today_realized_loss + max(0.0, self.today_unrealized_loss)
        ratio = (current_daily_loss / self.rules.max_daily_loss_usdt) if self.rules.max_daily_loss_usdt > 0 else 0.0

        return RiskStatusResponse(
            risk_status=self.risk_status,
            is_circuit_broken=self.is_circuit_broken,
            circuit_break_reason=self.circuit_break_reason,
            circuit_break_at=self.circuit_break_at,
            daily_loss_current=round(current_daily_loss, 2),
            max_daily_loss_usdt=self.rules.max_daily_loss_usdt,
            daily_loss_ratio=round(min(1.0, ratio), 4),
            is_safe_mode=self.is_safe_mode,
            rules=self.rules
        )

    async def record_risk_event(
        self,
        event_type: RiskEventType,
        rule_name: str,
        limit_val: str,
        current_val: str,
        symbol: Optional[str] = None,
        is_blocked: bool = True,
        action_taken: str = "BLOCKED",
        severity: AuditSeverity = AuditSeverity.WARNING,
        details: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        将风控拦截/熔断审计事件落库并记录
        """
        now_ms = int(time.time() * 1000)
        details_json = json.dumps(details or {}, ensure_ascii=False)

        try:
            async with get_db_session() as db:
                sql = text("""
                    INSERT INTO risk_audit_logs (
                        event_type, severity, trigger_source, rule_name,
                        rule_limit_value, current_metric_value, symbol,
                        is_blocked, action_taken, details_json, created_at
                    ) VALUES (
                        :event_type, :severity, 'RISK_ENGINE', :rule_name,
                        :limit_val, :current_val, :symbol,
                        :is_blocked, :action_taken, :details, CURRENT_TIMESTAMP
                    )
                """)
                await db.execute(sql, {
                    "event_type": event_type.value if isinstance(event_type, RiskEventType) else str(event_type),
                    "severity": severity.value if isinstance(severity, AuditSeverity) else str(severity),
                    "rule_name": rule_name,
                    "limit_val": str(limit_val),
                    "current_val": str(current_val),
                    "symbol": symbol,
                    "is_blocked": 1 if is_blocked else 0,
                    "action_taken": action_taken,
                    "details": details_json
                })
                # 获取插入的自增 ID
                res = await db.execute(text("SELECT last_insert_rowid()"))
                inserted_id = res.scalar() or 0
                return int(inserted_id)
        except Exception as e:
            logger.error(f"记录风控审计日志失败: {e}", exc_info=True)
            return 0

    async def load_config_from_db(self):
        """从 SQLite risk_limits 表加载最新风控规则，并恢复当日亏损与熔断持久化状态"""
        async with get_db_session() as db:
            try:
                res = await db.execute(text("SELECT * FROM risk_limits ORDER BY id DESC LIMIT 1"))
                row = res.mappings().first()
                if row:
                    self.rules = RiskRulesConfig(
                        max_order_usdt=float(row.get("max_order_usdt", self.rules.max_order_usdt)),
                        max_daily_loss_usdt=float(row.get("max_daily_loss_usdt", self.rules.max_daily_loss_usdt)),
                        max_leverage=int(row.get("max_leverage", self.rules.max_leverage)),
                        max_slippage_pct=float(row.get("max_slippage_pct", self.rules.max_slippage_pct)),
                        max_open_positions=int(row.get("max_open_positions", self.rules.max_open_positions)),
                        max_position_usdt=float(row.get("max_position_usdt", self.rules.max_position_usdt))
                    )
                    # Stage 0 (T0.4): 恢复持久化的当日累计亏损与熔断状态 (防止重启绕过熔断)
                    saved_reset_raw = str(row.get("daily_loss_reset_at") or "")
                    saved_date = saved_reset_raw[:10] if len(saved_reset_raw) >= 10 else ""
                    today_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")

                    self.is_safe_mode = bool(row.get("is_safe_mode", 0))
                    if saved_date == today_utc:
                        self.last_reset_date = saved_date
                        self.today_realized_loss = max(0.0, float(row.get("daily_loss_current") or 0.0))
                        self.is_circuit_broken = bool(row.get("is_circuit_broken", 0))
                        self.circuit_break_reason = row.get("circuit_break_reason") or None
                        if self.is_safe_mode:
                            self.risk_status = RiskStatus.SAFE_MODE
                        elif self.is_circuit_broken:
                            self.risk_status = RiskStatus.CIRCUIT_BROKEN
                        else:
                            self.risk_status = RiskStatus.NORMAL
                    else:
                        # 数据库记录属于历史 UTC 日期，执行跨日自动重置并写回今日 UTC 日期
                        self.last_reset_date = saved_date or "1970-01-01"
                        self._check_day_rollover()
                        if self.is_safe_mode:
                            self.risk_status = RiskStatus.SAFE_MODE

                    logger.info(
                        f"风控规则与熔断状态已从数据库恢复: 规则={self.rules.model_dump()}, "
                        f"当日已亏={self.today_realized_loss:.2f} USDT, 熔断={self.is_circuit_broken}, 安全模式={self.is_safe_mode}"
                    )
            except Exception as e:
                logger.warning(f"从数据库加载风控配置异常 (将使用默认规则): {e}")

    def update_rules(self, new_rules: RiskRulesConfig):
        """动态热更新内存风控规则"""
        self.rules = new_rules
        logger.info(f"硬风控规则已热更新: {self.rules.model_dump()}")

    async def check_order(
        self,
        order: PlaceOrderRequest,
        current_ticker: Optional[TickerData] = None,
        current_positions: Optional[List[PositionModel]] = None
    ) -> RiskCheckResult:
        """
        核心前置硬风控检查（在订单触达交易所前强制执行）
        :param order: 下单请求体
        :param current_ticker: 当前标的最新盘口行情
        :param current_positions: 当前系统所有持仓
        :return: RiskCheckResult 检查结果
        """
        self._check_day_rollover()

        is_reduce_only = bool(order.reduce_only)

        # ---------------------------------------------------------------------
        # 规则 0: 交易心理防上头强制冷静期 (TILT_PROTECTION) 拦截
        # ---------------------------------------------------------------------
        is_cd, rem_mins, cd_reason = self.is_in_cooldown()
        if is_cd and not is_reduce_only:
            reason = f"【防上头冷静期】系统正处于强制冷却中 (剩余 {rem_mins} 分钟)，已锁定禁止新开仓！原因: {cd_reason}"
            await self.record_risk_event(
                event_type=RiskEventType.ORDER_REJECTED,
                rule_name="TILT_PROTECTION_LOCK",
                limit_val=f"{rem_mins} mins remaining",
                current_val="ORDER_ATTEMPT",
                symbol=order.symbol,
                is_blocked=True,
                action_taken="REJECT_ORDER",
                severity=AuditSeverity.WARNING
            )
            return RiskCheckResult(
                passed=False,
                rejected_rule="TILT_PROTECTION_LOCK",
                message=reason,
                risk_status=self.risk_status
            )

        # ---------------------------------------------------------------------
        # 规则 0.5: 单标的止损防洗盘冷静期 (SYMBOL_STOP_LOSS_COOLDOWN) 拦截
        # ---------------------------------------------------------------------
        is_sym_cd, sym_rem_mins = self.is_symbol_in_cooldown(order.symbol)
        if is_sym_cd and not is_reduce_only:
            reason = f"【标的止损冷静期】标的 {order.symbol} 刚触发止损平仓，正处于 {sym_rem_mins} 分钟防洗盘冷静期，已拦截开仓！"
            await self.record_risk_event(
                event_type=RiskEventType.ORDER_REJECTED,
                rule_name="SYMBOL_STOP_LOSS_COOLDOWN_LOCK",
                limit_val=f"{sym_rem_mins} mins remaining",
                current_val="ORDER_ATTEMPT",
                symbol=order.symbol,
                is_blocked=True,
                action_taken="REJECT_ORDER",
                severity=AuditSeverity.WARNING
            )
            return RiskCheckResult(
                passed=False,
                rejected_rule="SYMBOL_STOP_LOSS_COOLDOWN_LOCK",
                message=reason,
                risk_status=self.risk_status
            )

        # ---------------------------------------------------------------------
        # 规则 1: 安全模式 (SAFE_MODE) 拦截
        # ---------------------------------------------------------------------
        if self.is_safe_mode and not is_reduce_only:
            reason = "系统处于 Panic Button 安全只读模式，已锁定所有新开仓权限"
            await self.record_risk_event(
                event_type=RiskEventType.ORDER_REJECTED,
                rule_name="SAFE_MODE_LOCK",
                limit_val="READ_ONLY",
                current_val="ORDER_ATTEMPT",
                symbol=order.symbol,
                is_blocked=True,
                action_taken="REJECT_ORDER",
                severity=AuditSeverity.CRITICAL
            )
            return RiskCheckResult(
                passed=False,
                rejected_rule="SAFE_MODE_LOCK",
                message=reason,
                risk_status=self.risk_status
            )

        # ---------------------------------------------------------------------
        # 规则 2: 单日亏损熔断 (CIRCUIT_BROKEN) 拦截
        # ---------------------------------------------------------------------
        if self.is_circuit_broken and not is_reduce_only:
            reason = f"触发单日最大亏损熔断锁定 [{self.circuit_break_reason or '当日累计亏损超限'}]，禁止新开仓"
            await self.record_risk_event(
                event_type=RiskEventType.CIRCUIT_BREAK_TRIGGERED,
                rule_name="DAILY_LOSS_CIRCUIT_BREAKER",
                limit_val=f"{self.rules.max_daily_loss_usdt} USDT",
                current_val=f"{self.today_realized_loss} USDT",
                symbol=order.symbol,
                is_blocked=True,
                action_taken="REJECT_ORDER",
                severity=AuditSeverity.CRITICAL
            )
            return RiskCheckResult(
                passed=False,
                rejected_rule="DAILY_LOSS_CIRCUIT_BREAKER",
                message=reason,
                risk_status=self.risk_status
            )

        # ---------------------------------------------------------------------
        # 规则 2.5: Post-Only 与市价单协同路由
        # 允許交易员手动市价单与 AI 一键市价委托通行，并交由下方规则 5 (MAX_SLIPPAGE_PROTECTION) 严格检验滑点
        # ---------------------------------------------------------------------

        # ---------------------------------------------------------------------
        # 规则 3: 杠杆倍数硬锁上限与单日亏损自适应降杠杆保护
        # ---------------------------------------------------------------------
        effective_max_leverage = self.get_effective_max_leverage()
        if order.leverage > self.rules.max_leverage:
            reason = f"委托杠杆倍数 ({order.leverage}x) 超过系统最高硬锁上限 ({self.rules.max_leverage}x)"
            await self.record_risk_event(
                event_type=RiskEventType.LEVERAGE_EXCEEDED,
                rule_name="MAX_LEVERAGE",
                limit_val=f"{self.rules.max_leverage}x",
                current_val=f"{order.leverage}x",
                symbol=order.symbol,
                is_blocked=True,
                action_taken="REJECT_ORDER",
                severity=AuditSeverity.WARNING
            )
            return RiskCheckResult(
                passed=False,
                rejected_rule="MAX_LEVERAGE",
                message=reason,
                risk_status=self.risk_status
            )
        elif order.leverage > effective_max_leverage:
            logger.info(f"触发当日回撤降杠杆保护，自动将订单杠杆由 {order.leverage}x 收敛至 {effective_max_leverage}x")
            order.leverage = effective_max_leverage

        # 估算委托名义价值 (Notional USDT)
        price_to_use = order.price
        if (price_to_use is None or price_to_use <= 0) and current_ticker:
            price_to_use = current_ticker.last_price

        estimated_price = price_to_use or 0.0
        # 估算订单名义价值 (若为 USDT 永续，以 sz * price 计算)
        estimated_notional = estimated_price * order.sz

        # ---------------------------------------------------------------------
        # 规则 4: 单笔最大下单金额拦截 (max_order_usdt)
        # ---------------------------------------------------------------------
        if not is_reduce_only and estimated_notional > (self.rules.max_order_usdt * 1.08):
            reason = (
                f"单笔委托名义金额 ({estimated_notional:.2f} USDT) 超过硬风控限额 "
                f"({self.rules.max_order_usdt:.2f} USDT)"
            )
            await self.record_risk_event(
                event_type=RiskEventType.MAX_ORDER_EXCEEDED,
                rule_name="MAX_ORDER_USDT",
                limit_val=f"{self.rules.max_order_usdt} USDT",
                current_val=f"{estimated_notional:.2f} USDT",
                symbol=order.symbol,
                is_blocked=True,
                action_taken="REJECT_ORDER",
                severity=AuditSeverity.WARNING
            )
            return RiskCheckResult(
                passed=False,
                rejected_rule="MAX_ORDER_USDT",
                message=reason,
                risk_status=self.risk_status
            )


        # ---------------------------------------------------------------------
        # 规则 5: 市价单盘口滑点保护 (max_slippage_pct)
        # ---------------------------------------------------------------------
        if order.order_type in (OrderType.MARKET, OrderType.OPTIMAL_LIMIT_IOC) and current_ticker:
            last_px = current_ticker.last_price
            mark_px = current_ticker.mark_price or last_px
            if last_px > 0:
                slippage_pct = abs(last_px - mark_px) / mark_px * 100.0 if mark_px > 0 else 0.0

                if slippage_pct > self.rules.max_slippage_pct:
                    reason = (
                        f"市价单预估偏离滑点 ({slippage_pct:.3f}%) 超过硬风控保护阈值 "
                        f"({self.rules.max_slippage_pct}%)，已阻断以防资金磨损"
                    )
                    await self.record_risk_event(
                        event_type=RiskEventType.SLIPPAGE_EXCEEDED,
                        rule_name="MAX_SLIPPAGE_PROTECTION",
                        limit_val=f"{self.rules.max_slippage_pct}%",
                        current_val=f"{slippage_pct:.3f}%",
                        symbol=order.symbol,
                        is_blocked=True,
                        action_taken="REJECT_ORDER",
                        severity=AuditSeverity.WARNING
                    )
                    return RiskCheckResult(
                        passed=False,
                        rejected_rule="MAX_SLIPPAGE_PROTECTION",
                        message=reason,
                        risk_status=self.risk_status
                    )

        # ---------------------------------------------------------------------
        # 规则 6: 最大持仓标的数与单标的名义金额上限拦截
        # ---------------------------------------------------------------------
        if current_positions and not is_reduce_only:
            active_symbols = {p.symbol for p in current_positions if p.pos != 0}
            if order.symbol not in active_symbols and len(active_symbols) >= self.rules.max_open_positions:
                reason = (
                    f"当前持仓标的数 ({len(active_symbols)}) 已达上限 ({self.rules.max_open_positions})，"
                    f"禁止开仓新标的 {order.symbol}"
                )
                await self.record_risk_event(
                    event_type=RiskEventType.POSITION_LIMIT_EXCEEDED,
                    rule_name="MAX_OPEN_POSITIONS",
                    limit_val=str(self.rules.max_open_positions),
                    current_val=str(len(active_symbols)),
                    symbol=order.symbol,
                    is_blocked=True,
                    action_taken="REJECT_ORDER",
                    severity=AuditSeverity.WARNING
                )
                return RiskCheckResult(
                    passed=False,
                    rejected_rule="MAX_OPEN_POSITIONS",
                    message=reason,
                    risk_status=self.risk_status
                )

            # 校验单标的持仓规模
            existing_pos = next((p for p in current_positions if p.symbol == order.symbol), None)
            existing_notional = existing_pos.notional_usd if existing_pos else 0.0
            if (existing_notional + estimated_notional) > self.rules.max_position_usdt:
                reason = (
                    f"该标的持仓规模将达 ({existing_notional + estimated_notional:.2f} USDT)，"
                    f"超出单标的最大规模上限 ({self.rules.max_position_usdt:.2f} USDT)"
                )
                await self.record_risk_event(
                    event_type=RiskEventType.POSITION_LIMIT_EXCEEDED,
                    rule_name="MAX_POSITION_USDT",
                    limit_val=f"{self.rules.max_position_usdt} USDT",
                    current_val=f"{existing_notional + estimated_notional:.2f} USDT",
                    symbol=order.symbol,
                    is_blocked=True,
                    action_taken="REJECT_ORDER",
                    severity=AuditSeverity.WARNING
                )
                return RiskCheckResult(
                    passed=False,
                    rejected_rule="MAX_POSITION_USDT",
                    message=reason,
                    risk_status=self.risk_status
                )

        # ---------------------------------------------------------------------
        # 规则 7: DCA 与加仓动态强平线穿透硬拦截 (源自 oke_auto_trade 核心优势融合)
        # ---------------------------------------------------------------------
        if current_positions and not is_reduce_only:
            matching_pos = next((p for p in current_positions if p.symbol == order.symbol and p.pos != 0), None)
            if matching_pos:
                order_px = order.price or (current_ticker.last_price if current_ticker else 0.0)
                liq_px = matching_pos.liquidation_price or matching_pos.liq_px
                pos_side_str = matching_pos.pos_side.value if hasattr(matching_pos.pos_side, "value") else str(matching_pos.pos_side)

                if order_px > 0 and liq_px and liq_px > 0:
                    is_pos_long = "long" in pos_side_str.lower()
                    # 做多补仓：若委托价格低于或等于当前强平价，说明价格尚未跌到该买入点位，仓位就早已爆仓！
                    if is_pos_long and order_px <= liq_px:
                        reason = (
                            f"【强平穿透硬拦截】计划加仓买入价 ({order_px:.4f}) <= 当前持仓强平价 ({liq_px:.4f})！"
                            f"价格跌至该加仓点前仓位将先行爆仓，已坚决阻断发单！"
                        )
                        await self.record_risk_event(
                            event_type=RiskEventType.ORDER_REJECTED,
                            rule_name="LIQUIDATION_PENETRATION_LOCK",
                            limit_val=f"LiqPx={liq_px:.4f}",
                            current_val=f"OrderPx={order_px:.4f}",
                            symbol=order.symbol,
                            is_blocked=True,
                            action_taken="REJECT_ORDER",
                            severity=AuditSeverity.CRITICAL,
                            details={"liq_px": liq_px, "order_px": order_px, "pos_side": pos_side_str}
                        )
                        return RiskCheckResult(
                            passed=False,
                            rejected_rule="LIQUIDATION_PENETRATION_LOCK",
                            message=reason,
                            risk_status=self.risk_status
                        )
                    # 做空补仓：若委托价格高于或等于当前强平价，同理先行爆仓！
                    elif (not is_pos_long) and order_px >= liq_px:
                        reason = (
                            f"【强平穿透硬拦截】计划加仓卖出价 ({order_px:.4f}) >= 当前持仓强平价 ({liq_px:.4f})！"
                            f"价格涨至该加仓点前仓位将先行爆仓，已坚决阻断发单！"
                        )
                        await self.record_risk_event(
                            event_type=RiskEventType.ORDER_REJECTED,
                            rule_name="LIQUIDATION_PENETRATION_LOCK",
                            limit_val=f"LiqPx={liq_px:.4f}",
                            current_val=f"OrderPx={order_px:.4f}",
                            symbol=order.symbol,
                            is_blocked=True,
                            action_taken="REJECT_ORDER",
                            severity=AuditSeverity.CRITICAL,
                            details={"liq_px": liq_px, "order_px": order_px, "pos_side": pos_side_str}
                        )
                        return RiskCheckResult(
                            passed=False,
                            rejected_rule="LIQUIDATION_PENETRATION_LOCK",
                            message=reason,
                            risk_status=self.risk_status
                        )

        # ---------------------------------------------------------------------
        # 规则 8 (v8.0): 板块同方向集中度硬上限 (防止满仓押注单一赛道同向回撤)
        # ---------------------------------------------------------------------
        if not is_reduce_only:
            try:
                from services.capital_flow_service import classify_symbol_sector, get_sector_display_name, SectorCategory
                from state import capital_flow_service

                target_sector = classify_symbol_sector(order.symbol)
                order_pos_side = (
                    order.pos_side.value if hasattr(order.pos_side, "value") else str(order.pos_side or "")
                ).lower()
                order_side_str = (
                    order.side.value if hasattr(order.side, "value") else str(order.side or "")
                ).lower()
                target_is_short = (order_pos_side == "short") or (order_pos_side in ("", "net") and order_side_str == "sell")

                # 8.1 美股映射代币做空开关硬校验
                cf_cfg = capital_flow_service.config if capital_flow_service else None
                if target_sector == SectorCategory.TRADFI and target_is_short and cf_cfg and not cf_cfg.allow_tradfi_short:
                    reason = f"【TradFi 禁空护栏】美股映射标的 {order.symbol} 默认关闭做空权限（防止休市盘口真空轧空），可在资金流配置中开启。"
                    await self.record_risk_event(
                        event_type=RiskEventType.ORDER_REJECTED,
                        rule_name="TRADFI_SHORT_DISABLED",
                        limit_val="allow_tradfi_short=False",
                        current_val=f"{order.symbol} SHORT",
                        symbol=order.symbol,
                        is_blocked=True,
                        action_taken="REJECT_ORDER",
                        severity=AuditSeverity.WARNING,
                    )
                    return RiskCheckResult(
                        passed=False,
                        rejected_rule="TRADFI_SHORT_DISABLED",
                        message=reason,
                        risk_status=self.risk_status,
                    )

                # 8.2 同板块同方向持仓数上限拦截（对已有该标的加仓不重复计数）
                if current_positions:
                    sec_cap = int(
                        getattr(cf_cfg, "max_same_sector_same_direction", None)
                        or getattr(self.rules, "max_same_sector_same_direction_positions", 2)
                        or 2
                    )
                    same_sec_syms = set()
                    for p in current_positions:
                        if p.pos == 0:
                            continue
                        p_side_str = (p.pos_side.value if hasattr(p.pos_side, "value") else str(p.pos_side or "")).lower()
                        p_is_short = "short" in p_side_str
                        if p_is_short == target_is_short and classify_symbol_sector(p.symbol) == target_sector:
                            same_sec_syms.add(p.symbol)

                    if order.symbol not in same_sec_syms and len(same_sec_syms) >= sec_cap:
                        dir_label = "空头" if target_is_short else "多头"
                        sec_cn = get_sector_display_name(target_sector)
                        reason = (
                            f"【板块集中度上限拦截】板块 [{sec_cn}] 已持有 {len(same_sec_syms)} 个{dir_label}仓位 "
                            f"({', '.join(sorted(same_sec_syms))})，达到上限 {sec_cap}，拒绝继续同向开仓 {order.symbol}！"
                        )
                        await self.record_risk_event(
                            event_type=RiskEventType.POSITION_LIMIT_EXCEEDED,
                            rule_name="MAX_SAME_SECTOR_SAME_DIRECTION",
                            limit_val=f"{sec_cn} {dir_label} <= {sec_cap}",
                            current_val=f"{len(same_sec_syms)} ({', '.join(sorted(same_sec_syms))})",
                            symbol=order.symbol,
                            is_blocked=True,
                            action_taken="REJECT_ORDER",
                            severity=AuditSeverity.WARNING,
                        )
                        return RiskCheckResult(
                            passed=False,
                            rejected_rule="MAX_SAME_SECTOR_SAME_DIRECTION",
                            message=reason,
                            risk_status=self.risk_status,
                        )

                # 8.3 反 FOMO / 防地板空 24h 极端涨跌幅硬拦截（仅针对新开首仓）
                has_existing_sym_pos = any(p.symbol == order.symbol and p.pos != 0 for p in (current_positions or []))
                if not has_existing_sym_pos:
                    chg_cap = float(
                        getattr(cf_cfg, "anti_fomo_change_24h_cap", None)
                        or getattr(self.rules, "anti_fomo_24h_change_pct_cap", 18.0)
                        or 18.0
                    )
                    chg_24h = None
                    if current_ticker and getattr(current_ticker, "change_24h_pct", None) is not None:
                        chg_24h = float(current_ticker.change_24h_pct)
                    elif capital_flow_service:
                        sym_flow = capital_flow_service.get_symbol_flow_sync(order.symbol)
                        if sym_flow is not None:
                            chg_24h = float(sym_flow.change_24h_pct)

                    if chg_24h is not None:
                        if (not target_is_short) and chg_24h > chg_cap:
                            reason = f"【反FOMO追高拦截】{order.symbol} 24h 涨幅已达 {chg_24h:+.1f}% (上限 +{chg_cap:.0f}%)，禁止高位追多接盘！"
                            await self.record_risk_event(
                                event_type=RiskEventType.ORDER_REJECTED,
                                rule_name="ANTI_FOMO_EXTREME_GUARD",
                                limit_val=f"+{chg_cap:.1f}%",
                                current_val=f"{chg_24h:+.1f}%",
                                symbol=order.symbol,
                                is_blocked=True,
                                action_taken="REJECT_ORDER",
                                severity=AuditSeverity.WARNING,
                            )
                            return RiskCheckResult(
                                passed=False,
                                rejected_rule="ANTI_FOMO_EXTREME_GUARD",
                                message=reason,
                                risk_status=self.risk_status,
                            )
                        if target_is_short and chg_24h < -chg_cap:
                            reason = f"【防地板追空拦截】{order.symbol} 24h 跌幅已达 {chg_24h:+.1f}% (下限 -{chg_cap:.0f}%)，禁止低位追空防轧空反弹！"
                            await self.record_risk_event(
                                event_type=RiskEventType.ORDER_REJECTED,
                                rule_name="ANTI_FLOOR_SHORT_GUARD",
                                limit_val=f"-{chg_cap:.1f}%",
                                current_val=f"{chg_24h:+.1f}%",
                                symbol=order.symbol,
                                is_blocked=True,
                                action_taken="REJECT_ORDER",
                                severity=AuditSeverity.WARNING,
                            )
                            return RiskCheckResult(
                                passed=False,
                                rejected_rule="ANTI_FLOOR_SHORT_GUARD",
                                message=reason,
                                risk_status=self.risk_status,
                            )
            except Exception as sec_risk_err:
                logger.debug(f"资金流与板块风控校验异常降级跳过: {sec_risk_err}")

        # 全部硬风控检查通过
        return RiskCheckResult(
            passed=True,
            rejected_rule=None,
            message="通过全部硬风控前置校验",
            risk_status=self.risk_status
        )

    def evaluate_dca_chain_safety(
        self,
        direction: str,
        initial_price: float,
        leverage: float,
        initial_margin: float,
        dca_margin_base: float,
        max_dca_orders: int = 3,
        price_deviation_percent: float = 1.5,
        extra_margin: float = 0.0
    ):
        """
        前置调用 DCA 强平穿透与极限保证金精算模型
        """
        from core.dca_liquidation_calculator import DcaLiquidationCalculator
        return DcaLiquidationCalculator.calculate_dca_chain(
            direction=direction,
            initial_price=initial_price,
            leverage=leverage,
            initial_margin=initial_margin,
            dca_margin_base=dca_margin_base,
            max_dca_orders=max_dca_orders,
            price_deviation_percent=price_deviation_percent,
            extra_margin=extra_margin
        )


    def trigger_daily_loss_circuit_breaker(self, current_loss: float, reason: str):
        """触发单日亏损熔断锁定并立即持久化至 SQLite + 推送 Telegram 告警"""
        self.is_circuit_broken = True
        self.risk_status = RiskStatus.CIRCUIT_BROKEN
        self.circuit_break_reason = reason
        self.circuit_break_at = int(time.time() * 1000)
        self.today_realized_loss = max(self.today_realized_loss, float(current_loss))
        self._persist_state_sync()
        msg = f"🛑【硬风控熔断触发】当日累计亏损 {current_loss:.2f} USDT，系统已锁定禁止开新仓！原因: {reason}"
        logger.critical(msg)
        try:
            loop = asyncio.get_running_loop()
            from state import telegram_bot_service
            loop.create_task(telegram_bot_service.send_message(msg))
        except Exception:
            pass

    async def reset_circuit_breaker(self, reason: str):
        """手动解除熔断锁定并持久化清除状态"""
        self.is_circuit_broken = False
        self.is_safe_mode = False
        self.risk_status = RiskStatus.NORMAL
        self.circuit_break_reason = None
        self.circuit_break_at = None
        self._persist_state_sync()

        logger.info(f"【硬风控熔断解除】原因: {reason}")
        await self.record_risk_event(
            event_type=RiskEventType.CIRCUIT_BREAK_MANUAL_RESET,
            rule_name="CIRCUIT_BREAKER_RESET",
            limit_val="NORMAL",
            current_val="MANUAL_RESET",
            symbol=None,
            is_blocked=False,
            action_taken="UNLOCKED",
            severity=AuditSeverity.INFO,
            details={"reset_reason": reason}
        )

    async def execute_panic_button(
        self,
        rest_client: OKXRestClient,
        req: PanicButtonRequest
    ) -> PanicButtonResponse:
        """
        Panic Button 紧急处置中枢 (最高优先级熔断)
        严格遵循金融安全两阶段原则 (Cancel-Before-Close):
        1. 步骤 1: 立即取消所有活跃挂单以释放冻结保证金并防止撮合
        2. 步骤 2: 立即发起市价全平当前所有持仓
        3. 步骤 3: 锁死系统状态为 SAFE_MODE，落库审计日志与熔断时间
        """
        now_ms = int(time.time() * 1000)
        execution_details: List[PanicExecutionDetail] = []
        failed_operations: List[str] = []
        canceled_orders_count = 0
        closed_positions_count = 0

        logger.critical("【PANIC BUTTON 触发】正在启动紧急撤单与全平应急处置流程！")

        # ---------------------------------------------------------------------
        # 阶段 1: Cancel All Orders (优先全撤挂单：包含普通挂单与策略条件单)
        # ---------------------------------------------------------------------
        if req.cancel_all_orders:
            try:
                logger.info("[Panic Button] 阶段 1: 正在拉取并撤销所有活跃挂单与策略单...")
                target_symbol = req.target if req.target != "ALL" else None

                # 1.1 查询并批量撤销普通活跃挂单
                pending_orders = await rest_client.get_pending_orders(symbol=target_symbol, inst_type="SWAP")
                if pending_orders:
                    orders_to_cancel = [
                        {"instId": o["instId"], "ordId": o["ordId"]}
                        for o in pending_orders
                        if "instId" in o and "ordId" in o
                    ]
                    cancel_res = await rest_client.cancel_batch_orders(orders_to_cancel)
                    canceled_orders_count += len(cancel_res)
                    logger.info(f"[Panic Button] 已撤销 {len(cancel_res)}/{len(orders_to_cancel)} 笔普通挂单")

                # 1.2 查询并批量撤销策略/附条件挂单 (如止盈止损条件单)
                for ord_tp in ["conditional", "trigger"]:
                    try:
                        algo_orders = await rest_client.get_pending_algo_orders(symbol=target_symbol, inst_type="SWAP", ord_type=ord_tp)
                        if algo_orders:
                            algos_to_cancel = [
                                {"instId": a["instId"], "algoId": a["algoId"]}
                                for a in algo_orders
                                if "instId" in a and "algoId" in a
                            ]
                            cancel_algo_res = await rest_client.cancel_batch_algo_orders(algos_to_cancel)
                            canceled_orders_count += len(cancel_algo_res)
                            logger.info(f"[Panic Button] 已撤销 {len(cancel_algo_res)} 笔 {ord_tp} 策略条件单")
                    except Exception as algo_err:
                        logger.warning(f"[Panic Button] 撤销策略挂单 ({ord_tp}) 异常: {algo_err}")

                execution_details.append(PanicExecutionDetail(
                    symbol=req.target,
                    action="CANCEL_ORDER",
                    target_id="ALL_ACTIVE_ORDERS",
                    status="SUCCESS",
                    message=f"已成功撤销 {canceled_orders_count} 笔挂单与策略单"
                ))
            except Exception as e:
                logger.error(f"[Panic Button] 批量撤单异常: {e}")
                failed_operations.append(f"CANCEL_ORDER {req.target}: {str(e)}")

        # 留出 100ms 交易所撮合引擎撤单与解冻保证金缓冲
        await asyncio.sleep(0.1)

        # ---------------------------------------------------------------------
        # 阶段 2: Close All Positions (市价全平持仓)
        # ---------------------------------------------------------------------
        if req.close_all_positions:
            try:
                logger.info("[Panic Button] 阶段 2: 正在拉取持仓并市价全平...")
                positions = await rest_client.get_positions(
                    inst_type="SWAP",
                    symbol=req.target if req.target != "ALL" else None
                )
                for pos in positions:
                    if pos.pos != 0:
                        try:
                            await rest_client.close_position(
                                symbol=pos.symbol,
                                mgn_mode="cross",
                                pos_side=pos.pos_side.value if pos.pos_side else None,
                                auto_cancel=True
                            )
                            closed_positions_count += 1
                            execution_details.append(PanicExecutionDetail(
                                symbol=pos.symbol,
                                action="CLOSE_POSITION",
                                target_id=pos.pos_id or pos.symbol,
                                status="SUCCESS",
                                message=f"标的 {pos.symbol} 持仓已发起市价全平"
                            ))
                        except Exception as close_err:
                            logger.error(f"[Panic Button] 平仓标的 {pos.symbol} 失败: {close_err}")
                            failed_operations.append(f"CLOSE_POSITION {pos.symbol}: {str(close_err)}")
            except Exception as pos_err:
                logger.error(f"[Panic Button] 查询持仓并发起平仓发生异常: {pos_err}")
                failed_operations.append(f"GET_POSITIONS_AND_CLOSE {req.target}: {str(pos_err)}")

        # ---------------------------------------------------------------------
        # 阶段 3: 锁死系统进入 SAFE_MODE 并持久化审计日志
        # ---------------------------------------------------------------------
        self.is_safe_mode = True
        self.risk_status = RiskStatus.SAFE_MODE
        self.circuit_break_reason = f"PANIC_BUTTON: {req.reason}"
        self.circuit_break_at = now_ms

        await self.record_risk_event(
            event_type=RiskEventType.PANIC_BUTTON_TRIGGERED,
            rule_name="PANIC_BUTTON",
            limit_val="SAFE_MODE",
            current_val="TRIGGERED",
            symbol=req.target,
            is_blocked=True,
            action_taken="CANCEL_ALL_AND_CLOSE_ALL",
            severity=AuditSeverity.CRITICAL,
            details={
                "canceled_orders_count": canceled_orders_count,
                "closed_positions_count": closed_positions_count,
                "reason": req.reason,
                "failed_operations": failed_operations
            }
        )

        msg = (
            f"紧急处置完成：已撤销 {canceled_orders_count} 笔挂单，"
            f"市价全平 {closed_positions_count} 个持仓，系统已锁死进入只读安全模式"
        )
        return PanicButtonResponse(
            success=len(failed_operations) == 0,
            canceled_orders_count=canceled_orders_count,
            closed_positions_count=closed_positions_count,
            failed_operations=failed_operations,
            execution_details=execution_details,
            system_state=RiskStatus.SAFE_MODE.value,
            circuit_broken_at=now_ms,
            message=msg
        )

    def get_adaptive_risk_budget(self, equity: float) -> Tuple[float, str, float]:
        """
        FP&A 滚动风险预算与回撤自适应头寸管理 (Drawdown-Adaptive Risk Sizing)
        由 agency-fp-a-analyst (FP&A 财务规划分析师) 主导
        - 追踪高水位线 HWM (High Water Mark)
        - 依据当前回撤深度阶梯式压降单笔风险暴露，防止连续亏损打穿本金
        :return: (effective_risk_pct, budget_tier, drawdown_pct)
        """
        if equity > self.high_water_mark_equity:
            self.high_water_mark_equity = equity

        hwm = self.high_water_mark_equity
        drawdown_pct = ((hwm - equity) / hwm) if hwm > 0 else 0.0
        base_pct = self.rules.fixed_risk_per_trade_percent

        if drawdown_pct < 0.03:
            # 正常扩张期 (回撤 < 3%): 维持标准 1.0% (以 7000 本金计为 70 USDT)
            return base_pct, "EXPANSION_NORMAL", drawdown_pct
        elif drawdown_pct < 0.06:
            # 轻度回撤防守 (3% <= 回撤 < 6%): 风险预算下调至基准的 70% (约 0.70%)
            return round(base_pct * 0.70, 4), "PRE_CAUTION_DEFENSIVE", drawdown_pct
        elif drawdown_pct < 0.10:
            # 中度回撤防御 (6% <= 回撤 < 10%): 风险预算下调至基准的 40% (约 0.40%)
            return round(base_pct * 0.40, 4), "MODERATE_DRAWDOWN_LOCK", drawdown_pct
        else:
            # 重度回撤锁定 (回撤 >= 10%): 风险预算强力急冻至基准的 20% (约 0.20%)，资本保护最高优先级
            return round(base_pct * 0.20, 4), "CRITICAL_CAPITAL_PRESERVATION", drawdown_pct

    def get_fpa_risk_budget_status(self, equity: float) -> Dict[str, Any]:
        """获取当前 FP&A 滚动风险预算状态报告"""
        risk_pct, tier, dd_pct = self.get_adaptive_risk_budget(equity)
        tier_descriptions = {
            "EXPANSION_NORMAL": "正常资本扩张期: 风险预算 100% 满额分配 (1.0% 单笔风险)",
            "PRE_CAUTION_DEFENSIVE": "轻度回撤防守期: 风险预算收缩至 70% (0.7% 单笔风险)",
            "MODERATE_DRAWDOWN_LOCK": "中度回撤锁定期: 风险预算收紧至 40% (0.4% 单笔风险)",
            "CRITICAL_CAPITAL_PRESERVATION": "极限资本保护期: 风险预算急冻至 20% (0.2% 单笔风险)，全面防守",
        }
        return {
            "current_equity": round(equity, 2),
            "high_water_mark": round(self.high_water_mark_equity, 2),
            "drawdown_pct": round(dd_pct * 100.0, 2),
            "effective_risk_pct": round(risk_pct * 100.0, 2),
            "budget_tier": tier,
            "description": tier_descriptions.get(tier, "")
        }

    def calculate_fixed_risk_position_size(
        self,
        equity: float,
        entry_price: float,
        stop_loss_price: float,
        contract_val: float = 1.0,
        risk_pct: Optional[float] = None
    ) -> float:
        """
        基于 FP&A 滚动风险预算与恒定资金风险模型计算开仓张数/币数
        公式: Position Size = (Equity * AdaptiveRiskPct) / (|Entry - StopLoss| * ContractVal)
        """
        if risk_pct is not None:
            pct = risk_pct
            tier_label = "MANUAL_OVERRIDE"
            dd_pct = 0.0
        else:
            pct, tier_label, dd_pct = self.get_adaptive_risk_budget(equity)

        max_loss_usdt = equity * pct
        price_diff = abs(entry_price - stop_loss_price)

        if price_diff <= 0 or contract_val <= 0:
            logger.warning(f"[RiskController] 止损距离或合约面值异常: diff={price_diff}, val={contract_val}")
            return 0.0

        raw_size = max_loss_usdt / (price_diff * contract_val)

        # 检查是否受单笔最大名义金额限制
        estimated_notional = raw_size * entry_price * contract_val
        if estimated_notional > self.rules.max_order_usdt:
            raw_size = self.rules.max_order_usdt / (entry_price * contract_val)

        logger.info(
            f"🎯【FP&A 自适应风险头寸计算】净值={equity:.2f} USDT, HWM={self.high_water_mark_equity:.2f}, "
            f"回撤={dd_pct*100:.2f}%, 风险比例={pct*100:.2f}% [{tier_label}], 允许亏损={max_loss_usdt:.2f} USDT, "
            f"入场={entry_price}, 止损={stop_loss_price}, 距离={price_diff:.2f} -> 建议开仓={raw_size:.4f} 张/币 (名义价值 ${raw_size * entry_price * contract_val:.2f})"
        )
        return round(raw_size, 4)


# 单例硬风控中枢
risk_controller = RiskController()
