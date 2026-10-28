"""
OKX-Dog 个人量化智能交易系统 - 7x24h 全自动后端常驻量化操盘引擎与双环境收益隔离中枢
模块: okx-dog-backend/auto_pilot.py

核心职责:
1. 7x24h 脱机自主运行：独立于前端浏览器，常驻后台协程循环巡检
2. 模拟盘 (DEMO) 与 实盘 (LIVE) 全生命周期双环境物理隔离：
   - 独立的收益采样历史 (snapshots)、成交战绩 (trades)、运行状态 (is_running) 与初始本金
   - 切换系统模式时自动安全关闭旧环境量化并记忆上一次策略配置
3. 多标的监控池 (Multi-Symbol Pool)：并发轮巡 TradFi (黄金/大宗/美股代币/RWA) 与主流加密资产
4. 持仓移动保本与动态追踪止损 (Breakeven & Trailing Stop)：浮盈达标自动将止损线无损移至开仓保本价
5. 顺势金字塔加仓 (Pyramiding) / 回调均摊 (DCA) / 趋势反转反手 (Reversal)
6. 组合级持仓与敞口控制 (max_concurrent_positions) 与超时挂单清理
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import json
import logging
import math
import time
import uuid
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import text
from database import get_db_session
from models import (
    AutoPilotConfigUpdateRequest,
    AutoPilotHistoryPoint,
    AutoPilotHistoryResponse,
    AutoPilotStartRequest,
    AutoPilotStatusModel,
    AutoPilotTradeRecord,
    InstType,
    OrderSide,
    OrderType,
    PlaceOrderRequest,
    PosSide,
    SignalAction,
    TickerData,
)

logger = logging.getLogger("okx_dog.auto_pilot")


class AutoPilotEnvState:
    """
    单个环境（模拟沙盒 vs 真实实盘）独立的操盘状态机与收益账本
    """

    def __init__(self, is_live_mode: bool):
        self.is_live_mode: bool = is_live_mode
        self.is_running: bool = False  # 默认停止，等待用户开启
        self.session_id: str = f"auto_{'live' if is_live_mode else 'paper'}_{int(time.time())}_{uuid.uuid4().hex[:4]}"
        self.start_time: int = int(time.time() * 1000)
        self.stop_time: Optional[int] = None
        self.start_equity: float = 0.0 if is_live_mode else 10000.0
        self.current_equity: float = self.start_equity
        self.peak_equity: float = self.start_equity
        self.max_drawdown_pct: float = 0.0

        self.symbol: str = "BTC-USDT-SWAP"
        self.focus_symbols: List[str] = [
            "BTC-USDT-SWAP",
            "ETH-USDT-SWAP",
            "SOL-USDT-SWAP",
            "XAU-USDT-SWAP",
            "NVDA-USDT-SWAP",
            "ONDO-USDT-SWAP",
        ]
        self.symbols: List[str] = list(self.focus_symbols)
        self.focus_mode: str = "SINGLE_SYMBOL"  # SINGLE_SYMBOL | MULTI_SYMBOL_POOL
        self.market_regime: str = "TRENDING_UP"  # TRENDING_UP | TRENDING_DOWN | RANGING | VOLATILE_BREAKOUT
        self.aggressiveness_mode: str = "BALANCED"  # CONSERVATIVE | BALANCED | AGGRESSIVE
        self.max_scale_layers: int = 3          # 最大加仓层数 (1~5)
        self.scale_in_strategy: str = "SMART_HYBRID"  # PYRAMIDING | PULLBACK_DCA | SMART_HYBRID
        self.auto_reversal_enabled: bool = True  # 遇到强反向信号是否自动反手

        self.max_concurrent_positions: int = 3
        self.trailing_stop_enabled: bool = True
        self.breakeven_trigger_ratio: float = 0.5  # 浮盈达 0.5R 触发移动保本
        self.trailing_profit_threshold: float = 0.012  # 跟踪止盈激活门槛 (例如 0.012 表示浮盈达 1.2% 时激活跟踪盯市)
        self.trailing_profit_callback: float = 0.25   # 跟踪止盈回撤平仓比例 (例如 0.25 表示从最高浮盈回撤 25% 时主动市价止盈)
        self.reentry_cooldown_seconds: int = 90       # 平仓后重新入场冷却时间 (秒)
        self.funding_fee_guard_enabled: bool = True   # 是否启用资金费结算时点防收割保护 (结算前15分钟避开高费率建仓)
        self.min_confidence: float = 0.70
        self.auto_leverage_enabled: bool = True
        self.post_only_preferred: bool = True       # 优先被动挂单 (POST_ONLY/Maker) 节省 60% 手续费
        self.stop_loss_cooldown_minutes: int = 30   # 止损后 30 分钟防洗盘追单冷静期
        self.stop_loss_equity_floor: Optional[float] = None  # 最低保留本金底线 (USDT)
        self.max_loss_limit_usdt: Optional[float] = None     # 最大允许亏损额 (USDT)
        self.order_timeout_seconds: int = 180  # 挂单超时清理 (秒)

        # 巡检与加仓状态
        self.last_patrol_time: Optional[int] = None
        self.last_patrol_log: Optional[str] = f"【{'实盘' if is_live_mode else '模拟'}环境】就绪，等待开启操盘"
        self._executed_plan_ids: Set[str] = set()
        self._breakeven_applied_positions: Set[str] = set()
        self.position_peaks: Dict[str, Dict[str, float]] = {}  # symbol -> {"peak_price": x, "peak_profit_ratio": y}
        self.last_closed_timestamps: Dict[str, int] = {}       # symbol -> 上次平仓完成时间戳 (ms)
        self.position_scale_records: Dict[str, int] = {}       # symbol -> 当前加仓层数
        self.last_scale_timestamps: Dict[str, int] = {}        # symbol -> 上次加仓时间戳

        # 独立的历史快照与平仓流水
        self.snapshots: List[AutoPilotHistoryPoint] = []
        self.trades: List[AutoPilotTradeRecord] = []

        # 注入初始基准点
        now_ms = int(time.time() * 1000)
        self.snapshots.append(
            AutoPilotHistoryPoint(
                timestamp=now_ms,
                equity=self.start_equity,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                total_pnl=0.0,
                pnl_ratio=0.0,
            )
        )

    def apply_aggressiveness_preset(self, mode: str):
        """应用激进程度预设参数"""
        m = (mode or "BALANCED").upper()
        self.aggressiveness_mode = m
        if m == "CONSERVATIVE":
            self.min_confidence = 0.80
            self.max_scale_layers = 1
        elif m == "AGGRESSIVE":
            self.min_confidence = 0.60
            self.max_scale_layers = 5
        else:  # BALANCED
            self.min_confidence = 0.70
            self.max_scale_layers = 3

    def reset_for_start(self, initial_equity: float):
        """开启或重置新会话"""
        now_ms = int(time.time() * 1000)
        self.is_running = True
        self.session_id = f"auto_{'live' if self.is_live_mode else 'paper'}_{int(time.time())}_{uuid.uuid4().hex[:4]}"
        self.start_time = now_ms
        self.stop_time = None
        self.start_equity = max(10.0, initial_equity)
        self.current_equity = self.start_equity
        self.peak_equity = self.start_equity
        self.max_drawdown_pct = 0.0

        # 清理运行时缓存
        self.snapshots.clear()
        self.trades.clear()
        self._executed_plan_ids.clear()
        self._breakeven_applied_positions.clear()
        self.position_scale_records.clear()
        self.last_scale_timestamps.clear()

        # 注入首个快照
        self.snapshots.append(
            AutoPilotHistoryPoint(
                timestamp=now_ms,
                equity=self.start_equity,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                total_pnl=0.0,
                pnl_ratio=0.0,
            )
        )


class AutoPilotManager:
    """
    自动操盘中枢与双环境隔离调度引擎 (单例)
    管理模拟盘与实盘两个完全独立的 AutoPilotEnvState，
    支持 7x24h 后端常驻、环境切换自动安全停机、配置双向记忆与独立收益曲线
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self._is_patrolling: bool = False
        self._active_is_live: bool = False  # 当前激活的环境模式 (False: 模拟盘, True: 实盘)

        # 初始化两套彻底隔离的操盘状态
        self._states: Dict[bool, AutoPilotEnvState] = {
            False: AutoPilotEnvState(is_live_mode=False),
            True: AutoPilotEnvState(is_live_mode=True),
        }

        # 默认模拟盘初始可以处于就绪状态
        self._states[False].is_running = False
        self._states[True].is_running = False

        logger.info("【自动操盘中枢】初始化完成，模拟盘与实盘双环境状态机已建立")

    @property
    def active_state(self) -> AutoPilotEnvState:
        """获取当前激活环境的状态对象"""
        return self._states[self._active_is_live]

    def get_state(self, is_live_mode: Optional[bool] = None) -> AutoPilotEnvState:
        """根据传参或当前环境获取状态"""
        if is_live_mode is None:
            return self.active_state
        return self._states[bool(is_live_mode)]

    # ---------------- 兼容外部调用的属性代理 ----------------
    @property
    def is_running(self) -> bool:
        return self.active_state.is_running

    @is_running.setter
    def is_running(self, val: bool):
        self.active_state.is_running = val

    @property
    def is_live_mode(self) -> bool:
        return self._active_is_live

    @is_live_mode.setter
    def is_live_mode(self, val: bool):
        self._active_is_live = bool(val)

    @property
    def session_id(self) -> str:
        return self.active_state.session_id

    @property
    def symbol(self) -> str:
        return self.active_state.symbol

    @symbol.setter
    def symbol(self, val: str):
        self.active_state.symbol = val

    @property
    def focus_symbols(self) -> List[str]:
        return self.active_state.focus_symbols

    @focus_symbols.setter
    def focus_symbols(self, val: List[str]):
        self.active_state.focus_symbols = val

    @property
    def symbols(self) -> List[str]:
        return self.active_state.symbols

    @symbols.setter
    def symbols(self, val: List[str]):
        self.active_state.symbols = val

    @property
    def focus_mode(self) -> str:
        return self.active_state.focus_mode

    @focus_mode.setter
    def focus_mode(self, val: str):
        self.active_state.focus_mode = val

    @property
    def current_equity(self) -> float:
        return self.active_state.current_equity

    @current_equity.setter
    def current_equity(self, val: float):
        self.active_state.current_equity = val

    @property
    def start_equity(self) -> float:
        return self.active_state.start_equity

    @property
    def peak_equity(self) -> float:
        return self.active_state.peak_equity

    @property
    def max_drawdown_pct(self) -> float:
        return self.active_state.max_drawdown_pct

    @property
    def snapshots(self) -> List[AutoPilotHistoryPoint]:
        return self.active_state.snapshots

    @property
    def trades(self) -> List[AutoPilotTradeRecord]:
        return self.active_state.trades

    @property
    def last_patrol_log(self) -> Optional[str]:
        return self.active_state.last_patrol_log

    @last_patrol_log.setter
    def last_patrol_log(self, val: Optional[str]):
        self.active_state.last_patrol_log = val

    @property
    def last_patrol_time(self) -> Optional[int]:
        return self.active_state.last_patrol_time

    @last_patrol_time.setter
    def last_patrol_time(self, val: Optional[int]):
        self.active_state.last_patrol_time = val

    @property
    def max_scale_layers(self) -> int:
        return self.active_state.max_scale_layers

    @property
    def scale_in_strategy(self) -> str:
        return self.active_state.scale_in_strategy

    @property
    def auto_reversal_enabled(self) -> bool:
        return self.active_state.auto_reversal_enabled

    @property
    def min_confidence(self) -> float:
        return self.active_state.min_confidence

    @property
    def auto_leverage_enabled(self) -> bool:
        return self.active_state.auto_leverage_enabled

    @property
    def stop_loss_equity_floor(self) -> Optional[float]:
        return self.active_state.stop_loss_equity_floor

    @property
    def max_loss_limit_usdt(self) -> Optional[float]:
        return self.active_state.max_loss_limit_usdt

    @property
    def trailing_stop_enabled(self) -> bool:
        return self.active_state.trailing_stop_enabled

    @property
    def breakeven_trigger_ratio(self) -> float:
        return self.active_state.breakeven_trigger_ratio

    @property
    def max_concurrent_positions(self) -> int:
        return self.active_state.max_concurrent_positions

    @property
    def live_state(self) -> EnvRuntimeState:
        """实盘自动操盘状态引用 (供 ReconciliationService 对账使用)"""
        return self._states[True]

    @property
    def paper_state(self) -> EnvRuntimeState:
        """模拟盘自动操盘状态引用 (供 ReconciliationService 对账使用)"""
        return self._states[False]

    # ---------------- 核心生命周期与操作 ----------------

    async def switch_environment(
        self,
        target_live_mode: bool,
        current_equity: Optional[float] = None
    ) -> AutoPilotStatusModel:
        """
        切换系统模式时的联动与安全停机：
        1. 若原环境正在运行，立即执行安全暂停 (stop) 并持久化
        2. 切换当前活跃环境至 target_live_mode
        3. 新环境默认处于停止状态，保留其历史和上次配置，等待交易员重新显式开启
        """
        async with self._lock:
            old_is_live = self._active_is_live
            target_is_live = bool(target_live_mode)

            # 1. 停机旧环境 (如果旧环境正在运行)
            old_state = self._states[old_is_live]
            if old_state.is_running:
                now_ms = int(time.time() * 1000)
                old_state.is_running = False
                old_state.stop_time = now_ms
                if current_equity is not None:
                    old_state.current_equity = current_equity
                old_state.last_patrol_log = f"已因切换至{'实盘' if target_is_live else '模拟'}模式而安全自动暂停操盘"
                try:
                    async with get_db_session() as session:
                        await session.execute(
                            text(
                                """
                                UPDATE auto_pilot_sessions 
                                SET is_running = 0, stop_time = :st, current_equity = :ceq, updated_at = CURRENT_TIMESTAMP
                                WHERE session_id = :sess
                                """
                            ),
                            {
                                "st": now_ms,
                                "ceq": old_state.current_equity,
                                "sess": old_state.session_id,
                            },
                        )
                except Exception as e:
                    logger.warning(f"【自动操盘中枢】保存旧环境停止状态失败: {e}")

                logger.info(
                    f"🛑 【安全停机】模式切换触发：已自动暂停原{'实盘' if old_is_live else '模拟'}环境自动操盘 (Session: {old_state.session_id})"
                )

            # 2. 切换激活环境
            self._active_is_live = target_is_live
            new_state = self._states[target_is_live]

            # 保证新环境处于停止状态，需交易员重新确认开启
            new_state.is_running = False
            new_state.last_patrol_log = f"已切换至{'实盘' if target_is_live else '模拟'}模式，请确认策略参数后点击开启操盘"

            logger.info(
                f"🔀 【环境切换】自动操盘活跃环境已切换为: {'🔴 实盘 (LIVE)' if target_is_live else '🟢 模拟 (DEMO)'}"
            )

            # 3. 异步确保 WebSocket 订阅关注标的
            try:
                from state import ws_manager
                for target_sym in new_state.focus_symbols:
                    asyncio.create_task(ws_manager.subscribe_market_symbol(target_sym))
            except Exception:
                pass

            return self.get_status(current_equity=new_state.current_equity, is_live_mode=target_is_live)

    async def start(
        self,
        initial_equity: float,
        is_live_mode: Optional[bool] = None,
        aggressiveness_mode: Optional[str] = "BALANCED",
        symbol: str = "BTC-USDT-SWAP",
        symbols: Optional[List[str]] = None,
        focus_symbols: Optional[List[str]] = None,
        focus_mode: Optional[str] = None,
        max_scale_layers: Optional[int] = None,
        scale_in_strategy: Optional[str] = None,
        auto_reversal_enabled: Optional[bool] = None,
        max_concurrent_positions: Optional[int] = None,
        trailing_stop_enabled: Optional[bool] = None,
        breakeven_trigger_ratio: Optional[float] = None,
        trailing_profit_threshold: Optional[float] = None,
        trailing_profit_callback: Optional[float] = None,
        reentry_cooldown_seconds: Optional[int] = None,
        funding_fee_guard_enabled: Optional[bool] = None,
        min_confidence: float = 0.70,
        auto_leverage_enabled: bool = True,
        stop_loss_equity_floor: Optional[float] = None,
        max_loss_limit_usdt: Optional[float] = None,
        force_reset: bool = False,
    ) -> AutoPilotStatusModel:
        """
        开启或恢复指定环境的自动量化操盘会话 (7x24h 后端常驻)
        """
        async with self._lock:
            effective_is_live = self._active_is_live if is_live_mode is None else bool(is_live_mode)
            self._active_is_live = effective_is_live
            state = self._states[effective_is_live]

            # 若已有历史曲线且非强制重置，则保留连续时序曲线与历史战绩，仅恢复运行状态
            if force_reset or (len(state.snapshots) <= 1 and len(state.trades) == 0):
                state.reset_for_start(initial_equity=initial_equity)
            else:
                state.is_running = True
                state.stop_time = None
                if state.start_equity <= 0 and initial_equity > 0:
                    state.start_equity = max(10.0, initial_equity)
                state.current_equity = initial_equity if initial_equity > 0 else state.current_equity
            if aggressiveness_mode:
                state.apply_aggressiveness_preset(aggressiveness_mode)
            state.symbol = symbol

            # 资金保护底线设定
            if stop_loss_equity_floor is not None and stop_loss_equity_floor > 0:
                state.stop_loss_equity_floor = min(state.start_equity, stop_loss_equity_floor)
                state.max_loss_limit_usdt = max(0.0, state.start_equity - state.stop_loss_equity_floor)
            elif max_loss_limit_usdt is not None and max_loss_limit_usdt > 0:
                state.max_loss_limit_usdt = max_loss_limit_usdt
                state.stop_loss_equity_floor = max(0.0, state.start_equity - max_loss_limit_usdt)
            else:
                state.stop_loss_equity_floor = None
                state.max_loss_limit_usdt = None

            effective_symbols = focus_symbols or symbols
            if effective_symbols and len(effective_symbols) > 0:
                cleaned = [s.strip().upper() for s in effective_symbols if s.strip()]
                state.focus_symbols = cleaned
                state.symbols = cleaned

            if focus_mode is not None:
                state.focus_mode = focus_mode
            if max_scale_layers is not None:
                state.max_scale_layers = max(1, min(5, max_scale_layers))
            if scale_in_strategy is not None:
                state.scale_in_strategy = scale_in_strategy
            if auto_reversal_enabled is not None:
                state.auto_reversal_enabled = auto_reversal_enabled

            if max_concurrent_positions is not None:
                state.max_concurrent_positions = max(1, min(10, max_concurrent_positions))
            if trailing_stop_enabled is not None:
                state.trailing_stop_enabled = trailing_stop_enabled
            if breakeven_trigger_ratio is not None:
                state.breakeven_trigger_ratio = breakeven_trigger_ratio
            if trailing_profit_threshold is not None:
                state.trailing_profit_threshold = trailing_profit_threshold
            if trailing_profit_callback is not None:
                state.trailing_profit_callback = trailing_profit_callback
            if reentry_cooldown_seconds is not None:
                state.reentry_cooldown_seconds = reentry_cooldown_seconds
            if funding_fee_guard_enabled is not None:
                state.funding_fee_guard_enabled = funding_fee_guard_enabled

            state.min_confidence = min_confidence
            state.auto_leverage_enabled = auto_leverage_enabled
            state.last_patrol_log = f"【{'实盘' if effective_is_live else '模拟'}】7x24h 自动操盘已启动"

            # 异步确保 WebSocket 订阅所有关注标的
            try:
                from state import ws_manager
                for target_sym in state.focus_symbols:
                    asyncio.create_task(ws_manager.subscribe_market_symbol(target_sym))
            except Exception as ws_sub_err:
                logger.debug(f"自动操盘订阅标的行情异常: {ws_sub_err}")

            # 异步写入数据库
            try:
                async with get_db_session() as session:
                    await session.execute(
                        text(
                            """
                            INSERT INTO auto_pilot_sessions 
                            (session_id, is_live_mode, is_running, start_time, start_equity, current_equity, 
                             symbol, min_confidence, auto_leverage)
                            VALUES (:sess, :live, 1, :st, :seq, :ceq, :sym, :conf, :lev)
                            """
                        ),
                        {
                            "sess": state.session_id,
                            "live": 1 if state.is_live_mode else 0,
                            "st": state.start_time,
                            "seq": state.start_equity,
                            "ceq": state.current_equity,
                            "sym": state.symbol,
                            "conf": state.min_confidence,
                            "lev": 1 if state.auto_leverage_enabled else 0,
                        },
                    )
            except Exception as e:
                logger.warning(f"【自动操盘中枢】写入数据库会话失败: {e}")

            logger.info(
                f"【自动操盘中枢】{'实盘' if effective_is_live else '模拟'} 7x24h 自动操盘已启动！Session: {state.session_id}, "
                f"模式: {state.focus_mode}, 主标的: {state.symbol}, 关注池: {state.focus_symbols}, "
                f"加仓层数上限: {state.max_scale_layers}层 ({state.scale_in_strategy}), 自动反手: {state.auto_reversal_enabled}"
            )

            return self.get_status(current_equity=initial_equity, unrealized_pnl=0.0, is_live_mode=effective_is_live)

    async def stop(
        self,
        current_equity: Optional[float] = None,
        is_live_mode: Optional[bool] = None,
    ) -> AutoPilotStatusModel:
        """暂停 / 停止自动操盘"""
        async with self._lock:
            effective_is_live = self._active_is_live if is_live_mode is None else bool(is_live_mode)
            state = self._states[effective_is_live]
            now_ms = int(time.time() * 1000)

            state.is_running = False
            state.stop_time = now_ms
            if current_equity is not None:
                state.current_equity = current_equity
            state.last_patrol_log = f"【{'实盘' if effective_is_live else '模拟'}】自动操盘已手动暂停"

            try:
                async with get_db_session() as session:
                    await session.execute(
                        text(
                            """
                            UPDATE auto_pilot_sessions 
                            SET is_running = 0, stop_time = :st, current_equity = :ceq, updated_at = CURRENT_TIMESTAMP
                            WHERE session_id = :sess
                            """
                        ),
                        {
                            "st": state.stop_time,
                            "ceq": state.current_equity,
                            "sess": state.session_id,
                        },
                    )
            except Exception as e:
                logger.warning(f"【自动操盘中枢】更新停止状态失败: {e}")

            logger.info(f"【自动操盘中枢】已暂停{'实盘' if effective_is_live else '模拟'}自动操盘。Session: {state.session_id}")
            return self.get_status(current_equity=state.current_equity, unrealized_pnl=0.0, is_live_mode=effective_is_live)

    async def update_config(
        self,
        aggressiveness_mode: Optional[str] = None,
        min_confidence: Optional[float] = None,
        auto_leverage_enabled: Optional[bool] = None,
        symbol: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        focus_symbols: Optional[List[str]] = None,
        focus_mode: Optional[str] = None,
        max_scale_layers: Optional[int] = None,
        scale_in_strategy: Optional[str] = None,
        auto_reversal_enabled: Optional[bool] = None,
        max_concurrent_positions: Optional[int] = None,
        trailing_stop_enabled: Optional[bool] = None,
        breakeven_trigger_ratio: Optional[float] = None,
        trailing_profit_threshold: Optional[float] = None,
        trailing_profit_callback: Optional[float] = None,
        reentry_cooldown_seconds: Optional[int] = None,
        funding_fee_guard_enabled: Optional[bool] = None,
        stop_loss_equity_floor: Optional[float] = None,
        max_loss_limit_usdt: Optional[float] = None,
        is_live_mode: Optional[bool] = None,
    ):
        """更新并记忆指定环境的策略配置"""
        async with self._lock:
            state = self.get_state(is_live_mode)

            if aggressiveness_mode is not None:
                state.apply_aggressiveness_preset(aggressiveness_mode)

            if min_confidence is not None:
                state.min_confidence = min_confidence
            if auto_leverage_enabled is not None:
                state.auto_leverage_enabled = auto_leverage_enabled
            if symbol is not None:
                state.symbol = symbol

            if stop_loss_equity_floor is not None and stop_loss_equity_floor > 0:
                state.stop_loss_equity_floor = min(state.start_equity, stop_loss_equity_floor)
                state.max_loss_limit_usdt = max(0.0, state.start_equity - state.stop_loss_equity_floor)
            elif max_loss_limit_usdt is not None and max_loss_limit_usdt > 0:
                state.max_loss_limit_usdt = max_loss_limit_usdt
                state.stop_loss_equity_floor = max(0.0, state.start_equity - max_loss_limit_usdt)

            effective_symbols = focus_symbols or symbols
            if effective_symbols is not None and len(effective_symbols) > 0:
                cleaned = [s.strip().upper() for s in effective_symbols if s.strip()]
                state.focus_symbols = cleaned
                state.symbols = cleaned
                try:
                    from state import ws_manager
                    for target_sym in cleaned:
                        asyncio.create_task(ws_manager.subscribe_market_symbol(target_sym))
                except Exception:
                    pass

            if focus_mode is not None:
                state.focus_mode = focus_mode
            if max_scale_layers is not None:
                state.max_scale_layers = max(1, min(5, max_scale_layers))
            if scale_in_strategy is not None:
                state.scale_in_strategy = scale_in_strategy
            if auto_reversal_enabled is not None:
                state.auto_reversal_enabled = auto_reversal_enabled

            if max_concurrent_positions is not None:
                state.max_concurrent_positions = max(1, min(10, max_concurrent_positions))
            if trailing_stop_enabled is not None:
                state.trailing_stop_enabled = trailing_stop_enabled
            if breakeven_trigger_ratio is not None:
                state.breakeven_trigger_ratio = breakeven_trigger_ratio
            if trailing_profit_threshold is not None:
                state.trailing_profit_threshold = trailing_profit_threshold
            if trailing_profit_callback is not None:
                state.trailing_profit_callback = trailing_profit_callback
            if reentry_cooldown_seconds is not None:
                state.reentry_cooldown_seconds = reentry_cooldown_seconds
            if funding_fee_guard_enabled is not None:
                state.funding_fee_guard_enabled = funding_fee_guard_enabled

            logger.info(
                f"【自动操盘中枢】{'实盘' if state.is_live_mode else '模拟'}配置已更新并记忆: 激进程度={state.aggressiveness_mode}, "
                f"模式={state.focus_mode}, 主标的={state.symbol}, 关注池={state.focus_symbols}, "
                f"置信度={state.min_confidence}, 跟踪止盈门槛={state.trailing_profit_threshold}, "
                f"回撤止盈={state.trailing_profit_callback}, 冷却={state.reentry_cooldown_seconds}s"
            )

    # ---------------- 收益采样与交易战绩记录 ----------------

    async def record_snapshot(
        self,
        current_equity: float,
        realized_pnl: float,
        unrealized_pnl: float,
        is_live_mode: Optional[bool] = None,
    ):
        """时序采样记录资产与收益曲线点 (分别写入对应环境)"""
        state = self.get_state(is_live_mode)
        if not state.is_running and len(state.snapshots) > 0:
            return

        async with self._lock:
            now_ms = int(time.time() * 1000)
            state.current_equity = current_equity

            # 动态更新峰值与回撤
            if current_equity > state.peak_equity:
                state.peak_equity = current_equity
            elif state.peak_equity > 0:
                dd = (state.peak_equity - current_equity) / state.peak_equity * 100.0
                if dd > state.max_drawdown_pct:
                    state.max_drawdown_pct = dd

            total_pnl = current_equity - state.start_equity
            pnl_ratio = (total_pnl / state.start_equity) if state.start_equity > 0 else 0.0

            point = AutoPilotHistoryPoint(
                timestamp=now_ms,
                equity=round(current_equity, 2),
                realized_pnl=round(realized_pnl, 2),
                unrealized_pnl=round(unrealized_pnl, 2),
                total_pnl=round(total_pnl, 2),
                pnl_ratio=round(pnl_ratio, 4),
            )

            if len(state.snapshots) >= 5000:
                state.snapshots.pop(0)
            state.snapshots.append(point)

            try:
                async with get_db_session() as session:
                    await session.execute(
                        text(
                            """
                            INSERT INTO auto_pilot_pnl_snapshots 
                            (session_id, timestamp, equity, realized_pnl, unrealized_pnl, total_pnl, pnl_ratio)
                            VALUES (:sess, :ts, :eq, :rpnl, :upnl, :tpnl, :ratio)
                            """
                        ),
                        {
                            "sess": state.session_id,
                            "ts": now_ms,
                            "eq": current_equity,
                            "rpnl": realized_pnl,
                            "upnl": unrealized_pnl,
                            "tpnl": total_pnl,
                            "ratio": pnl_ratio,
                        },
                    )
            except Exception:
                pass

    async def record_trade(
        self,
        symbol: str,
        pos_side: str,
        entry_price: float,
        exit_price: float,
        size: float,
        leverage: int,
        realized_pnl: float,
        roi_pct: float,
        reason: str = "AI策略自动平仓",
        order_id: Optional[str] = None,
        is_live_mode: Optional[bool] = None,
    ) -> AutoPilotTradeRecord:
        """记录自动操盘产生的成交平仓 (写入对应环境)"""
        async with self._lock:
            state = self.get_state(is_live_mode)
            now_ms = int(time.time() * 1000)
            record_id = f"apt_{now_ms}_{uuid.uuid4().hex[:4]}"
            trade_obj = AutoPilotTradeRecord(
                id=record_id,
                symbol=symbol,
                pos_side=pos_side,
                entry_price=entry_price,
                exit_price=exit_price,
                size=size,
                leverage=leverage,
                realized_pnl=realized_pnl,
                roi_pct=roi_pct,
                reason=reason,
                timestamp=now_ms,
            )
            state.trades.insert(0, trade_obj)

            # 若产生止损/亏损平仓，自动同步风控中枢并触发单币种 30 分钟防洗盘追单冷静期
            if realized_pnl < 0:
                try:
                    from state import risk_controller
                    if risk_controller:
                        risk_controller.record_closed_trade_pnl(realized_pnl, reason)
                        risk_controller.record_symbol_stop_loss(symbol, state.stop_loss_cooldown_minutes, reason)
                except Exception as rc_err:
                    logger.debug(f"风控止损记录同步跳过: {rc_err}")

            # 触发多智能体强化学习自演进与 AI 员工战绩闭环更新
            try:
                import sys, os
                ai_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "okx-dog-ai"))
                if ai_path not in sys.path:
                    sys.path.insert(0, ai_path)
                from agent.evolution.evolution_manager import AgentEvolutionManager
                evo_mgr = AgentEvolutionManager.get_instance()
                evo_mgr.process_trade_reinforcement(
                    trade_id=order_id or record_id,
                    symbol=symbol,
                    pos_side=pos_side,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    sl_price=exit_price if ("止损" in reason or "SL" in reason) else entry_price * 0.985,
                    realized_pnl=realized_pnl,
                    is_crisis_defended=("保本" in reason or "防守" in reason or "排雷" in reason or "风控" in reason)
                )
            except Exception as evo_err:
                logger.warning(f"【自动操盘中枢】AI员工自演进联动跳过: {evo_err}")

            # 同步 CFO 战绩激励引擎：派发或核销员工算力与深度思考奖励
            try:
                from agent.evolution.cfo_performance_manager import CfoPerformanceManager
                cfo = CfoPerformanceManager.get_instance()
                if realized_pnl > 0:
                    cfo.reward_trade_profit(
                        trade_id=order_id or record_id,
                        pnl_usdt=realized_pnl,
                        side="LONG" if "LONG" in str(pos_side).upper() else "SHORT"
                    )
                elif ("保本" in reason or "防守" in reason or "排雷" in reason or "风控" in reason):
                    cfo.record_successful_hedge(symbol=symbol, reason=reason)
            except Exception as cfo_err:
                logger.debug(f"【自动操盘中枢】CFO战绩结算联动跳过: {cfo_err}")


            # 持久化到 SQLite
            try:
                async with get_db_session() as session:
                    await session.execute(
                        text(
                            """
                            INSERT INTO auto_pilot_trades 
                            (session_id, order_id, symbol, pos_side, entry_price, exit_price, 
                             size, leverage, realized_pnl, roi_pct, reason, timestamp)
                            VALUES (:sess, :oid, :sym, :side, :ep, :xp, :sz, :lev, :rpnl, :roi, :rsn, :ts)
                            """
                        ),
                        {
                            "sess": state.session_id,
                            "oid": order_id or record_id,
                            "sym": symbol,
                            "side": pos_side,
                            "ep": entry_price,
                            "xp": exit_price,
                            "sz": size,
                            "lev": leverage,
                            "rpnl": realized_pnl,
                            "roi": roi_pct,
                            "rsn": reason,
                            "ts": now_ms,
                        },
                    )
            except Exception as e:
                logger.warning(f"【自动操盘中枢】写入自动成交记录失败: {e}")

            logger.info(
                f"【自动操盘战绩 ({'实盘' if state.is_live_mode else '模拟'})】已记录自动平仓: {symbol} {pos_side}, "
                f"已实现盈亏: {'+' if realized_pnl >= 0 else ''}{realized_pnl:.2f} USDT ({roi_pct:+.2f}%)"
            )
            return trade_obj

    # ---------------- 状态与历史查询 ----------------

    def get_status(
        self,
        current_equity: Optional[float] = None,
        unrealized_pnl: float = 0.0,
        is_live_mode: Optional[bool] = None,
    ) -> AutoPilotStatusModel:
        """获取指定环境开启操盘以来的实时收益统计全景与配置 (100% 隔离)"""
        state = self.get_state(is_live_mode)
        now_ms = int(time.time() * 1000)
        eff_current_equity = current_equity if (current_equity is not None and current_equity > 0) else state.current_equity
        state.current_equity = eff_current_equity

        # 计算运行时长 (秒)
        if state.is_running:
            running_secs = max(0, int((now_ms - state.start_time) / 1000))
        elif state.stop_time and state.start_time:
            running_secs = max(0, int((state.stop_time - state.start_time) / 1000))
        else:
            running_secs = 0

        # 计算累计已实现盈亏 (仅统计属于该环境的 trades)
        tot_realized = sum(t.realized_pnl for t in state.trades)
        total_pnl = eff_current_equity - state.start_equity
        pnl_ratio = (total_pnl / state.start_equity) if state.start_equity > 0 else 0.0

        # 胜率与盈亏比
        win_trades = [t for t in state.trades if t.realized_pnl > 0]
        loss_trades = [t for t in state.trades if t.realized_pnl < 0]
        tot_trades_cnt = len(state.trades)
        win_cnt = len(win_trades)
        loss_cnt = len(loss_trades)

        win_rate = (win_cnt / tot_trades_cnt) if tot_trades_cnt > 0 else 0.0
        total_gain = sum(t.realized_pnl for t in win_trades)
        total_loss = abs(sum(t.realized_pnl for t in loss_trades))
        profit_factor = (total_gain / total_loss) if total_loss > 0 else (2.5 if total_gain > 0 else 0.0)

        # 动态回撤更新
        if eff_current_equity > state.peak_equity:
            state.peak_equity = eff_current_equity
        elif state.peak_equity > 0:
            dd = (state.peak_equity - eff_current_equity) / state.peak_equity * 100.0
            if dd > state.max_drawdown_pct:
                state.max_drawdown_pct = dd

        return AutoPilotStatusModel(
            is_running=state.is_running,
            session_id=state.session_id,
            start_time=state.start_time,
            running_seconds=running_secs,
            start_equity_usdt=round(state.start_equity, 2),
            current_equity_usdt=round(eff_current_equity, 2),
            total_pnl_usdt=round(total_pnl, 2),
            total_pnl_ratio=round(pnl_ratio, 4),
            realized_pnl_usdt=round(tot_realized, 2),
            unrealized_pnl_usdt=round(unrealized_pnl, 2),
            total_trades_count=tot_trades_cnt,
            winning_trades_count=win_cnt,
            losing_trades_count=loss_cnt,
            win_rate=round(win_rate, 4),
            profit_factor=round(profit_factor, 2),
            max_drawdown_pct=round(state.max_drawdown_pct, 2),
            active_symbol=state.symbol,
            symbols=list(state.symbols),
            focus_symbols=list(state.focus_symbols),
            focus_mode=state.focus_mode,
            max_scale_layers=state.max_scale_layers,
            scale_in_strategy=state.scale_in_strategy,
            auto_reversal_enabled=state.auto_reversal_enabled,
            max_concurrent_positions=state.max_concurrent_positions,
            trailing_stop_enabled=state.trailing_stop_enabled,
            breakeven_trigger_ratio=state.breakeven_trigger_ratio,
            trailing_profit_threshold=state.trailing_profit_threshold,
            trailing_profit_callback=state.trailing_profit_callback,
            reentry_cooldown_seconds=state.reentry_cooldown_seconds,
            funding_fee_guard_enabled=getattr(state, "funding_fee_guard_enabled", True),
            min_confidence=state.min_confidence,
            auto_leverage_enabled=state.auto_leverage_enabled,
            stop_loss_equity_floor=state.stop_loss_equity_floor,
            max_loss_limit_usdt=state.max_loss_limit_usdt,
            aggressiveness_mode=state.aggressiveness_mode,
            is_live_mode=state.is_live_mode,
            last_patrol_time=state.last_patrol_time,
            last_patrol_log=state.last_patrol_log,
            last_updated_at=now_ms,
        )

    def get_history(self, is_live_mode: Optional[bool] = None) -> AutoPilotHistoryResponse:
        """获取指定环境的时序收益曲线与交易流水 (100% 隔离)"""
        state = self.get_state(is_live_mode)
        return AutoPilotHistoryResponse(
            session_id=state.session_id,
            is_running=state.is_running,
            curve=list(state.snapshots),
            trades=list(state.trades),
        )

    async def get_history_async(
        self,
        is_live_mode: Optional[bool] = None,
        limit: int = 3000
    ) -> AutoPilotHistoryResponse:
        """获取指定环境的完整时序收益曲线与交易流水 (若内存刚重启则自动从 SQLite 恢复长周期历史)"""
        state = self.get_state(is_live_mode)
        eff_live = state.is_live_mode
        sess_prefix = "auto_live_%" if eff_live else "auto_paper_%"

        if len(state.snapshots) <= 2:
            try:
                async with get_db_session() as session:
                    res = await session.execute(
                        text(
                            """
                            SELECT timestamp, equity, realized_pnl, unrealized_pnl, total_pnl, pnl_ratio
                            FROM auto_pilot_pnl_snapshots
                            WHERE session_id LIKE :prefix
                            ORDER BY timestamp DESC
                            LIMIT :lim
                            """
                        ),
                        {"prefix": sess_prefix, "lim": max(100, min(5000, limit))},
                    )
                    rows = res.mappings().all()
                    if rows and len(rows) > len(state.snapshots):
                        restored = [
                            AutoPilotHistoryPoint(
                                timestamp=int(r["timestamp"]),
                                equity=round(float(r["equity"]), 2),
                                realized_pnl=round(float(r["realized_pnl"]), 2),
                                unrealized_pnl=round(float(r["unrealized_pnl"]), 2),
                                total_pnl=round(float(r["total_pnl"]), 2),
                                pnl_ratio=round(float(r["pnl_ratio"]), 4),
                            )
                            for r in reversed(rows)
                        ]
                        state.snapshots = restored

                    if len(state.trades) == 0:
                        t_res = await session.execute(
                            text(
                                """
                                SELECT order_id, symbol, pos_side, entry_price, exit_price,
                                       size, leverage, realized_pnl, roi_pct, reason, timestamp
                                FROM auto_pilot_trades
                                WHERE session_id LIKE :prefix
                                ORDER BY timestamp DESC
                                LIMIT 200
                                """
                            ),
                            {"prefix": sess_prefix},
                        )
                        t_rows = t_res.mappings().all()
                        if t_rows:
                            state.trades = [
                                AutoPilotTradeRecord(
                                    id=str(tr["order_id"]),
                                    symbol=str(tr["symbol"]),
                                    pos_side=str(tr["pos_side"]),
                                    entry_price=float(tr["entry_price"]),
                                    exit_price=float(tr["exit_price"]),
                                    size=float(tr["size"]),
                                    leverage=int(tr["leverage"]),
                                    realized_pnl=float(tr["realized_pnl"]),
                                    roi_pct=float(tr["roi_pct"]),
                                    reason=str(tr["reason"] or "AI策略自动平仓"),
                                    timestamp=int(tr["timestamp"]),
                                )
                                for tr in t_rows
                            ]
            except Exception as db_err:
                logger.debug(f"从数据库恢复历史时序曲线跳过: {db_err}")

        return AutoPilotHistoryResponse(
            session_id=state.session_id,
            is_running=state.is_running,
            curve=list(state.snapshots[-limit:]),
            trades=list(state.trades[:200]),
        )

    async def reset_stats(
        self,
        current_equity: float,
        is_live_mode: Optional[bool] = None
    ) -> AutoPilotStatusModel:
        """重置收益统计基准（将当前资金设为新基准）"""
        state = self.get_state(is_live_mode)
        return await self.start(
            initial_equity=current_equity,
            is_live_mode=state.is_live_mode,
            symbol=state.symbol,
            symbols=state.symbols,
            focus_symbols=state.focus_symbols,
            focus_mode=state.focus_mode,
            max_scale_layers=state.max_scale_layers,
            scale_in_strategy=state.scale_in_strategy,
            auto_reversal_enabled=state.auto_reversal_enabled,
            max_concurrent_positions=state.max_concurrent_positions,
            trailing_stop_enabled=state.trailing_stop_enabled,
            breakeven_trigger_ratio=state.breakeven_trigger_ratio,
            min_confidence=state.min_confidence,
            auto_leverage_enabled=state.auto_leverage_enabled,
            stop_loss_equity_floor=state.stop_loss_equity_floor,
            max_loss_limit_usdt=state.max_loss_limit_usdt,
            force_reset=True,
        )

    # ---------------- 资深投资研究员: 标的深度尽调与优选排序 ----------------

    async def rank_investment_targets_by_research(
        self,
        targets: List[str],
        market_regime: str = "TRENDING_UP"
    ) -> List[Tuple[str, float, str]]:
        """
        资深投资研究员 (agency-investment-researcher) 资产尽调与标的动态优选排序
        针对候选关注池中的标的，根据市场体制契合度、24h 涨跌动量、波动率舒适度进行深度综合打分
        :return: List of (symbol, research_score, commentary) 降序排列
        """
        if not targets:
            return []

        from state import ws_manager, paper_engine

        scored_targets = []
        regime = market_regime.upper() if market_regime else "TRENDING_UP"

        for sym in targets:
            base_score = 50.0
            reasons = []

            # 1. 尝试从 WebSocket 或模拟沙盒获取行情快照
            cur_tk = ws_manager.latest_tickers.get(sym) if ws_manager else None
            if cur_tk and cur_tk.last_price > 0:
                last_px = cur_tk.last_price
                if getattr(cur_tk, "change_24h_pct", None) is not None:
                    chg_24h = float(cur_tk.change_24h_pct)
                elif cur_tk.open_24h > 0:
                    chg_24h = (last_px - cur_tk.open_24h) / cur_tk.open_24h * 100.0
                else:
                    chg_24h = 0.0
            else:
                last_px = paper_engine.last_price_map.get(sym, 100.0) if paper_engine else 100.0
                chg_24h = 0.0

            # 2. 市场体制契合度深度尽调
            if regime == "TRENDING_UP":
                # 单边多头体制: 优选突破顺势动量最强 (+1.5% ~ +12%)
                if 1.5 <= chg_24h <= 12.0:
                    base_score += 25.0
                    reasons.append(f"多头领涨顺势突破(+{chg_24h:.1f}%)")
                elif chg_24h > 12.0:
                    base_score += 10.0
                    reasons.append(f"超买逼空谨慎(+{chg_24h:.1f}%)")
                elif chg_24h < -1.0:
                    base_score -= 20.0
                    reasons.append(f"逆势阴跌弱势({chg_24h:.1f}%)")
                else:
                    base_score += 5.0
            elif regime == "TRENDING_DOWN":
                # 单边空头体制: 优选破位领跌 (-1.5% ~ -15%)
                if -15.0 <= chg_24h <= -1.5:
                    base_score += 25.0
                    reasons.append(f"空头顺势破位领跌({chg_24h:.1f}%)")
                elif chg_24h > 2.0:
                    base_score -= 20.0
                    reasons.append(f"逆势抗跌({chg_24h:.1f}%)")
                else:
                    base_score += 5.0
            elif regime == "RANGING":
                # 箱体震荡体制: 优选振幅收敛稳定 (±2.5% 以内)
                if abs(chg_24h) <= 2.5:
                    base_score += 25.0
                    reasons.append(f"箱体窄幅整理(±{abs(chg_24h):.1f}%)")
                elif abs(chg_24h) > 6.0:
                    base_score -= 25.0
                    reasons.append(f"震荡市异常宽幅异动({chg_24h:.1f}%)")
                else:
                    base_score += 10.0
            else:  # VOLATILE_BREAKOUT
                if abs(chg_24h) >= 4.0:
                    base_score += 25.0
                    reasons.append(f"剧烈放量异动突破({chg_24h:.1f}%)")
                else:
                    base_score += 5.0

            # 3. 规避流动性异常或美股休市代币
            is_tradfi = any(t in sym.upper() for t in ["NVDA", "TSLA", "AAPL", "MSFT", "AMZN", "SPY", "QQQ"])
            if is_tradfi and datetime.utcnow().weekday() in (5, 6):
                base_score -= 50.0
                reasons.append("美股周末休市")

            final_score = max(0.0, min(100.0, base_score))
            scored_targets.append((sym, round(final_score, 1), "; ".join(reasons) or "中性观望"))

        # 投资研究员打分降序排列
        scored_targets.sort(key=lambda x: x[1], reverse=True)
        return scored_targets

    # ---------------- 7x24h 后端常驻自主巡检核心循环 ----------------

    async def execute_patrol_step(self):
        """
        后端守护协程调用的单步自主巡检：
        仅对当前激活且处于 is_running=True 的环境执行巡检
        1. 检查运行状态、本金底线保护与硬风控熔断锁
        2. 扫描已有持仓：执行移动保本 (Breakeven)、顺势金字塔加仓 (Pyramiding)、回调补仓 (DCA) 与趋势强反转反手 (Reversal)
        3. 巡检未持仓标的：依据关注标的池执行科学仓位预算初始首仓建仓 (Layer 1)
        """
        state = self.active_state
        if not state.is_running or self._is_patrolling:
            return

        self._is_patrolling = True
        now_ms = int(time.time() * 1000)
        state.last_patrol_time = now_ms

        try:
            from state import (
                paper_engine,
                position_sizer,
                rest_client,
                risk_controller,
                smart_execution_engine,
                ws_manager,
            )
            from services.ai_service import generate_quantitative_rule_decision
            from services.market_service import get_multi_period_indicators_data, get_derivatives_metrics_data

            # 0. 检查系统硬风控状态
            if risk_controller.risk_status.value == "PANIC_LOCKED":
                state.last_patrol_log = "⚠️ 硬风控处于 PANIC_LOCKED 熔断锁定状态，跳过自动操盘巡检"
                return

            is_live = state.is_live_mode

            # 1. 获取当前持仓与资产
            if is_live:
                try:
                    positions = await rest_client.get_positions()
                    bal = await rest_client.get_account_balance()
                    avail_funds = bal.available_margin_usdt if bal.available_margin_usdt > 0 else bal.total_equity_usdt
                    cur_eq = bal.total_equity_usdt
                except Exception as e:
                    state.last_patrol_log = f"实盘账户信息拉取异常: {e}"
                    return
            else:
                positions = paper_engine.get_positions()
                bal = paper_engine.get_account_balance()
                avail_funds = bal.available_margin_usdt if bal.available_margin_usdt > 0 else bal.total_equity_usdt
                cur_eq = bal.total_equity_usdt

            state.current_equity = cur_eq

            # 0.1 核心风控：本金底线保护熔断检查 (Capital Stop-Loss Floor)
            if state.stop_loss_equity_floor is not None and state.stop_loss_equity_floor > 0:
                if cur_eq <= state.stop_loss_equity_floor:
                    log_floor = (
                        f"🛑 【触发本金底线保护】当前总权益 (${cur_eq:.2f} USDT) 触及或低于最低保留底线 "
                        f"(${state.stop_loss_equity_floor:.2f} USDT)！已自动停止 7x24h 自动操盘！"
                    )
                    logger.warning(log_floor)
                    state.last_patrol_log = f"🛑 触及本金底线 (${state.stop_loss_equity_floor:.2f} USDT) 自动停止操盘"
                    await self.stop(current_equity=cur_eq, is_live_mode=is_live)
                    try:
                        from models import WSPushPayload, WSServerEvent
                        await ws_manager.broadcast_to_subscribers(WSPushPayload[AutoPilotStatusModel](
                            channel="auto_pilot",
                            event=WSServerEvent.UPDATE,
                            data=self.get_status(current_equity=cur_eq, is_live_mode=is_live),
                            timestamp=now_ms
                        ))
                    except Exception:
                        pass
                    return

            # 2. 构建持仓字典与持仓标的集合
            held_pos_map: Dict[str, Any] = {}
            for pos in positions:
                if pos.pos > 0:
                    held_pos_map[pos.symbol] = pos

            patrol_summaries = []

            # =========================================================================
            # 阶段 1: 针对已有持仓标的进行深度巡检 (移动保本 / 动态加仓 / 趋势反转反手)
            # =========================================================================
            for sym, pos in list(held_pos_map.items()):
                pos_id = pos.pos_id or f"{sym}_{pos.pos_side}"
                entry_px = pos.avg_px
                cur_tk = ws_manager.latest_tickers.get(sym)
                if not cur_tk or (now_ms - getattr(cur_tk, "timestamp", 0)) > 8000:
                    try:
                        fresh_tk = await rest_client.get_ticker(sym)
                        if fresh_tk and fresh_tk.last_price > 0:
                            cur_tk = fresh_tk
                            ws_manager.latest_tickers[sym] = fresh_tk
                            if not is_live:
                                paper_engine.update_market_price(sym, fresh_tk.last_price)
                    except Exception:
                        pass
                if not is_live:
                    cur_px = (cur_tk.last_price if (cur_tk and cur_tk.last_price > 0) else 0) or paper_engine.last_price_map.get(sym) or pos.mark_px or entry_px
                else:
                    cur_px = (cur_tk.last_price if (cur_tk and cur_tk.last_price > 0) else 0) or pos.mark_px or entry_px

                if cur_px <= 0 or entry_px <= 0:
                    continue

                is_long = pos.pos_side == PosSide.LONG
                profit_ratio = (cur_px - entry_px) / entry_px if is_long else (entry_px - cur_px) / entry_px

                # 动态跟踪并记录持仓生命周期最高价、最高浮盈峰值与阶段1减仓标记 (兼容实盘每次拉取新 Position 对象)
                if sym not in state.position_peaks:
                    state.position_peaks[sym] = {
                        "peak_price": cur_px,
                        "peak_profit_ratio": profit_ratio,
                        "tp_stage1_done": bool(getattr(pos, "tp_triggered_50", False)),
                    }
                else:
                    pk = state.position_peaks[sym]
                    if is_long:
                        pk["peak_price"] = max(pk.get("peak_price", cur_px), cur_px)
                    else:
                        pk["peak_price"] = min(pk.get("peak_price", cur_px), cur_px)
                    pk["peak_profit_ratio"] = max(pk.get("peak_profit_ratio", profit_ratio), profit_ratio)
                    if getattr(pos, "tp_triggered_50", False):
                        pk["tp_stage1_done"] = True

                has_tp_50 = bool(state.position_peaks[sym].get("tp_stage1_done", False))
                pos.tp_triggered_50 = has_tp_50
                peak_profit = state.position_peaks[sym].get("peak_profit_ratio", profit_ratio)

                # 1.1 先行提取多周期指标、衍生品情绪与 AI 量化策略研判 (携带准确的 pos.tp_triggered_50 状态)
                try:
                    multi_ind = await get_multi_period_indicators_data(sym)
                    deriv = await get_derivatives_metrics_data(sym)
                    decision = generate_quantitative_rule_decision(
                        symbol=sym,
                        current_price=cur_px,
                        multi_indicators=multi_ind,
                        derivatives=deriv,
                        risk_limits=risk_controller.rules,
                        position=pos,
                    )
                except Exception as eval_err:
                    logger.debug(f"标的 {sym} 持仓策略计算跳过: {eval_err}")
                    decision = None
                    multi_ind = None
                    deriv = None

                sig = decision.signal if decision else None
                plan = decision.trade_plan if decision else None
                conf = sig.confidence if sig else 0.0
                action = sig.action if sig else SignalAction.HOLD_WAIT

                # 动态计算 ATR 波动率自适应跟踪止盈门槛 (Volatility-Adaptive)
                atr_15m = 0.0
                if multi_ind and "15m" in multi_ind.indicators and multi_ind.indicators["15m"].atr_14:
                    atr_15m = multi_ind.indicators["15m"].atr_14
                atr_ratio = (atr_15m / max(cur_px, 1.0)) if atr_15m > 0 else 0.008
                dynamic_tp_threshold = max(0.012, min(0.035, 1.6 * atr_ratio))
                effective_tp_threshold = max(state.trailing_profit_threshold, dynamic_tp_threshold)

                # 1.2 两阶段阶梯止盈：阶段 1 (未减半状态，平仓 50% 锁定确定性利润 + 移损保本)
                if state.trailing_stop_enabled and not has_tp_50:
                    drawdown_from_peak = peak_profit - profit_ratio
                    callback_limit = max(0.004, peak_profit * state.trailing_profit_callback)
                    is_trailing_hit = (peak_profit >= effective_tp_threshold) and (drawdown_from_peak >= callback_limit or (profit_ratio <= 0.005 and peak_profit >= 0.014))
                    is_ai_close_hit = (action == SignalAction.CLOSE_POSITION and conf >= 0.75 and profit_ratio >= 0.012)

                    if is_trailing_hit or is_ai_close_hit:
                        ct_val, _, _, min_sz = rest_client.get_instrument_spec(sym)
                        min_coin_sz = max(0.0001, ct_val * min_sz)
                        half_sz = max(min_coin_sz, round(pos.pos * 0.5, 5))
                        reason_tp50 = (
                            f"🎯 阶段1阶梯止盈50% (最高浮盈+{peak_profit*100:.2f}%, 现浮盈+{profit_ratio*100:.2f}%, "
                            f"{'AI超买触发' if is_ai_close_hit else '动态追踪触发'})"
                        )
                        logger.info(f"【自动操盘-阶梯止盈50%】{sym} {reason_tp50}, 平仓币数: {half_sz}")
                        try:
                            if is_live:
                                try:
                                    await rest_client.cancel_all_orders(symbol=sym)
                                except Exception:
                                    pass
                                await rest_client.place_order(
                                    symbol=sym,
                                    side=OrderSide.SELL if is_long else OrderSide.BUY,
                                    pos_side=pos.pos_side,
                                    order_type=OrderType.MARKET,
                                    sz=half_sz,
                                    reduce_only=True
                                )
                            else:
                                paper_engine.cancel_all_orders(symbol=sym)
                                paper_engine.close_position(symbol=sym, pos_side=pos.pos_side, price=cur_px, size=half_sz, reason=reason_tp50)

                            # 状态迁移：标记 50% 阶梯已触发，强制移损至「均价+摩擦覆盖缓冲」(True Breakeven Locked)
                            pos.tp_triggered_50 = True
                            pos.is_breakeven_locked = True
                            be_friction_buf = entry_px * 0.0015
                            be_sl_target = float(rest_client._format_order_px(sym, entry_px + be_friction_buf if is_long else entry_px - be_friction_buf))
                            pos.sl_trigger_price = be_sl_target
                            pos.tp_trigger_price = None
                            state._breakeven_applied_positions.add(pos_id)

                            if is_live:
                                try:
                                    rem_sz = max(min_coin_sz, round(pos.pos - half_sz, 5))
                                    await rest_client.place_tpsl_algo_order(
                                        symbol=sym,
                                        pos_side=pos.pos_side,
                                        sz=rem_sz,
                                        sl_trigger_price=be_sl_target
                                    )
                                except Exception as algo_e:
                                    logger.debug(f"实盘阶段1移损保本条件单挂载提示: {algo_e}")
                            else:
                                paper_engine.update_position_stops(
                                    symbol=sym,
                                    pos_side=pos.pos_side,
                                    stop_loss_price=be_sl_target,
                                    take_profit_price=None,
                                    clear_take_profit=True,
                                    is_breakeven_locked=True
                                )

                            # 保持峰值并记录 tp_stage1_done = True，交由阶段2宽幅 ATR 吊灯止盈跟踪剩余 50% 仓位
                            state.position_peaks[sym] = {
                                "peak_price": max(cur_px, state.position_peaks[sym].get("peak_price", cur_px)) if is_long else min(cur_px, state.position_peaks[sym].get("peak_price", cur_px)),
                                "peak_profit_ratio": max(peak_profit, profit_ratio),
                                "tp_stage1_done": True,
                            }
                            patrol_summaries.append(f"🎯{sym}阶梯止盈50%(+{profit_ratio*100:.1f}%)")
                            continue
                        except Exception as tp_err:
                            logger.warning(f"阶段1阶梯止盈异常 {sym}: {tp_err}")

                # 1.3 两阶段阶梯止盈：阶段 2 (已完成50%减半，剩余50%长尾波段博取 - ATR 吊灯宽幅容忍度或触及TP2出清)
                elif state.trailing_stop_enabled and has_tp_50:
                    drawdown_from_peak = peak_profit - profit_ratio
                    # 宽幅波段容忍度：取 1.5倍 15m ATR (至少 0.8% 价格波动空间) 与峰值 45% 回撤的较大值，防止正常震荡秒平剩余半仓
                    wave_dd_limit = max(0.008, 1.5 * atr_ratio, peak_profit * 0.45)
                    tp2_hit = False
                    if plan and plan.take_profit_levels and len(plan.take_profit_levels) > 1:
                        tp2_px = plan.take_profit_levels[-1].price
                        tp2_hit = (cur_px >= tp2_px) if is_long else (cur_px <= tp2_px)

                    is_stage2_ai_exit = (action == SignalAction.CLOSE_POSITION and conf >= 0.80 and profit_ratio >= max(0.024, 2.4 * atr_ratio))
                    is_tail_exit = (
                        (peak_profit >= 0.018 and drawdown_from_peak >= wave_dd_limit)
                        or (profit_ratio <= 0.0020)
                        or tp2_hit
                        or is_stage2_ai_exit
                    )
                    if is_tail_exit:
                        reason_tail = (
                            f"🏁 阶段2长尾波段出清 (最高浮盈+{peak_profit*100:.2f}%, 现浮盈+{profit_ratio*100:.2f}%, "
                            f"{'触及TP2' if tp2_hit else ('ATR吊灯回撤止盈' if drawdown_from_peak >= wave_dd_limit else '回踩保本线')})"
                        )
                        logger.info(f"【自动操盘-波段出清】{sym} {reason_tail}")
                        try:
                            if is_live:
                                try:
                                    await rest_client.cancel_all_orders(symbol=sym)
                                except Exception:
                                    pass
                                await rest_client.close_position(symbol=sym, pos_side=pos.pos_side)
                                est_pnl = (cur_px - entry_px) * pos.pos if is_long else (entry_px - cur_px) * pos.pos
                                await self.record_trade(
                                    symbol=sym,
                                    pos_side=pos.pos_side.value if hasattr(pos.pos_side, "value") else str(pos.pos_side),
                                    entry_price=entry_px,
                                    exit_price=cur_px,
                                    size=pos.pos,
                                    leverage=pos.leverage or 3,
                                    realized_pnl=round(est_pnl, 2),
                                    roi_pct=round(profit_ratio * (pos.leverage or 3) * 100.0, 2),
                                    reason=reason_tail,
                                )
                            else:
                                paper_engine.cancel_all_orders(symbol=sym)
                                paper_engine.close_position(symbol=sym, pos_side=pos.pos_side, price=cur_px, reason=reason_tail)

                            state.position_peaks.pop(sym, None)
                            state.position_scale_records.pop(sym, None)
                            state._breakeven_applied_positions.discard(pos_id)
                            state.last_closed_timestamps[sym] = now_ms
                            patrol_summaries.append(f"🏁{sym}波段出清(+{profit_ratio*100:.1f}%)")
                            continue
                        except Exception as tail_err:
                            logger.warning(f"阶段2波段出清异常 {sym}: {tail_err}")

                # 1.4 常规移动保本锁定 (浮盈超 1.0% 且尚未触发过保本)
                if state.trailing_stop_enabled and pos_id not in state._breakeven_applied_positions:
                    if profit_ratio >= 0.010:
                        state._breakeven_applied_positions.add(pos_id)
                        pos.is_breakeven_locked = True
                        be_friction_buf = entry_px * 0.0015
                        be_sl_target = float(rest_client._format_order_px(sym, entry_px + be_friction_buf if is_long else entry_px - be_friction_buf))
                        pos.sl_trigger_price = be_sl_target
                        if not is_live:
                            paper_engine.update_position_stops(symbol=sym, stop_loss_price=be_sl_target)
                        log_msg = f"🛡️ 【移动保本触发】{sym} 浮盈达 {profit_ratio*100:+.2f}%，止损线已移至超额保本价 ${be_sl_target} (覆盖手续费摩擦)"
                        logger.info(log_msg)
                        patrol_summaries.append(f"{sym}超额保本锁定")

                cur_layers = getattr(pos, "scale_layers", None) or state.position_scale_records.get(sym, 1)

                # 1.5 强反向信号 -> 趋势反转平反开 (Directional Reversal，增加 10 分钟持仓保护期与 0.82 置信度门槛防震荡磨损)
                last_entry_or_scale_ts = state.last_scale_timestamps.get(sym, 0)
                holding_elapsed_ms = now_ms - last_entry_or_scale_ts
                is_opposite_signal = (is_long and action == SignalAction.SELL_SHORT) or ((not is_long) and action == SignalAction.BUY_LONG)
                if state.auto_reversal_enabled and is_opposite_signal and conf >= 0.82 and plan and holding_elapsed_ms >= 600000:
                    try:
                        logger.info(f"🔄 【趋势反转触发】{sym} 当前持仓 {pos.pos_side.value} 遭遇强反向信号 ({action.value}, 置信度:{conf*100:.0f}%)，执行平仓并反手！")
                        if is_live:
                            try:
                                await rest_client.cancel_all_orders(symbol=sym)
                            except Exception:
                                pass
                            await rest_client.close_position(symbol=sym, pos_side=pos.pos_side)
                        else:
                            paper_engine.cancel_all_orders(symbol=sym)
                            paper_engine.close_position(symbol=sym, pos_side=pos.pos_side, price=cur_px, reason=f"趋势反转强信号 ({action.value}) 自动平仓反手")

                        state.position_peaks.pop(sym, None)
                        state._breakeven_applied_positions.discard(pos_id)

                        new_side = OrderSide.SELL if action == SignalAction.SELL_SHORT else OrderSide.BUY
                        new_pos_side = PosSide.SHORT if action == SignalAction.SELL_SHORT else PosSide.LONG
                        eff_max_lev = risk_controller.get_effective_max_leverage() if risk_controller else 5
                        lev = min(plan.suggested_leverage or 3, eff_max_lev)
                        risk_budget = max(5.0, avail_funds * 0.015)
                        sl_px = plan.stop_loss_price or (cur_px * 1.015 if new_pos_side == PosSide.SHORT else cur_px * 0.985)
                        stop_dist = max(abs(cur_px - sl_px), cur_px * 0.008)
                        raw_coins = risk_budget / stop_dist
                        raw_notional = raw_coins * cur_px
                        max_order_cap = min(
                            risk_controller.rules.max_order_usdt if risk_controller.rules else 5000.0,
                            avail_funds * lev * 0.85
                        )
                        final_notional = min(raw_notional, max_order_cap)
                        ct_val, _, _, min_sz = rest_client.get_instrument_spec(sym)
                        min_coin_sz = max(0.0001, ct_val * min_sz)
                        calc_coins = max(min_coin_sz, math.floor((final_notional / cur_px) * 10000) / 10000.0)

                        tp_target = None
                        if hasattr(plan, "take_profit_levels") and plan.take_profit_levels:
                            tp_target = plan.take_profit_levels[0].price
                        elif new_pos_side == PosSide.SHORT:
                            tp_target = round(cur_px - stop_dist * 2.0, 2)
                        else:
                            tp_target = round(cur_px + stop_dist * 2.0, 2)

                        rev_order_req = PlaceOrderRequest(
                            symbol=sym,
                            inst_type=InstType.SWAP,
                            side=new_side,
                            pos_side=new_pos_side,
                            order_type=OrderType.MARKET,
                            size=calc_coins,
                            leverage=lev,
                            tp_trigger_price=tp_target,
                            sl_trigger_price=sl_px,
                        )
                        if is_live:
                            await rest_client.place_order(symbol=sym, side=new_side, pos_side=new_pos_side, order_type=OrderType.MARKET, sz=calc_coins, leverage=lev, tp_trigger_price=tp_target, sl_trigger_price=sl_px)
                        else:
                            paper_engine.place_order(rev_order_req)

                        state.position_scale_records[sym] = 1
                        state.last_scale_timestamps[sym] = now_ms
                        patrol_summaries.append(f"🔄{sym}已反手开{new_pos_side.value}")
                        continue
                    except Exception as rev_err:
                        logger.warning(f"反手开仓执行异常 {sym}: {rev_err}")

                # 1.6 核心情景 C: 同向高置信度信号 -> 智能阶梯加仓 (Pyramiding / Pullback Scale-In)
                # 严格约束：若当前已从最高点回撤超过 0.5%，说明行情见顶滞涨，禁止盲目加仓拉高均价！
                drawdown_from_peak = peak_profit - profit_ratio
                is_same_direction = (is_long and action == SignalAction.BUY_LONG) or ((not is_long) and action == SignalAction.SELL_SHORT)
                if is_same_direction and conf >= state.min_confidence and cur_layers < state.max_scale_layers and plan and drawdown_from_peak < 0.005:
                    last_scale_ts = state.last_scale_timestamps.get(sym, 0)
                    min_interval = getattr(self, "min_scale_interval_ms", getattr(state, "min_scale_interval_ms", 30000))
                    # 防频繁加仓间隔
                    if (now_ms - last_scale_ts) >= min_interval:
                        should_scale = False
                        scale_reason = ""

                        # 判定加仓类型：
                        # (1) 顺势金字塔加仓: 浮盈超 1.0% 且趋势延续
                        if profit_ratio >= 0.01 and state.scale_in_strategy in ("PYRAMIDING", "SMART_HYBRID"):
                            should_scale = True
                            scale_reason = f"顺势金字塔加仓 (浮盈 +{profit_ratio*100:.1f}%)"
                        # (2) 回调均摊加仓: 浮亏在 -0.5% ~ -2.5% (未触及止损) 且遇到支撑
                        elif -0.025 <= profit_ratio <= -0.005 and state.scale_in_strategy in ("PULLBACK_DCA", "SMART_HYBRID"):
                            should_scale = True
                            scale_reason = f"回调支撑均摊加仓 (回撤 {profit_ratio*100:.1f}%)"

                        if should_scale:
                            # 【核心风控：加仓强平线穿透前置预检 (源自 oke_auto_trade 优势融合)】
                            try:
                                from core.dca_liquidation_calculator import DcaLiquidationCalculator
                                cur_pos_liq = pos.liquidation_price or pos.liq_px or 0.0
                                scale_check = DcaLiquidationCalculator.calculate_dca_chain(
                                    direction="long" if is_long else "short",
                                    initial_price=cur_px,
                                    leverage=float(pos.leverage or 3),
                                    initial_margin=max(10.0, float(pos.margin or 50.0)),
                                    dca_margin_base=max(10.0, avail_funds * 0.02),
                                    max_dca_orders=2,
                                    price_deviation_percent=1.2,
                                    extra_margin=0.0
                                )
                                # 检查加仓是否会导致强平击穿
                                if is_long and cur_pos_liq > 0 and cur_px <= cur_pos_liq * 1.01:
                                    logger.warning(f"[AUTOPILOT_SCALE_BLOCKED] 标的 {sym} 当前价 {cur_px} 过度接近强平价 {cur_pos_liq}，阻断加仓")
                                    should_scale = False
                                elif (not is_long) and cur_pos_liq > 0 and cur_px >= cur_pos_liq * 0.99:
                                    logger.warning(f"[AUTOPILOT_SCALE_BLOCKED] 标的 {sym} 当前价 {cur_px} 过度接近强平价 {cur_pos_liq}，阻断加仓")
                                    should_scale = False
                                elif not scale_check.is_safe:
                                    logger.warning(f"[AUTOPILOT_SCALE_BLOCKED] 标的 {sym} 连锁加仓强平预演未通过: {scale_check.violation_reason}")
                                    should_scale = False
                            except Exception as e_dca:
                                logger.debug(f"[AUTOPILOT_DCA_CHECK_ERR] 强平预检计算辅助异常 (放行): {e_dca}")

                        if should_scale:
                            try:
                                eff_max_lev = risk_controller.get_effective_max_leverage() if risk_controller else 5
                                lev = min(plan.suggested_leverage or 3, eff_max_lev)
                                risk_budget = max(5.0, avail_funds * 0.016)
                                sl_px = plan.stop_loss_price or (cur_px * 0.985 if is_long else cur_px * 1.015)
                                stop_dist = max(abs(cur_px - sl_px), cur_px * 0.008)
                                raw_coins = risk_budget / stop_dist
                                raw_notional = raw_coins * cur_px
                                max_order_cap = min(
                                    risk_controller.rules.max_order_usdt if risk_controller.rules else 5000.0,
                                    avail_funds * lev * 0.80
                                )
                                final_notional = min(raw_notional, max_order_cap)
                                ct_val, _, _, min_sz = rest_client.get_instrument_spec(sym)
                                min_coin_sz = max(0.0001, ct_val * min_sz)
                                calc_coins = max(min_coin_sz, math.floor((final_notional / cur_px) * 10000) / 10000.0)

                                new_sl_px = max(entry_px, cur_px * 0.99) if is_long else min(entry_px, cur_px * 1.01)
                                tp_target = plan.take_profit_levels[-1].price if (plan and plan.take_profit_levels) else None
                                if pos.tp_trigger_price and pos.tp_trigger_price > 0:
                                    if is_long:
                                        tp_target = max(tp_target or 0.0, pos.tp_trigger_price)
                                    else:
                                        tp_target = min(tp_target or 99999999.0, pos.tp_trigger_price)

                                scale_ot = OrderType.POST_ONLY if (state.post_only_preferred or (risk_controller.rules and risk_controller.rules.enforce_post_only)) else OrderType.MARKET
                                scale_px = round(cur_px * 0.9995, 2) if is_long else round(cur_px * 1.0005, 2)
                                if scale_ot == OrderType.MARKET:
                                    scale_px = cur_px

                                scale_req = PlaceOrderRequest(
                                    symbol=sym,
                                    inst_type=InstType.SWAP,
                                    side=OrderSide.BUY if is_long else OrderSide.SELL,
                                    pos_side=pos.pos_side,
                                    order_type=scale_ot,
                                    price=scale_px,
                                    size=calc_coins,
                                    leverage=lev,
                                    tp_trigger_price=tp_target,
                                    sl_trigger_price=new_sl_px,
                                    reduce_only=False,
                                )

                                # 构造行情对象用于风控检查
                                if not is_live:
                                    tk_for_check = TickerData(
                                        symbol=sym,
                                        last_price=cur_px,
                                        high_24h=cur_px * 1.05,
                                        low_24h=cur_px * 0.95,
                                        change_24h_pct=1.5,
                                        volume_24h=1000.0,
                                        vol_currency_24h=1000.0 * cur_px,
                                        volume_24h_currency=1000.0 * cur_px,
                                        timestamp=now_ms,
                                    )
                                else:
                                    tk_for_check = cur_tk or TickerData(
                                        symbol=sym,
                                        last_price=cur_px,
                                        high_24h=cur_px * 1.05,
                                        low_24h=cur_px * 0.95,
                                        change_24h_pct=1.5,
                                        volume_24h=1000.0,
                                        vol_currency_24h=1000.0 * cur_px,
                                        volume_24h_currency=1000.0 * cur_px,
                                        timestamp=now_ms,
                                    )

                                # 硬风控检查
                                risk_check = await risk_controller.check_order(scale_req, tk_for_check, positions)
                                if risk_check.passed:
                                    if is_live:
                                        if scale_ot == OrderType.POST_ONLY:
                                            peg_res = await smart_execution_engine.execute_smart_peg_order(
                                                symbol=sym,
                                                side=OrderSide.BUY if is_long else OrderSide.SELL,
                                                pos_side=pos.pos_side,
                                                size=calc_coins,
                                                leverage=lev,
                                                tp_trigger_price=tp_target,
                                                sl_trigger_price=new_sl_px,
                                                max_retries=2,
                                                fallback_to_ioc=True,
                                            )
                                            if not peg_res.get("success"):
                                                logger.warning(f"⚡ [AutoPilot] 加仓 Smart Pegging 未能成交: {peg_res.get('message')}")
                                                patrol_summaries.append(f"{sym}(加仓Peg超时未成)")
                                                continue
                                        else:
                                            await rest_client.place_order(
                                                symbol=sym,
                                                side=OrderSide.BUY if is_long else OrderSide.SELL,
                                                pos_side=pos.pos_side,
                                                order_type=scale_ot,
                                                price=scale_px if scale_ot != OrderType.MARKET else None,
                                                sz=calc_coins,
                                                leverage=lev,
                                                tp_trigger_price=tp_target,
                                                sl_trigger_price=new_sl_px,
                                            )
                                    else:
                                        scale_req.price = scale_px
                                        paper_engine.place_order(scale_req, current_market_price=scale_px if scale_ot == OrderType.POST_ONLY else cur_px)

                                    next_layer = cur_layers + 1
                                    state.position_scale_records[sym] = next_layer
                                    state.last_scale_timestamps[sym] = now_ms
                                    exec_msg = f"⚡ 【自动加仓执行】{sym} 第{next_layer}/{state.max_scale_layers}层: {scale_reason}, 加仓 {calc_coins} 币 @ ${cur_px}"
                                    logger.info(exec_msg)
                                    patrol_summaries.append(f"⚡{sym}第{next_layer}层加仓")
                                    continue
                                else:
                                    logger.debug(f"{sym} 加仓风控拦截: {risk_check.rejected_rule}")
                            except Exception as scale_err:
                                logger.warning(f"加仓执行异常 {sym}: {scale_err}")

            # =========================================================================
            # 阶段 2: 未持仓标的巡检开仓 (New Entry Scan)
            # =========================================================================
            scan_pool = [state.symbol] if state.focus_mode == "SINGLE_SYMBOL" else (state.focus_symbols or state.symbols)
            unheld_targets = [s for s in scan_pool if s not in held_pos_map]

            # 资深投资研究员 (agency-investment-researcher) 资产尽调与体制动态优选排序
            current_regime = getattr(state, "market_regime", "TRENDING_UP")
            try:
                ranked_research = await self.rank_investment_targets_by_research(
                    unheld_targets,
                    market_regime=current_regime
                )
            except Exception as rank_err:
                logger.warning(f"【投资研究员尽调排序】跳过异常: {rank_err}")
                ranked_research = []
            sorted_unheld_targets = [item[0] for item in ranked_research] if ranked_research else unheld_targets
            if ranked_research:
                top_syms = ", ".join([f"{item[0]}({item[1]}分:{item[2]})" for item in ranked_research[:2]])
                logger.debug(f"【投资研究员尽调排序】体制={current_regime}, 优选标的: {top_syms}")

            if len(held_pos_map) < state.max_concurrent_positions:
                for target_symbol in sorted_unheld_targets:
                    if len(held_pos_map) >= state.max_concurrent_positions:
                        break

                    # 平仓后重新入场冷却检查 (默认 90 秒，避免刚止盈就在同一高位假突破追高)
                    last_closed_ts = state.last_closed_timestamps.get(target_symbol, 0)
                    cooldown_ms = (getattr(state, "reentry_cooldown_seconds", 90) or 90) * 1000
                    if (now_ms - last_closed_ts) < cooldown_ms:
                        remain_sec = max(1, int((cooldown_ms - (now_ms - last_closed_ts)) / 1000))
                        patrol_summaries.append(f"{target_symbol}(止盈冷静期{remain_sec}s)")
                        continue

                    # 动态刷新当前最新可用保证金
                    if is_live:
                        try:
                            bal = await rest_client.get_account_balance()
                            avail_funds = bal.available_margin_usdt if bal.available_margin_usdt > 0 else bal.total_equity_usdt
                            cur_eq = bal.total_equity_usdt
                        except Exception:
                            pass
                    else:
                        bal = paper_engine.get_account_balance()
                        avail_funds = bal.available_margin_usdt if bal.available_margin_usdt > 0 else bal.total_equity_usdt
                        cur_eq = bal.total_equity_usdt

                    if avail_funds < 20.0:
                        patrol_summaries.append(f"{target_symbol}(可用保证金不足)")
                        continue

                    # 交易时段检查：美股代币周末休市过滤
                    is_tradfi_equity = any(t in target_symbol.upper() for t in ["NVDA", "TSLA", "AAPL", "MSFT", "AMZN", "SPY", "QQQ"])
                    now_dt = datetime.utcnow()
                    if is_tradfi_equity and now_dt.weekday() in (5, 6):
                        patrol_summaries.append(f"{target_symbol}(美股周末休市)")
                        continue

                    # 提取多周期指标与衍生品情绪
                    try:
                        multi_ind = await get_multi_period_indicators_data(target_symbol)
                        deriv = await get_derivatives_metrics_data(target_symbol)
                        cur_tk = ws_manager.latest_tickers.get(target_symbol)
                        if not cur_tk or (now_ms - getattr(cur_tk, "timestamp", 0)) > 8000:
                            try:
                                fresh_tk = await rest_client.get_ticker(target_symbol)
                                if fresh_tk and fresh_tk.last_price > 0:
                                    cur_tk = fresh_tk
                                    ws_manager.latest_tickers[target_symbol] = fresh_tk
                                    if not is_live:
                                        paper_engine.last_price_map[target_symbol] = fresh_tk.last_price
                            except Exception:
                                pass
                        ref_px = (cur_tk.last_price if (cur_tk and cur_tk.last_price > 0) else 0.0) or paper_engine.last_price_map.get(target_symbol, 100.0)

                        agg_mode = getattr(state, "aggressiveness_mode", "BALANCED").upper()
                        decision = generate_quantitative_rule_decision(
                            symbol=target_symbol,
                            current_price=ref_px,
                            multi_indicators=multi_ind,
                            derivatives=deriv,
                            risk_limits=risk_controller.rules,
                            aggressiveness_mode=agg_mode,
                        )
                    except Exception as eval_err:
                        logger.warning(f"标的 {target_symbol} 决策推导跳过: {eval_err}")
                        patrol_summaries.append(f"{target_symbol}(指标异常跳过)")
                        continue

                    sig = decision.signal
                    plan = decision.trade_plan
                    conf = sig.confidence if sig else 0.0
                    action = sig.action if sig else SignalAction.HOLD_WAIT

                    # 判定是否满足自动开仓门槛
                    if action in (SignalAction.BUY_LONG, SignalAction.SELL_SHORT) and conf >= state.min_confidence and plan:
                        plan_uuid = decision.analysis_id or f"{target_symbol}_{int(now_ms/60000)}"
                        if plan_uuid in state._executed_plan_ids:
                            patrol_summaries.append(f"{target_symbol}(已建仓)")
                            continue

                        # 资金费结算时点防收割保护 (Funding Fee Rollover Guard)
                        if getattr(state, "funding_fee_guard_enabled", True):
                            utc_now = datetime.utcnow()
                            # OKX 资金费每 8 小时结算 (UTC 00:00, 08:00, 16:00)，结算前 15 分钟跳过顺费率开仓
                            if utc_now.hour in (23, 7, 15) and utc_now.minute >= 45:
                                fr = deriv.funding_rate if deriv else 0.0
                                # 年化 > 30% (单期 > 0.00025) 且顺费率方向开仓
                                if (action == SignalAction.BUY_LONG and fr > 0.00025) or (action == SignalAction.SELL_SHORT and fr < -0.00025):
                                    patrol_summaries.append(f"{target_symbol}(资金费结算避让:年化{fr*3*365*100:.1f}%)")
                                    continue

                        # 科学方向与仓位核算
                        is_short = (action == SignalAction.SELL_SHORT)
                        side = OrderSide.SELL if is_short else OrderSide.BUY
                        pos_side = PosSide.SHORT if is_short else PosSide.LONG

                        # 根据激进程度动态决定资金风险比例与杠杆上限
                        agg_mode = getattr(state, "aggressiveness_mode", "BALANCED").upper()
                        if agg_mode == "CONSERVATIVE":
                            risk_pct = 0.005
                            lev_cap = 3
                        elif agg_mode == "AGGRESSIVE":
                            risk_pct = 0.020
                            lev_cap = 8
                        else:  # BALANCED
                            risk_pct = 0.010
                            lev_cap = 5

                        max_sys_lev = risk_controller.get_effective_max_leverage() if risk_controller else 5
                        lev = min(plan.suggested_leverage or 3, lev_cap, max_sys_lev)

                        # 提取 ATR 指标进行波动率校准
                        atr_val = None
                        if multi_ind and hasattr(multi_ind, "indicators") and multi_ind.indicators:
                            ind_1h = multi_ind.indicators.get("1h")
                            ind_15m = multi_ind.indicators.get("15m")
                            if ind_1h and ind_1h.atr_14 > 0:
                                atr_val = ind_1h.atr_14
                            elif ind_15m and ind_15m.atr_14 > 0:
                                atr_val = ind_15m.atr_14

                        # 使用企业级 PositionSizer 计算 ATR 自适应固定风险头寸 (Fixed Fractional Risk)
                        size_result = position_sizer.calculate_adaptive_size(
                            symbol=target_symbol,
                            equity=cur_eq,
                            entry_price=ref_px,
                            is_short=is_short,
                            stop_loss_price=plan.stop_loss_price,
                            atr=atr_val,
                            atr_multiplier=2.0,
                            risk_pct=risk_pct,
                            max_leverage=lev,
                            max_order_usdt=risk_controller.rules.max_order_usdt if risk_controller.rules else 3000.0,
                            available_margin=avail_funds,
                        )

                        final_coins = size_result.calculated_size
                        sl_px = size_result.stop_loss_price
                        stop_dist = size_result.stop_distance

                        tp_target = None
                        if hasattr(plan, "take_profit_levels") and plan.take_profit_levels:
                            tp_target = plan.take_profit_levels[0].price
                        elif hasattr(plan, "take_profit_target_1") and plan.take_profit_target_1:
                            tp_target = plan.take_profit_target_1
                        elif is_short:
                            tp_target = round(ref_px - stop_dist * 2.0, 2)
                        else:
                            tp_target = round(ref_px + stop_dist * 2.0, 2)

                        # 确定限价挂单价格 (Maker POST_ONLY: 确保做多挂在买一档微观下方、做空挂在卖一档微观上方，避免立即吃单)
                        if hasattr(plan, "entry_range") and plan.entry_range and len(plan.entry_range) == 2:
                            raw_entry = (plan.entry_range[0] + plan.entry_range[1]) / 2.0
                            if is_short:
                                limit_entry_px = round(max(raw_entry, ref_px * 1.0003), 2 if ref_px > 10 else 4)
                            else:
                                limit_entry_px = round(min(raw_entry, ref_px * 0.9997), 2 if ref_px > 10 else 4)
                        elif is_short:
                            limit_entry_px = round(ref_px * 1.0005, 2 if ref_px > 10 else 4)
                        else:
                            limit_entry_px = round(ref_px * 0.9995, 2 if ref_px > 10 else 4)

                        # 1. 确定最终发单类型 (确立强制 Post-Only 硬风控最高优先级，严禁破位升级绕过)
                        if risk_controller.rules.enforce_post_only or state.post_only_preferred:
                            target_ot = OrderType.POST_ONLY
                        elif conf >= 0.85:
                            # 仅在硬风控允许且置信度极高时才允许使用 MARKET 追单
                            target_ot = OrderType.MARKET
                        else:
                            target_ot = OrderType.POST_ONLY

                        actual_order_price = limit_entry_px if target_ot != OrderType.MARKET else ref_px

                        # 2. 构造与最终发单 100% 严格一致的 PlaceOrderRequest 送检风控中枢
                        order_req = PlaceOrderRequest(
                            symbol=target_symbol,
                            inst_type=InstType.SWAP,
                            side=side,
                            pos_side=pos_side,
                            order_type=target_ot,
                            price=actual_order_price,
                            size=final_coins,
                            leverage=lev,
                            tp_trigger_price=tp_target,
                            sl_trigger_price=sl_px,
                        )

                        if not is_live:
                            tk_for_new_check = TickerData(
                                symbol=target_symbol,
                                last_price=ref_px,
                                high_24h=ref_px * 1.05,
                                low_24h=ref_px * 0.95,
                                change_24h_pct=1.5,
                                volume_24h=1000.0,
                                vol_currency_24h=1000.0 * ref_px,
                                volume_24h_currency=1000.0 * ref_px,
                                timestamp=now_ms,
                            )
                        else:
                            tk_for_new_check = cur_tk or TickerData(
                                symbol=target_symbol,
                                last_price=ref_px,
                                high_24h=ref_px * 1.05,
                                low_24h=ref_px * 0.95,
                                change_24h_pct=1.5,
                                volume_24h=1000.0,
                                vol_currency_24h=1000.0 * ref_px,
                                volume_24h_currency=1000.0 * ref_px,
                                timestamp=now_ms,
                            )

                        # 3. 严格前置硬风控校验 (包含 Post-Only 强阻断、滑点保护、单笔限额等)
                        risk_check = await risk_controller.check_order(order_req, tk_for_new_check, positions)
                        if not risk_check.passed:
                            patrol_summaries.append(f"{target_symbol}(风控拦截:{risk_check.rejected_rule})")
                            continue

                        # 4. 原样执行下单 (严禁风控后篡改发单参数)
                        try:
                            if is_live:
                                if target_ot == OrderType.POST_ONLY:
                                    peg_res = await smart_execution_engine.execute_smart_peg_order(
                                        symbol=target_symbol,
                                        side=side,
                                        pos_side=pos_side,
                                        size=final_coins,
                                        leverage=lev,
                                        tp_trigger_price=tp_target,
                                        sl_trigger_price=sl_px,
                                        max_retries=2,
                                        fallback_to_ioc=True,
                                    )
                                    if not peg_res.get("success"):
                                        logger.warning(f"🚀 [AutoPilot] 首仓 Smart Pegging 未能成交: {peg_res.get('message')}")
                                        patrol_summaries.append(f"{target_symbol}(首仓Peg超时未成)")
                                        continue
                                else:
                                    await rest_client.place_order(
                                        symbol=target_symbol,
                                        side=side,
                                        pos_side=pos_side,
                                        order_type=target_ot,
                                        price=limit_entry_px if target_ot != OrderType.MARKET else None,
                                        sz=final_coins,
                                        leverage=lev,
                                        tp_trigger_price=tp_target,
                                        sl_trigger_price=sl_px,
                                    )
                            else:
                                paper_engine.place_order(order_req, current_market_price=actual_order_price)

                            state._executed_plan_ids.add(plan_uuid)
                            state.position_scale_records[target_symbol] = 1
                            state.last_scale_timestamps[target_symbol] = now_ms
                            held_pos_map[target_symbol] = None
                            exec_msg = f"🚀 【自动首仓执行 ({target_ot.value})】{target_symbol} {pos_side.value.upper()} {final_coins} 币 @ ~${limit_entry_px if target_ot == OrderType.POST_ONLY else ref_px} (置信度:{conf*100:.0f}%, 止损:${sl_px:.2f})"
                            logger.info(exec_msg)
                            patrol_summaries.append(f"✅{target_symbol}首仓已开")
                        except Exception as exec_err:
                            logger.warning(f"自动开仓下单异常 {target_symbol}: {exec_err}")
                            patrol_summaries.append(f"{target_symbol}(下单失败)")
                    else:
                        patrol_summaries.append(f"{target_symbol}({action.value if hasattr(action, 'value') else action}:{conf*100:.0f}%)")

            state.last_patrol_log = f"巡检完成: {', '.join(patrol_summaries)}" if patrol_summaries else "无巡检事件"

        except Exception as global_patrol_err:
            logger.warning(f"【自动操盘中枢】巡检循环异常: {global_patrol_err}")
            state.last_patrol_log = f"巡检异常: {global_patrol_err}"
        finally:
            self._is_patrolling = False


# 单例全局实例
auto_pilot_manager = AutoPilotManager()
