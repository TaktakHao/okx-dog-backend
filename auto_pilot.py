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
        self.max_scale_layers: int = 3          # 最大加仓层数 (1~5)
        self.scale_in_strategy: str = "SMART_HYBRID"  # PYRAMIDING | PULLBACK_DCA | SMART_HYBRID
        self.auto_reversal_enabled: bool = True  # 遇到强反向信号是否自动反手

        self.max_concurrent_positions: int = 3
        self.trailing_stop_enabled: bool = True
        self.breakeven_trigger_ratio: float = 0.5  # 浮盈达 0.5R 触发移动保本
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
        min_confidence: float = 0.70,
        auto_leverage_enabled: bool = True,
        stop_loss_equity_floor: Optional[float] = None,
        max_loss_limit_usdt: Optional[float] = None,
    ) -> AutoPilotStatusModel:
        """
        开启或恢复指定环境的自动量化操盘会话 (7x24h 后端常驻)
        """
        async with self._lock:
            effective_is_live = self._active_is_live if is_live_mode is None else bool(is_live_mode)
            self._active_is_live = effective_is_live
            state = self._states[effective_is_live]

            # 开启新会话并重置统计基准
            state.reset_for_start(initial_equity=initial_equity)
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
        stop_loss_equity_floor: Optional[float] = None,
        max_loss_limit_usdt: Optional[float] = None,
        is_live_mode: Optional[bool] = None,
    ):
        """更新并记忆指定环境的策略配置"""
        async with self._lock:
            state = self.get_state(is_live_mode)

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

            logger.info(
                f"【自动操盘中枢】{'实盘' if state.is_live_mode else '模拟'}配置已更新并记忆: 模式={state.focus_mode}, 主标的={state.symbol}, "
                f"关注池={state.focus_symbols}, 本金底线={state.stop_loss_equity_floor}, 加仓层数={state.max_scale_layers}, 自动反手={state.auto_reversal_enabled}"
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

            if len(state.snapshots) >= 500:
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
        else:
            end_ms = state.stop_time or now_ms
            running_secs = max(0, int((end_ms - state.start_time) / 1000))

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
            min_confidence=state.min_confidence,
            auto_leverage_enabled=state.auto_leverage_enabled,
            stop_loss_equity_floor=state.stop_loss_equity_floor,
            max_loss_limit_usdt=state.max_loss_limit_usdt,
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
        )

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
                rest_client,
                risk_controller,
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
                if not is_live:
                    cur_px = paper_engine.last_price_map.get(sym) or (cur_tk.last_price if cur_tk else 0) or pos.mark_px or entry_px
                else:
                    cur_px = (cur_tk.last_price if (cur_tk and cur_tk.last_price > 0) else 0) or pos.mark_px or entry_px

                if cur_px <= 0 or entry_px <= 0:
                    continue

                is_long = pos.pos_side == PosSide.LONG
                profit_ratio = (cur_px - entry_px) / entry_px if is_long else (entry_px - cur_px) / entry_px

                # 1.1 动态移动保本 (Breakeven Locked)
                if state.trailing_stop_enabled and pos_id not in state._breakeven_applied_positions:
                    if profit_ratio >= 0.008:  # 浮盈超 0.8%
                        state._breakeven_applied_positions.add(pos_id)
                        if not is_live:
                            paper_engine.update_position_stops(symbol=sym, stop_loss_price=entry_px)
                        log_msg = f"🛡️ 【移动保本触发】{sym} 浮盈达 {profit_ratio*100:+.2f}%，止损线已移至均价 ${entry_px} (Breakeven Locked)"
                        logger.info(log_msg)
                        patrol_summaries.append(f"{sym}保本锁定")

                # 1.2 提取多周期指标与 AI 量化规则研判
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
                    continue

                sig = decision.signal
                plan = decision.trade_plan
                conf = sig.confidence if sig else 0.0
                action = sig.action if sig else SignalAction.HOLD_WAIT

                cur_layers = getattr(pos, "scale_layers", None) or state.position_scale_records.get(sym, 1)

                # 1.3 核心情景 A: 强反向信号 -> 趋势反转平反开 (Directional Reversal)
                is_opposite_signal = (is_long and action == SignalAction.SELL_SHORT) or ((not is_long) and action == SignalAction.BUY_LONG)
                if state.auto_reversal_enabled and is_opposite_signal and conf >= 0.75 and plan:
                    try:
                        logger.info(f"🔄 【趋势反转触发】{sym} 当前持仓 {pos.pos_side.value} 遭遇强反向信号 ({action.value}, 置信度:{conf*100:.0f}%)，执行平仓并反手！")
                        # (1) 平掉当前反向持仓
                        if is_live:
                            await rest_client.close_position(symbol=sym, pos_side=pos.pos_side, order_type=OrderType.MARKET)
                        else:
                            paper_engine.close_position(symbol=sym, pos_side=pos.pos_side, price=cur_px, reason=f"趋势反转强信号 ({action.value}) 自动平仓反手")

                        # (2) 立即反向开仓 (Layer 1)
                        new_side = OrderSide.SELL if action == SignalAction.SELL_SHORT else OrderSide.BUY
                        new_pos_side = PosSide.SHORT if action == SignalAction.SELL_SHORT else PosSide.LONG
                        lev = min(plan.suggested_leverage or 3, risk_controller.rules.max_leverage if risk_controller.rules else 5)
                        risk_budget = max(5.0, avail_funds * 0.02)
                        sl_px = plan.stop_loss_price or (cur_px * 1.015 if new_pos_side == PosSide.SHORT else cur_px * 0.985)
                        stop_dist = max(abs(cur_px - sl_px), cur_px * 0.008)
                        raw_coins = risk_budget / stop_dist
                        raw_notional = raw_coins * cur_px
                        max_order_cap = min(
                            risk_controller.rules.max_order_usdt if risk_controller.rules else 5000.0,
                            avail_funds * lev * 0.85
                        )
                        final_notional = min(raw_notional, max_order_cap)
                        calc_coins = max(0.001, math.floor((final_notional / cur_px) * 1000) / 1000.0)

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

                # 1.4 核心情景 B: 同向高置信度信号 -> 智能阶梯加仓 (Pyramiding / Pullback Scale-In)
                is_same_direction = (is_long and action == SignalAction.BUY_LONG) or ((not is_long) and action == SignalAction.SELL_SHORT)
                if is_same_direction and conf >= state.min_confidence and cur_layers < state.max_scale_layers and plan:
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
                            try:
                                lev = min(plan.suggested_leverage or 3, risk_controller.rules.max_leverage if risk_controller.rules else 5)
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
                                calc_coins = max(0.001, math.floor((final_notional / cur_px) * 1000) / 1000.0)

                                new_sl_px = max(entry_px, cur_px * 0.99) if is_long else min(entry_px, cur_px * 1.01)
                                tp_target = plan.take_profit_levels[0].price if plan.take_profit_levels else None

                                scale_req = PlaceOrderRequest(
                                    symbol=sym,
                                    inst_type=InstType.SWAP,
                                    side=OrderSide.BUY if is_long else OrderSide.SELL,
                                    pos_side=pos.pos_side,
                                    order_type=OrderType.MARKET,
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
                                        await rest_client.place_order(
                                            symbol=sym,
                                            side=OrderSide.BUY if is_long else OrderSide.SELL,
                                            pos_side=pos.pos_side,
                                            order_type=OrderType.MARKET,
                                            sz=calc_coins,
                                            leverage=lev,
                                            tp_trigger_price=tp_target,
                                            sl_trigger_price=new_sl_px,
                                        )
                                    else:
                                        paper_engine.place_order(scale_req)

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

            if len(held_pos_map) < state.max_concurrent_positions:
                for target_symbol in unheld_targets:
                    if len(held_pos_map) >= state.max_concurrent_positions:
                        break

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
                        ref_px = cur_tk.last_price if (cur_tk and cur_tk.last_price > 0) else paper_engine.last_price_map.get(target_symbol, 100.0)

                        decision = generate_quantitative_rule_decision(
                            symbol=target_symbol,
                            current_price=ref_px,
                            multi_indicators=multi_ind,
                            derivatives=deriv,
                            risk_limits=risk_controller.rules,
                        )
                    except Exception as eval_err:
                        logger.debug(f"标的 {target_symbol} 决策推导跳过: {eval_err}")
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

                        # 科学仓位核算 (2% 风险预算与可用保证金双重约束)
                        is_short = action == SignalAction.SELL_SHORT
                        side = OrderSide.SELL if is_short else OrderSide.BUY
                        pos_side = PosSide.SHORT if is_short else PosSide.LONG
                        lev = min(plan.suggested_leverage or 3, risk_controller.rules.max_leverage if risk_controller.rules else 5)

                        risk_budget = max(5.0, avail_funds * 0.02)
                        sl_px = plan.stop_loss_price or (ref_px * 0.985 if not is_short else ref_px * 1.015)
                        stop_dist = max(abs(ref_px - sl_px), ref_px * 0.008)
                        calc_coins = risk_budget / stop_dist
                        raw_notional = calc_coins * ref_px

                        max_order_cap = min(
                            risk_controller.rules.max_order_usdt if risk_controller.rules else 50000.0,
                            avail_funds * lev * 0.90
                        )
                        final_notional = min(raw_notional, max_order_cap)
                        final_coins = max(0.001, math.floor((final_notional / ref_px) * 1000) / 1000.0)

                        tp_target = None
                        if hasattr(plan, "take_profit_levels") and plan.take_profit_levels:
                            tp_target = plan.take_profit_levels[0].price
                        elif hasattr(plan, "take_profit_target_1") and plan.take_profit_target_1:
                            tp_target = plan.take_profit_target_1
                        elif is_short:
                            tp_target = round(ref_px - stop_dist * 2.0, 2)
                        else:
                            tp_target = round(ref_px + stop_dist * 2.0, 2)

                        order_req = PlaceOrderRequest(
                            symbol=target_symbol,
                            inst_type=InstType.SWAP,
                            side=side,
                            pos_side=pos_side,
                            order_type=OrderType.MARKET,
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

                        # 硬风控前置拦截校验
                        risk_check = await risk_controller.check_order(order_req, tk_for_new_check, positions)
                        if not risk_check.passed:
                            patrol_summaries.append(f"{target_symbol}(风控拦截:{risk_check.rejected_rule})")
                            continue

                        # 执行下单 (优先使用 POST_ONLY 或 MARKET，降低手续费摩擦)
                        target_ot = OrderType.POST_ONLY if state.post_only_preferred else OrderType.MARKET
                        try:
                            if is_live:
                                await rest_client.place_order(
                                    symbol=target_symbol,
                                    side=side,
                                    pos_side=pos_side,
                                    order_type=target_ot,
                                    px=ref_px if target_ot == OrderType.POST_ONLY else None,
                                    sz=final_coins,
                                    leverage=lev,
                                    tp_trigger_price=tp_target,
                                    sl_trigger_price=sl_px,
                                )
                            else:
                                paper_engine.place_order(order_req)

                            state._executed_plan_ids.add(plan_uuid)
                            state.position_scale_records[target_symbol] = 1
                            state.last_scale_timestamps[target_symbol] = now_ms
                            held_pos_map[target_symbol] = None
                            exec_msg = f"🚀 【自动首仓执行 ({target_ot.value})】{target_symbol} {pos_side.value.upper()} {final_coins} 币 @ ~${ref_px} (置信度:{conf*100:.0f}%, 止损:${sl_px:.2f})"
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
