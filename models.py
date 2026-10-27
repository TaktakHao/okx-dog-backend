"""
OKX-Dog 个人量化智能交易系统 - 核心数据模型与契约定义
模块: okx-dog-backend/models.py
Python 版本: Python 3.10+
验证框架: Pydantic v2 (BaseModel, Field, ConfigDict, field_validator, field_serializer)

本模块定义了系统所有的领域枚举、数据库映射模型、REST API 请求/响应体、
WebSocket 传输协议包、OKX v5 消息映射以及 AI 研判结构化输入输出契约。
100% 严格对齐 AI_SCHEMA.json, DATABASE_SCHEMA.sql, API_SPEC.md 与 TYPES_CONTRACT.ts。
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union
from pydantic import BaseModel, Field, ConfigDict, field_validator, field_serializer, model_validator


# =============================================================================
# 1. 核心领域枚举 (Domain Enums)
# =============================================================================

class CredentialType(str, Enum):
    """凭证类型枚举"""
    OKX_V5 = "OKX_V5"
    OPENAI = "OPENAI"
    CUSTOM_LLM = "CUSTOM_LLM"


class CopilotMode(str, Enum):
    """人机协同工作模式"""
    MANUAL = "MANUAL"          # 手动模式：AI提供建议，交易员手动确认下单
    SEMI_AUTO = "SEMI_AUTO"    # 半自动模式：系统推送研判提醒，人工二次确认执行
    FULL_AUTO = "FULL_AUTO"    # 全自动模式：高置信度信号在硬风控限额下自动委托


class InstType(str, Enum):
    """OKX 产品类型"""
    SPOT = "SPOT"              # 现货
    SWAP = "SWAP"              # 永续合约
    FUTURES = "FUTURES"        # 交割合约
    MARGIN = "MARGIN"          # 杠杆交易


InstrumentType = InstType  # 前后端别名兼容


class OrderSide(str, Enum):
    """订单买卖方向"""
    BUY = "buy"                # 买入
    SELL = "sell"              # 卖出


class PosSide(str, Enum):
    """持仓方向 (合约)"""
    LONG = "long"              # 多头 (双向持仓)
    SHORT = "short"            # 空头 (双向持仓)
    NET = "net"                # 单向持仓 (现货或单向合约)


class OrderType(str, Enum):
    """订单类型"""
    LIMIT = "limit"                        # 普通限价单
    MARKET = "market"                      # 市价单
    TRIGGER = "trigger"                    # 计划委托单
    POST_ONLY = "post_only"                # 只做 Maker (Post-Only)
    FOK = "fok"                            # 全部成交或立即取消 (Fill-Or-Kill)
    IOC = "ioc"                            # 立即成交并取消剩余 (Immediate-Or-Cancel)
    OPTIMAL_LIMIT_IOC = "optimal_limit_ioc"# 市价委托立即成交并取消 (OKX 特有)


class OrderState(str, Enum):
    """订单生命周期状态 (全链路对齐前端与数据库)"""
    LIVE = "live"                          # 活跃挂单中 (等待成交)
    PARTIALLY_FILLED = "partially_filled"  # 部分成交
    FILLED = "filled"                      # 完全成交
    CANCELED = "canceled"                  # 用户手动撤销
    MMP_CANCELED = "mmp_canceled"          # 做市商保护撤单
    REJECTED = "rejected"                  # 交易所或网关拒绝
    RISK_REJECTED = "risk_rejected"        # 遭后端硬风控中枢拦截拒绝
    EXPIRED = "expired"                    # 委托过期
    FAIL = "fail"                          # OKX 上游报错拒绝


class RiskStatus(str, Enum):
    """系统风控状态"""
    NORMAL = "NORMAL"                      # 正常交易中
    WARNING = "WARNING"                    # 预警状态 (接近风控阈值或强平线)
    CIRCUIT_BROKEN = "CIRCUIT_BROKEN"      # 触发单日亏损熔断锁定
    SAFE_MODE = "SAFE_MODE"                # Panic Button 触发后的安全只读模式


class TimeFrame(str, Enum):
    """行情周期枚举"""
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"


class MarketRegime(str, Enum):
    """宏观盘面结构分类 (严格对齐 AI_SCHEMA.json 与 TYPES_CONTRACT.ts)"""
    TRENDING_UP = "TRENDING_UP"              # 多头趋势
    TRENDING_DOWN = "TRENDING_DOWN"          # 空头趋势
    RANGING = "RANGING"                      # 震荡整理
    VOLATILE_BREAKOUT = "VOLATILE_BREAKOUT"  # 剧烈突破/异动


MarketState = MarketRegime  # 别名兼容


class TimeframeTrend(str, Enum):
    """单一周期趋势研判 (严格对齐 AI_SCHEMA.json definitions.TimeframeDetail)"""
    BULLISH = "BULLISH"                      # 多头排列/强劲上涨
    BEARISH = "BEARISH"                      # 空头排列/破位下跌
    NEUTRAL_CHOPPY = "NEUTRAL_CHOPPY"        # 均线缠绕/宽幅震荡
    OVERBOUGHT = "OVERBOUGHT"                # 超买顶背离
    OVERSOLD = "OVERSOLD"                    # 超卖底背离


class FundingRateBias(str, Enum):
    """资金费率倾向评估 (严格对齐 AI_SCHEMA.json properties.derivatives_sentiment)"""
    EXTREME_POSITIVE = "EXTREME_POSITIVE"    # 极端看多 (费率过热风险)
    MODERATE_POSITIVE = "MODERATE_POSITIVE"  # 健康看多
    NEUTRAL = "NEUTRAL"                      # 中性平衡
    MODERATE_NEGATIVE = "MODERATE_NEGATIVE"  # 健康看空
    EXTREME_NEGATIVE = "EXTREME_NEGATIVE"    # 极端看空 (空头挤压风险)


class SignalAction(str, Enum):
    """AI 核心操作动作 (补全 CLOSE_POSITION，100% 对齐 AI_SCHEMA.json)"""
    BUY_LONG = "BUY_LONG"                    # 做多 / 买入开多
    SELL_SHORT = "SELL_SHORT"                # 做空 / 卖出开空
    CLOSE_POSITION = "CLOSE_POSITION"        # 平仓离场
    HOLD_WAIT = "HOLD_WAIT"                  # 空仓观望 / 维持现有持仓


SignalDirection = SignalAction  # 别名兼容


class SignalUrgency(str, Enum):
    """交易信号执行紧迫度"""
    LOW = "LOW"                              # 挂单等待回调入场
    MEDIUM = "MEDIUM"                        # 常规限价单挂单
    HIGH = "HIGH"                            # 市价即时成交 / 突破紧急入场或紧急止损


class TradePlanOrderType(str, Enum):
    """建议交易计划委托类型"""
    POST_ONLY = "POST_ONLY"                  # 只做 Maker 被动挂单 (无吃单滑点与费率返还)
    LIMIT = "LIMIT"                          # 限价单
    MARKET = "MARKET"                        # 市价单 (兼容历史)
    TRIGGER_LIMIT = "TRIGGER_LIMIT"          # 条件计划限价单


class LLMProvider(str, Enum):
    """支持的大模型服务商"""
    ANTIGRAVITY = "antigravity"
    DEEPSEEK = "deepseek"
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    CUSTOM = "custom"


class RiskEventType(str, Enum):
    """风控审计事件类型 (包含全集枚举与前端映射别名)"""
    MAX_ORDER_EXCEEDED = "MAX_ORDER_EXCEEDED"                            # 单笔金额超限
    DAILY_LOSS_LIMIT = "DAILY_LOSS_LIMIT"                                # 触发单日亏损熔断
    LEVERAGE_EXCEEDED = "LEVERAGE_EXCEEDED"                              # 杠杆倍数超限
    SLIPPAGE_EXCEEDED = "SLIPPAGE_EXCEEDED"                              # 预估滑点超限
    POSITION_LIMIT_EXCEEDED = "POSITION_LIMIT_EXCEEDED"                  # 持仓数量或规模超限
    UNAUTHORIZED_WITHDRAWAL_ATTEMPT = "UNAUTHORIZED_WITHDRAWAL_ATTEMPT"  # 非法提现权限探测
    PANIC_BUTTON_TRIGGERED = "PANIC_BUTTON_TRIGGERED"                    # 紧急一键平仓触发
    CIRCUIT_BREAK_TRIGGERED = "CIRCUIT_BREAK_TRIGGERED"                  # 熔断触发锁定
    CIRCUIT_BREAK_MANUAL_RESET = "CIRCUIT_BREAK_MANUAL_RESET"            # 交易员手动解除熔断
    # 兼容前端命名别名
    ORDER_REJECTED = "ORDER_REJECTED"                                    # 订单被拒
    DAILY_LOSS_TRIGGERED = "DAILY_LOSS_TRIGGERED"                        # 单日亏损触发
    PANIC_TRIGGERED = "PANIC_TRIGGERED"                                  # 紧急熔断触发
    UNAUTHORIZED_WITHDRAWAL_DETECTED = "UNAUTHORIZED_WITHDRAWAL_DETECTED"# 提现权限探测
    SLIPPAGE_PROTECTION = "SLIPPAGE_PROTECTION"                          # 滑点保护触发


class AuditSeverity(str, Enum):
    """审计日志严重程度"""
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    FATAL = "FATAL"


RiskSeverity = AuditSeverity  # 别名兼容


class AttributionStatus(str, Enum):
    """AI 信号归因追踪状态"""
    TRACKING = "TRACKING"          # 追踪中
    COMPLETED = "COMPLETED"        # 已完成评估
    EXPIRED = "EXPIRED"            # 超时未触发入场
    INVALIDATED = "INVALIDATED"    # 达到失效条件提前作废


class AIAttributionOutcome(str, Enum):
    """研判历史归因评估结果"""
    WIN = "WIN"
    LOSS = "LOSS"
    NEUTRAL = "NEUTRAL"
    PENDING = "PENDING"


# =============================================================================
# 2. 通用 API 响应与包装模型 (Generic API Envelopes)
# =============================================================================

T = TypeVar("T")


class ApiResponse(BaseModel, Generic[T]):
    """标准 REST API 响应外壳 (统一毫秒时间戳)"""
    model_config = ConfigDict(populate_by_name=True)

    code: int = Field(0, description="状态码：0 为成功，非 0 为系统业务错误码")
    msg: str = Field("success", description="提示信息或错误描述")
    data: Optional[T] = Field(None, description="业务数据载荷")
    timestamp: int = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), description="服务器响应毫秒时间戳")


class PaginatedData(BaseModel, Generic[T]):
    """分页数据通用包装"""
    model_config = ConfigDict(populate_by_name=True)

    items: List[T] = Field(..., description="当前页数据列表")
    total: int = Field(..., description="总记录条数")
    page: int = Field(..., description="当前页码 (从1开始)")
    page_size: int = Field(..., description="每页条数")
    has_more: bool = Field(False, description="是否存在下一页")


# =============================================================================
# 3. 凭证密盒与安全配置模型 (Vault & Credentials Models)
# =============================================================================

class SaveCredentialsRequest(BaseModel):
    """保存或更新 OKX 凭证请求"""
    model_config = ConfigDict(populate_by_name=True)

    master_password: str = Field(..., min_length=8, description="用于派生 AES 密盒密钥的主密码")
    is_live_mode: bool = Field(False, description="是否为实盘环境 (False: 模拟盘, True: 实盘)")
    api_key: str = Field(..., min_length=10, description="OKX v5 API Key (明文输入，经加密后绝不以明文存储)")
    secret_key: str = Field(..., min_length=10, description="OKX v5 Secret Key (明文输入，经加密存储)")
    passphrase: str = Field(..., min_length=6, description="OKX v5 Passphrase 密码")


class CredentialsStatusResponse(BaseModel):
    """凭证状态查询响应 (完全脱敏)"""
    model_config = ConfigDict(populate_by_name=True)

    has_configured_okx: bool = Field(..., description="是否已配置 OKX 凭证")
    is_live_mode: bool = Field(..., description="当前使用的环境 (True: 实盘, False: 模拟盘)")
    api_key_masked: Optional[str] = Field(None, description="脱敏 API Key (如 6b2a****8e91)")
    permissions: List[str] = Field(default_factory=lambda: ["read", "trade"], description="当前凭证具有的权限列表")
    has_withdraw_permission: bool = Field(False, description="是否包含提现权限 (若为 True 则触发最高安全告警)")
    is_valid: bool = Field(..., description="当前凭证连通性测试是否通过")
    last_tested_at: Optional[Union[int, datetime]] = Field(None, description="上次成功连通测试时间戳 (ms)")

    @field_serializer("last_tested_at")
    def serialize_dt(self, dt: Optional[Union[int, datetime]]) -> Optional[int]:
        if dt is None:
            return None
        if isinstance(dt, datetime):
            return int(dt.timestamp() * 1000)
        return dt


class TestConnectionRequest(BaseModel):
    """测试连通性请求"""
    model_config = ConfigDict(populate_by_name=True)

    master_password: Optional[str] = Field("OKXDog_Master_2026", description="用于解密密盒的主密码")
    is_live_mode: Optional[bool] = Field(None, description="测试的目标环境，为空则测试当前配置环境")
    api_key: Optional[str] = Field(None, description="即时测试的 API Key (若传入则优先使用传入凭证进行连通性测试)")
    secret_key: Optional[str] = Field(None, description="即时测试的 Secret Key")
    passphrase: Optional[str] = Field(None, description="即时测试的 Passphrase")



class TestConnectionResponse(BaseModel):
    """测试连通性结果响应"""
    model_config = ConfigDict(populate_by_name=True)

    connected: bool = Field(..., description="网络连通与签名认证是否成功")
    latency_ms: int = Field(..., description="OKX API 端到端通信延迟 (毫秒)")
    permissions: List[str] = Field(..., description="查询到的账户权限列表")
    security_warning: Optional[str] = Field(None, description="安全预警信息 (例如包含提现权限时给出强阻断告警)")


class LLMConfigRequest(BaseModel):
    """大模型配置更新请求"""
    model_config = ConfigDict(populate_by_name=True)

    master_password: str = Field(..., description="主密码 (用于加密保存 LLM API Key)")
    llm_provider: LLMProvider = Field(LLMProvider.ANTIGRAVITY, description="大模型服务提供商")
    llm_base_url: str = Field("http://127.0.0.1:8001", description="OpenAI 兼容的基础 API URL 或本地服务地址")
    llm_api_key: Optional[str] = Field(None, description="大模型 API Key (若使用 Antigravity 本地免 Key 驱动可传空)")
    llm_model: str = Field("gemini-3.7-flash", description="模型名称 (如 gemini-3.7-flash, deepseek-reasoner, gpt-4o)")
    llm_temperature: float = Field(0.2, ge=0.0, le=2.0, description="采样温度")
    llm_max_tokens: int = Field(4096, ge=512, le=16384, description="最大生成 Token 数量")


class LLMConfigResponse(BaseModel):
    """大模型配置信息响应 (脱敏)"""
    model_config = ConfigDict(populate_by_name=True)

    llm_provider: LLMProvider = Field(..., description="服务提供商")
    llm_base_url: str = Field(..., description="Base URL")
    llm_model: str = Field(..., description="当前模型名称")
    llm_api_key_masked: Optional[str] = Field(None, description="脱敏 API Key")
    llm_temperature: float = Field(..., description="采样温度")
    llm_max_tokens: int = Field(..., description="最大 Token 数")


class TestLLMRequest(BaseModel):
    """测试大模型连通性请求"""
    model_config = ConfigDict(populate_by_name=True)

    master_password: Optional[str] = Field(None, description="主密码 (若使用已保存的加密 Key)")
    llm_provider: Optional[LLMProvider] = Field(None, description="大模型服务商")
    llm_base_url: Optional[str] = Field(None, description="大模型 Base URL")
    llm_api_key: Optional[str] = Field(None, description="大模型 API Key (若传则使用，不传则使用密盒中解密的 Key)")
    llm_model: Optional[str] = Field(None, description="模型名称")


class TestLLMResponse(BaseModel):
    """测试大模型连通性响应"""
    model_config = ConfigDict(populate_by_name=True)

    connected: bool = Field(..., description="是否连通成功")
    latency_ms: int = Field(0, description="响应耗时 (毫秒)")
    model: str = Field(..., description="响应模型名称")
    message: Optional[str] = Field(None, description="测试响应内容或错误说明")


# =============================================================================
# 4. 系统运行与人机协同配置模型 (System Config Models)
# =============================================================================

class SystemConfigModel(BaseModel):
    """系统运行完整配置视图"""
    model_config = ConfigDict(populate_by_name=True)

    copilot_mode: CopilotMode = Field(CopilotMode.MANUAL, description="人机协同工作模式")
    active_symbol: str = Field("BTC-USDT-SWAP", description="当前主监控交易对")
    supported_timeframes: List[str] = Field(default_factory=lambda: ["15m", "1h", "4h", "1d"], description="支持的计算周期")
    is_trading_enabled: bool = Field(True, description="全局交易主开关")
    is_ai_auto_trigger: bool = Field(False, description="是否启用 AI 定时自动研判")
    auto_trigger_interval_mins: int = Field(60, ge=5, le=1440, description="定时研判间隔 (分钟)")


class UpdateSystemConfigRequest(BaseModel):
    """修改系统配置请求"""
    model_config = ConfigDict(populate_by_name=True)

    copilot_mode: Optional[CopilotMode] = None
    active_symbol: Optional[str] = None
    supported_timeframes: Optional[List[str]] = None
    is_trading_enabled: Optional[bool] = None
    is_ai_auto_trigger: Optional[bool] = None
    auto_trigger_interval_mins: Optional[int] = None


# =============================================================================
# 5. 多重硬风控拦截中枢模型 (Hard Risk Controller Models)
# =============================================================================

class RiskRulesConfig(BaseModel):
    """硬风控规则配置 (严格对齐前端 HardRiskRules)"""
    model_config = ConfigDict(populate_by_name=True)

    max_order_usdt: float = Field(50000.0, gt=0, description="单笔最大委托名义价值 (USDT)")
    max_daily_loss_usdt: float = Field(10000.0, gt=0, description="单日累计最大亏损熔断阈值 (USDT)")
    max_leverage: int = Field(20, ge=1, le=100, description="最高允许杠杆倍数")
    max_slippage_pct: float = Field(2.0, gt=0, le=10.0, description="市价单允许最大滑点百分比 (2.0%)")
    max_open_positions: int = Field(10, ge=1, le=50, description="最大同时持仓标的数")
    max_position_usdt: float = Field(100000.0, gt=0, description="单标的最大持仓名义价值 (USDT)")
    default_stop_loss_pct: Optional[float] = Field(None, description="默认附带止损百分比")
    default_take_profit_pct: Optional[float] = Field(None, description="默认附带止盈百分比")
    fixed_risk_per_trade_percent: float = Field(0.01, gt=0.0, le=0.1, description="单笔最大风险暴露比例 (默认 1% 净值，7000 USDT 本金对应 70 USDT)")
    enforce_post_only: bool = Field(True, description="开仓委托是否强制 Post-Only 只做 Maker 消除 Taker 滑点与费率磨损")



HardRiskRules = RiskRulesConfig  # 别名兼容


class RiskDailyStats(BaseModel):
    """单日亏损统计与熔断状态"""
    model_config = ConfigDict(populate_by_name=True)

    date: str = Field(..., description="统计日期 (YYYY-MM-DD)")
    today_realized_pnl: float = Field(..., description="当日已实现亏损 (USDT)")
    today_unrealized_pnl: float = Field(..., description="当日当前未实现浮动亏损 (USDT)")
    today_total_loss: float = Field(..., description="当日总亏损金额 (已实现 + 未实现)")
    max_daily_loss_limit: float = Field(..., description="最大允许亏损额度 (USDT)")
    loss_usage_ratio: float = Field(..., description="亏损额度使用百分比 (0.0 ~ 1.0)")
    is_circuit_broken: bool = Field(..., description="系统是否已被硬风控熔断锁死")
    circuit_break_reason: Optional[str] = Field(None, description="熔断原因说明")
    circuit_break_at: Optional[int] = Field(None, description="熔断触发时间戳 (ms)")


class RiskStatusResponse(BaseModel):
    """风控中枢实时状态响应"""
    model_config = ConfigDict(populate_by_name=True)

    risk_status: RiskStatus = Field(..., description="当前风控全局状态")
    is_circuit_broken: bool = Field(..., description="是否处于熔断锁定中")
    circuit_break_reason: Optional[str] = Field(None, description="熔断触发原因")
    circuit_break_at: Optional[Union[int, datetime]] = Field(None, description="熔断触发时间戳")
    daily_loss_current: float = Field(..., description="当日已发生亏损 (USDT)")
    max_daily_loss_usdt: float = Field(..., description="单日亏损熔断阈值 (USDT)")
    daily_loss_ratio: float = Field(..., description="当日亏损占熔断阈值比例 (0.0~1.0+)")
    is_safe_mode: bool = Field(..., description="是否处于安全只读模式")
    rules: RiskRulesConfig = Field(..., description="当前生效的硬风控规则参数")

    @field_serializer("circuit_break_at")
    def serialize_cb_at(self, dt: Optional[Union[int, datetime]]) -> Optional[int]:
        if dt is None:
            return None
        if isinstance(dt, datetime):
            return int(dt.timestamp() * 1000)
        return dt


class RiskCheckResult(BaseModel):
    """硬风控拦截单次评估结果"""
    model_config = ConfigDict(populate_by_name=True)

    passed: bool = Field(..., description="是否通过全部硬风控校验")
    risk_status: Optional[RiskStatus] = Field(RiskStatus.NORMAL, description="当前风控状态")
    rejected_rule: Optional[str] = Field(None, alias="violated_rule", description="被触发并阻断的风控规则名称")
    violated_rule: Optional[str] = Field(None, description="别名兼容")
    message: str = Field("通过风控检查", description="审核通过确认或拦截拒绝说明")
    threshold: Optional[float] = Field(None, description="风控阈值")
    actual_value: Optional[float] = Field(None, description="尝试提交的实际数值")
    evaluated_metrics: Dict[str, Any] = Field(default_factory=dict, description="评估时的上下文指标参数")


class ResetCircuitBreakerRequest(BaseModel):
    """手动解除熔断锁定请求"""
    model_config = ConfigDict(populate_by_name=True)

    master_password: str = Field(..., description="主密码 (防止误触，需密码授权)")
    reason: str = Field(..., min_length=4, description="解除熔断的原因与交易员备注")


class PanicButtonRequest(BaseModel):
    """Panic Button 紧急一键平仓请求 (统一前后端参数)"""
    model_config = ConfigDict(populate_by_name=True)

    master_password: str = Field(..., description="用于解密与安全二次确认的主密码")
    confirm_phrase: str = Field("PANIC_CLOSE_ALL", description="确认文本，必须严格等于 'PANIC_CLOSE_ALL'")
    target: str = Field("ALL", description="熔断目标范围: 'ALL', 'SWAP_ONLY', 'SPOT_ONLY'")
    cancel_all_orders: bool = Field(True, description="是否撤销所有挂单")
    close_all_positions: bool = Field(True, description="是否市价全平所有持仓")
    reason: Optional[str] = Field("PANIC_BUTTON_TRIGGERED", description="触发原因说明")
    confirm_token: Optional[str] = Field(None, description="安全校验 Token (前端兼容)")

    @field_validator("confirm_phrase")
    @classmethod
    def validate_phrase(cls, v: str) -> str:
        if v != "PANIC_CLOSE_ALL":
            raise ValueError("确认文本错误，必须输入 'PANIC_CLOSE_ALL'")
        return v


class PanicExecutionDetail(BaseModel):
    """Panic 执行明细项"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="交易标的代码")
    action: str = Field(..., description="执行动作 (CANCEL_ORDER / CLOSE_POSITION)")
    target_id: str = Field(..., description="订单 ID 或 持仓 ID")
    status: str = Field(..., description="执行状态 (SUCCESS / FAILED)")
    message: Optional[str] = Field(None, description="状态描述")


class PanicButtonResponse(BaseModel):
    """Panic Button 触发处置结果 (统一前后端响应)"""
    model_config = ConfigDict(populate_by_name=True)

    success: bool = Field(..., description="紧急处置动作是否成功启动")
    canceled_orders_count: int = Field(..., description="成功批量撤销的挂单数量")
    closed_positions_count: int = Field(..., description="成功发起市价全平的持仓数量")
    failed_operations: List[str] = Field(default_factory=list, description="失败的操作细节 (若有)")
    execution_details: List[PanicExecutionDetail] = Field(default_factory=list, description="各标的执行明细列表")
    system_state: str = Field("LOCKED_SAFE_MODE", description="系统后续保护状态")
    circuit_broken_at: int = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), description="熔断触发时间戳 (ms)")
    message: str = Field(..., description="处置完成摘要")


# =============================================================================
# 6. 行情与多周期技术指标模型 (Market Data & Indicator Models)
# =============================================================================

class TickerData(BaseModel):
    """单个标的实时 Ticker 数据 (严格对齐前端 Ticker)"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="标的代码 (如 BTC-USDT-SWAP)")
    last_price: float = Field(..., description="最新成交价")
    last_sz: float = Field(0.0, description="最新成交数量")
    bid_price: Optional[float] = Field(None, description="最优买一价 (Best Bid)")
    ask_price: Optional[float] = Field(None, description="最优卖一价 (Best Ask)")
    high_24h: float = Field(..., description="24小时最高价")
    low_24h: float = Field(..., description="24小时最低价")
    open_24h: float = Field(0.0, description="24小时开盘价")
    change_24h: Optional[float] = Field(None, description="24小时价格变动绝对值")
    change_24h_pct: float = Field(..., description="24小时涨跌幅 (例如 2.35 表示 +2.35%)")
    volume_24h: float = Field(..., description="24小时成交量 (张数/币数)")
    vol_currency_24h: float = Field(..., alias="volume_24h_currency", description="24小时成交额 (USDT)")
    volume_24h_currency: Optional[float] = Field(None, description="别名兼容")
    mark_price: Optional[float] = Field(None, description="最新标记价格")
    index_price: Optional[float] = Field(None, description="指数价格")
    funding_rate: Optional[float] = Field(None, description="当前资金费率")
    next_funding_time: Optional[int] = Field(None, description="下次结算资金费率时间戳 (ms)")
    timestamp: int = Field(..., description="行情毫秒时间戳")


Ticker = TickerData  # 别名兼容


class KlineCandle(BaseModel):
    """单个 K 线 Bar 数据 (对齐 Lightweight Charts 与前端 CandleStick)"""
    model_config = ConfigDict(populate_by_name=True)

    timestamp: int = Field(..., description="Bar 开盘时间戳 (ms)")
    open: float = Field(..., description="开盘价")
    high: float = Field(..., description="最高价")
    low: float = Field(..., description="最低价")
    close: float = Field(..., description="收盘价")
    volume: float = Field(..., description="成交量")
    vol_currency: Optional[float] = Field(0.0, description="成交额 (USDT)")
    is_closed: bool = Field(True, description="该 Bar 是否已收线确认")

    @model_validator(mode="before")
    @classmethod
    def _normalize_candle_inputs(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "timestamp" not in data and "time" in data and data["time"] is not None:
                data["timestamp"] = data["time"]
            if "is_closed" not in data and "confirmed" in data and data["confirmed"] is not None:
                data["is_closed"] = data["confirmed"]
        return data


CandleStick = KlineCandle  # 别名兼容


class OrderBookLevel(BaseModel):
    """订单簿档位"""
    model_config = ConfigDict(populate_by_name=True)

    price: float = Field(..., description="价格")
    size: float = Field(..., description="委托数量")
    order_count: Optional[int] = Field(1, description="订单数")
    cumulative_size: Optional[float] = Field(None, description="累计深度")
    depth_ratio: Optional[float] = Field(None, description="深度占比")


class OrderBookDepth(BaseModel):
    """深度盘口数据 (Level-2)"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="标的代码")
    timestamp: int = Field(..., description="盘口毫秒时间戳")
    bids: List[OrderBookLevel] = Field(..., description="买盘档位 (价格从高到低)")
    asks: List[OrderBookLevel] = Field(..., description="卖盘档位 (价格从低到高)")
    checksum: Optional[int] = Field(None, description="序列校验和")


OrderBook = OrderBookDepth  # 别名兼容


class MarketTrade(BaseModel):
    """逐笔实时成交记录"""
    model_config = ConfigDict(populate_by_name=True)

    trade_id: str = Field(..., description="成交唯一 ID")
    symbol: str = Field(..., description="标的代码")
    side: OrderSide = Field(..., description="成交方向 (buy / sell)")
    price: float = Field(..., description="成交价格")
    size: float = Field(..., description="成交数量")
    timestamp: int = Field(..., description="成交时间戳 (ms)")
    is_whale_trade: Optional[bool] = Field(False, description="是否为大单异动")


class DerivativesMetrics(BaseModel):
    """衍生品专项监控指标"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="标的代码")
    funding_rate: float = Field(..., description="当前资金费率 (例如 0.0001)")
    predicted_funding_rate: Optional[float] = Field(None, description="预测下期资金费率")
    funding_rate_annualized_pct: Optional[float] = Field(None, description="年化资金费率百分比")
    next_funding_time: Optional[int] = Field(None, description="下次结算时间戳 (ms)")
    funding_countdown_ms: Optional[int] = Field(None, description="距离下次结算倒计时毫秒")
    open_interest: float = Field(..., alias="open_interest_size", description="全网未平仓合约张数 (OI)")
    open_interest_size: Optional[float] = Field(None, description="别名兼容")
    open_interest_usdt: float = Field(..., alias="open_interest_usd", description="全网未平仓合约估值 (USDT)")
    open_interest_usd: Optional[float] = Field(None, description="别名兼容")
    oi_change_24h_pct: float = Field(..., alias="open_interest_change_24h_pct", description="24小时 OI 变动百分比")
    open_interest_change_24h_pct: Optional[float] = Field(None, description="别名兼容")
    long_short_ratio: Optional[float] = Field(None, description="多空持仓人数比")
    top_trader_long_short_ratio: Optional[float] = Field(None, description="大户多空持仓比")
    sentiment_score: Optional[float] = Field(0.0, description="情绪量化得分 (-1.0 ~ +1.0)")
    timestamp: Optional[int] = Field(None, description="指标生成时间戳 (ms)")


class MACDIndicator(BaseModel):
    """MACD 详细指标"""
    model_config = ConfigDict(populate_by_name=True)

    dif: float = Field(..., description="MACD DIF (快线)")
    dea: float = Field(..., description="MACD DEA (慢线)")
    macd_bar: float = Field(..., description="MACD 柱状图")
    is_golden_cross: bool = Field(False, description="是否金叉")
    is_death_cross: bool = Field(False, description="是否死叉")


class BollingerIndicator(BaseModel):
    """布林带详细指标"""
    model_config = ConfigDict(populate_by_name=True)

    upper: float = Field(..., description="上轨")
    mid: float = Field(..., description="中轨")
    lower: float = Field(..., description="下轨")
    bandwidth: float = Field(..., description="带宽")
    percent_b: float = Field(..., description="位置 %b")


class SinglePeriodIndicators(BaseModel):
    """单个时间周期的量化技术指标集合"""
    model_config = ConfigDict(populate_by_name=True)

    timeframe: str = Field(..., description="周期 (15m, 1h, 4h, 1d)")
    ema_20: float = Field(..., description="EMA 20")
    ema_50: float = Field(..., description="EMA 50")
    ema_200: Optional[float] = Field(None, description="EMA 200")
    macd_dif: float = Field(..., description="MACD DIF")
    macd_dea: float = Field(..., description="MACD DEA")
    macd_hist: float = Field(..., description="MACD 柱状图")
    rsi_14: float = Field(..., description="RSI 14")
    bb_upper: float = Field(..., description="布林带上轨")
    bb_middle: float = Field(..., description="布林带中轨")
    bb_lower: float = Field(..., description="布林带下轨")
    atr_14: float = Field(..., description="ATR 14 真实波动区间")


TechnicalIndicatorSet = SinglePeriodIndicators  # 别名兼容


class MultiPeriodIndicators(BaseModel):
    """多周期指标汇总聚合包"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="标的代码")
    timestamp: int = Field(..., description="计算时间戳")
    indicators: Dict[str, SinglePeriodIndicators] = Field(..., description="各周期指标映射，例如 {'15m': ..., '1h': ...}")


MultiTimeframeIndicators = MultiPeriodIndicators  # 别名兼容


class MarketContextSnapshot(BaseModel):
    """送入 AI 决策中枢的结构化行情完整快照"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="标的代码")
    timestamp: int = Field(..., description="毫秒时间戳")
    ticker: TickerData = Field(..., description="实时行情 Ticker")
    derivatives: DerivativesMetrics = Field(..., description="衍生品指标")
    multi_indicators: MultiPeriodIndicators = Field(..., description="多周期技术指标")
    current_position: Optional[PositionModel] = Field(None, description="交易员当前持仓状态 (若无持仓为 null)")
    account_available_margin_usdt: float = Field(..., description="当前可用保证金 (USDT)")


# =============================================================================
# 7. AI 智能研判与决策契约模型 (LLM Decision Models)
# 100% 严格对齐 okx-dog-ai/AI_SCHEMA.json 与 TYPES_CONTRACT.ts
# =============================================================================

class TimeframeDetail(BaseModel):
    """单一周期技术指标与形态研判细节 (AI_SCHEMA definitions.TimeframeDetail)"""
    model_config = ConfigDict(populate_by_name=True)

    trend: TimeframeTrend = Field(..., description="周期趋势：BULLISH, BEARISH, NEUTRAL_CHOPPY, OVERBOUGHT, OVERSOLD")
    key_indicators_summary: str = Field(..., description="该周期关键指标表现描述")
    support_level: float = Field(..., ge=0.0, description="识别出的第一关键支撑价位")
    resistance_level: float = Field(..., ge=0.0, description="识别出的第一关键阻力价位")


class TimeframeAnalysis(BaseModel):
    """多周期技术面共振分析字典 (AI_SCHEMA properties.timeframe_analysis)"""
    model_config = ConfigDict(populate_by_name=True)

    tf_15m: TimeframeDetail = Field(..., description="15分钟周期研判细节")
    tf_1h: TimeframeDetail = Field(..., description="1小时周期研判细节")
    tf_4h: TimeframeDetail = Field(..., description="4小时周期研判细节")
    tf_1d: TimeframeDetail = Field(..., description="日线周期研判细节")


class DerivativesSentiment(BaseModel):
    """衍生品与市场情绪专项研判 (AI_SCHEMA properties.derivatives_sentiment)"""
    model_config = ConfigDict(populate_by_name=True)

    funding_rate_bias: FundingRateBias = Field(..., description="资金费率倾向")
    open_interest_interpretation: str = Field(..., description="全网未平仓合约量 (OI) 异动解读")
    long_short_ratio_state: str = Field(..., description="多空持仓比与大户状态分析")
    sentiment_score: float = Field(..., ge=-1.0, le=1.0, description="综合情绪量化得分 (-1.0 ~ +1.0)")


AIDerivativesSentiment = DerivativesSentiment  # 别名兼容


class AISignal(BaseModel):
    """最终交易决策信号与置信度 (AI_SCHEMA properties.signal)"""
    model_config = ConfigDict(populate_by_name=True)

    action: SignalAction = Field(..., description="核心操作动作：BUY_LONG, SELL_SHORT, CLOSE_POSITION, HOLD_WAIT")
    confidence: float = Field(..., ge=0.0, le=1.0, description="决策置信度打分 [0.0, 1.0]")
    urgency: SignalUrgency = Field(..., description="执行紧迫度：LOW, MEDIUM, HIGH")


class TakeProfitLevel(BaseModel):
    """分阶段止盈点位与平仓比例 (AI_SCHEMA definitions.TakeProfitLevel)"""
    model_config = ConfigDict(populate_by_name=True)

    price: float = Field(..., ge=0.0, description="目标止盈价格")
    percentage: float = Field(..., ge=0.01, le=1.0, description="该点位建议平仓比例 (0.01 ~ 1.0)")
    description: str = Field(..., description="止盈依据说明")


AITakeProfitLevel = TakeProfitLevel  # 别名兼容


class TradePlan(BaseModel):
    """建议交易计划与量化点位参数 (AI_SCHEMA properties.trade_plan)"""
    model_config = ConfigDict(populate_by_name=True)

    intent: str = Field("WAIT_OBSERVE", description="战术交易逻辑分类：SHORT_SQUEEZE, LIQUIDITY_SWEEP, TREND_CONTINUATION, MEAN_REVERSION, BREAKOUT_FOLLOW, WAIT_OBSERVE")
    entry_range: List[float] = Field(..., min_length=2, max_length=2, description="建议入场价格区间 [最低入场价, 最高入场价]")
    take_profit_levels: List[TakeProfitLevel] = Field(..., min_length=1, max_length=4, description="分批止盈目标列表")
    stop_loss_price: float = Field(..., ge=0.0, description="硬止损价格")
    risk_reward_ratio: float = Field(..., ge=0.0, description="理论盈亏比 (R:R Ratio)")
    suggested_leverage: int = Field(..., ge=1, le=20, description="建议杠杆倍数 (1 ~ 20)")
    order_type: TradePlanOrderType = Field(TradePlanOrderType.POST_ONLY, description="建议委托类型：POST_ONLY, LIMIT, TRIGGER_LIMIT")


AITradePlan = TradePlan  # 别名兼容


class MarketAnomalyTriggerType(str, Enum):
    """L1 毫秒级微观异动触发类型"""
    OI_SURGE_BREAKOUT = "OI_SURGE_BREAKOUT"                    # 15m 持仓量 (OI) 暴增突破
    EXTREME_FUNDING_DIVERGENCE = "EXTREME_FUNDING_DIVERGENCE"  # 资金费率极端倒挂与情绪偏离
    LIQUIDITY_SWEEP_REVERSAL = "LIQUIDITY_SWEEP_REVERSAL"      # 流动性假突破与均值回归
    MASSIVE_LIQUIDATION_CASCADE = "MASSIVE_LIQUIDATION_CASCADE"# 密集清算爆仓踩踏


class MicrostructureMetrics(BaseModel):
    """L1 毫秒级微观结构指标"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="标的代码")
    timestamp: int = Field(..., description="毫秒时间戳")
    order_book_imbalance: float = Field(..., description="前10档买卖盘挂单失衡率 OBI [-1.0, 1.0]")
    cumulative_volume_delta: float = Field(..., description="15m 累积主动买卖量差 CVD")
    oi_surge_pct: float = Field(..., description="15m 未平仓合约量变化率 (%)")
    funding_rate_annualized: float = Field(..., description="当前当期资金费率折合年化率 (%)")
    recent_liquidations_usdt: float = Field(0.0, description="近5分钟清算爆仓名义金额")


class MarketAnomalyEvent(BaseModel):
    """L1 触发 L2 条件唤醒的异动事件数据包"""
    model_config = ConfigDict(populate_by_name=True)

    event_id: str = Field(..., description="事件唯一 UUID")
    symbol: str = Field(..., description="异动标的代码")
    timestamp: int = Field(..., description="毫秒时间戳")
    trigger_type: MarketAnomalyTriggerType = Field(..., description="触发异动类型")
    severity: str = Field("WARNING", description="严重程度: INFO, WARNING, CRITICAL")
    metrics_snapshot: MicrostructureMetrics = Field(..., description="异动发生时的微观指标快照")
    reason: str = Field(..., description="异动触发具体依据与参数")


class RiskAssessment(BaseModel):
    """风险评估与逻辑失效边界 (AI_SCHEMA properties.risk_assessment)"""
    model_config = ConfigDict(populate_by_name=True)

    key_risks: List[str] = Field(..., min_length=1, max_length=5, description="面临的核心风险因素列表")
    invalidation_condition: str = Field(..., description="交易逻辑彻底失效的技术形态或价格条件")
    max_holding_time_hours: float = Field(..., ge=0.5, le=720.0, description="建议最长持仓时间 (小时)")


AIRiskAssessment = RiskAssessment  # 别名兼容


class AIAnalysisResponse(BaseModel):
    """AI 大模型完整结构化研判响应 (100% 对齐 AI_SCHEMA.json 与前端契约)"""
    model_config = ConfigDict(populate_by_name=True)

    analysis_id: str = Field(..., description="研判唯一标识符 (UUID v4)")
    symbol: str = Field(..., description="交易标的代码，例如 'BTC-USDT-SWAP'")
    timestamp: int = Field(..., description="研判生成时的 Unix 毫秒时间戳 (ms)")
    market_regime: MarketRegime = Field(..., description="宏观盘面结构分类")
    timeframe_analysis: TimeframeAnalysis = Field(..., description="多周期（15m, 1h, 4h, 1d）共振分析")
    derivatives_sentiment: DerivativesSentiment = Field(..., description="衍生品与市场情绪专项研判")
    signal: AISignal = Field(..., description="最终交易决策信号与置信度")
    trade_plan: TradePlan = Field(..., description="建议交易计划与量化点位参数")
    risk_assessment: RiskAssessment = Field(..., description="风险评估与逻辑失效边界")
    reasoning_summary: str = Field(..., description="精炼中文研判结论与核心逻辑摘要 (150字以内)")
    reasoning_details: str = Field(..., description="详尽的量化逻辑推导过程")
    
    # 网关与运行时扩展字段
    model_used: Optional[str] = Field(None, description="实际使用的模型名称 (如 deepseek-reasoner)")
    latency_ms: Optional[int] = Field(0, description="模型推理耗时 (毫秒)")
    thinking_process: Optional[str] = Field(None, description="推理思维链完整内容 (<think> 标签内内容)")



class TriggerAIAnalysisRequest(BaseModel):
    """手动触发 AI 研判请求"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: Optional[str] = Field("BTC-USDT-SWAP", description="研判标的")
    timeframe: Optional[str] = Field("1h", description="主研判周期")
    include_thinking: bool = Field(True, description="是否返回思维链内容")
    mode: Optional[str] = Field("agent", description="研判模式: 'agent' (LangGraph 资深量化交易员多阶段反思) 或 'fast' (单步快速生成)")



class SSEStreamChunk(BaseModel):
    """SSE 流式传输数据块 (AICoTStreamEvent)"""
    model_config = ConfigDict(populate_by_name=True)

    event: str = Field("chunk", description="事件类型: start, thinking, reasoning_chunk, schema_chunk, complete, done, error")
    data: Union[str, AIAnalysisResponse, Dict[str, Any]] = Field(..., description="数据载荷")
    reasoning_content: Optional[str] = Field(None, description="思维链增量文本")
    structured_output: Optional[AIAnalysisResponse] = Field(None, description="解析出的完整研判对象")
    error_message: Optional[str] = Field(None, description="错误信息")


AICoTStreamEvent = SSEStreamChunk  # 别名兼容


# =============================================================================
# 8. 交易执行与订单持仓模型 (Trade, Order & Position Models)
# =============================================================================

class PlaceOrderRequest(BaseModel):
    """统一委托下单请求 (对齐前端 OrderCreateRequest)"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="标的代码 (如 BTC-USDT-SWAP)")
    inst_type: InstType = Field(InstType.SWAP, description="产品类型")
    side: OrderSide = Field(..., description="买卖方向 (buy / sell)")
    pos_side: PosSide = Field(PosSide.NET, description="持仓方向 (long / short / net)")
    order_type: OrderType = Field(OrderType.LIMIT, description="订单类型")
    price: Optional[float] = Field(None, description="委托价格 (市价单传 null)")
    sz: float = Field(..., gt=0, alias="size", description="委托数量 (张数或币数)")
    size: Optional[float] = Field(None, description="数量别名")
    leverage: int = Field(1, ge=1, le=20, description="杠杆倍数")
    # 附带止盈止损 (OCO)
    tp_trigger_price: Optional[float] = Field(None, description="止盈触发价")
    tp_ord_price: Optional[float] = Field(None, alias="tp_order_price", description="止盈委托价 (-1 或 null 为市价)")
    tp_order_price: Optional[float] = Field(None, description="别名兼容")
    sl_trigger_price: Optional[float] = Field(None, description="止损触发价")
    sl_ord_price: Optional[float] = Field(None, alias="sl_order_price", description="止损委托价 (-1 或 null 为市价)")
    sl_order_price: Optional[float] = Field(None, description="别名兼容")
    reduce_only: Optional[bool] = Field(False, description="是否仅减仓")
    client_order_id: Optional[str] = Field(None, description="客户端自定义订单 ID")
    # 溯源与协同模式
    copilot_mode: CopilotMode = Field(CopilotMode.MANUAL, description="触发此订单的模式")
    ai_analysis_id: Optional[Union[int, str]] = Field(None, description="关联的 AI 研判 ID (若为一键采纳下单)")


OrderCreateRequest = PlaceOrderRequest  # 别名兼容


class AdoptAIPlanRequest(BaseModel):
    """一键采纳 AI 建议交易计划下单请求 (对齐前端 AIAdoptPayload)"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field("BTC-USDT-SWAP", description="标的代码")
    action: Optional[SignalAction] = Field(None, description="交易方向")
    analysis_id: Optional[Union[str, int]] = Field(None, alias="ai_analysis_id", description="所采纳的 AI 研判记录 ID (UUID 或 ID)")
    ai_analysis_id: Optional[Union[int, str]] = Field(None, description="别名兼容")
    selected_entry_price: Optional[float] = Field(None, alias="entry_price", description="选定的入场价")
    entry_price: Optional[float] = Field(None, description="别名兼容")
    stop_loss_price: Optional[float] = Field(None, description="止损价格")
    take_profit_target_1: Optional[float] = Field(None, description="第一止盈目标位")
    take_profit_target_2: Optional[float] = Field(None, description="第二止盈目标位")
    order_type: OrderType = Field(OrderType.LIMIT, description="入场订单类型")
    leverage: Optional[int] = Field(None, alias="suggested_leverage", description="杠杆倍数")
    suggested_leverage: Optional[int] = Field(None, description="别名兼容")
    notional_usdt: Optional[float] = Field(None, alias="notional_usd", description="本次计划下单名义金额 (USDT，若为空则由系统按 2% 风险预算自动计算)")
    notional_usd: Optional[float] = Field(None, description="别名兼容")
    confidence_score: Optional[float] = Field(None, description="信号置信度")
    copilot_mode: Optional[CopilotMode] = Field(None, description="协同模式 (MANUAL / SEMI_AUTO / FULL_AUTO)")




AIAdoptPayload = AdoptAIPlanRequest  # 别名兼容


class CancelOrderRequest(BaseModel):
    """撤单请求"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="标的代码")
    client_order_id: Optional[str] = Field(None, description="客户端订单 ID")
    okx_order_id: Optional[str] = Field(None, alias="order_id", description="OKX 订单 ID")
    order_id: Optional[str] = Field(None, description="别名兼容")


OrderCancelRequest = CancelOrderRequest  # 别名兼容


class BatchCancelOrdersRequest(BaseModel):
    """批量撤单请求"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: Optional[str] = Field(None, description="指定标的代码 (若为空则撤销所有标的的挂单)")
    order_ids: Optional[List[str]] = Field(None, description="指定撤销的订单 ID 列表")


class OrderModel(BaseModel):
    """订单完整详情模型 (对齐数据库与前端 OrderItem)"""
    model_config = ConfigDict(populate_by_name=True)

    id: Optional[int] = Field(None, description="数据库自增 ID")
    client_order_id: str = Field(..., description="客户端唯一订单号")
    okx_order_id: Optional[str] = Field(None, description="OKX 平台订单号")
    order_id: Optional[str] = Field(None, description="别名兼容")
    symbol: str = Field(..., description="标的代码")
    inst_type: InstType = Field(InstType.SWAP, description="产品类型")
    side: OrderSide = Field(..., description="买卖方向")
    pos_side: PosSide = Field(PosSide.NET, description="持仓方向")
    order_type: OrderType = Field(..., description="订单类型")
    price: Optional[float] = Field(None, description="委托价")
    sz: float = Field(0.0, description="委托数量")
    size: Optional[float] = Field(None, description="数量别名")
    notional_usdt: float = Field(0.0, description="名义金额 (USDT)")
    notional_usd: Optional[float] = Field(None, description="别名兼容")
    filled_sz: float = Field(0.0, description="已成交数量")
    filled_size: Optional[float] = Field(None, description="别名兼容")
    avg_price: float = Field(0.0, description="成交均价")
    avg_px: Optional[float] = Field(None, description="别名兼容")
    state: OrderState = Field(..., description="订单状态")
    leverage: int = Field(1, description="杠杆倍数")
    tp_trigger_price: Optional[float] = Field(None, description="止盈触发价")
    tp_ord_price: Optional[float] = Field(None, description="止盈委托价")
    tp_order_price: Optional[float] = Field(None, description="别名兼容")
    sl_trigger_price: Optional[float] = Field(None, description="止损触发价")
    sl_ord_price: Optional[float] = Field(None, description="止损委托价")
    sl_order_price: Optional[float] = Field(None, description="别名兼容")
    fee: float = Field(0.0, description="手续费 (USDT)")
    fee_currency: str = Field("USDT", description="手续费币种")
    pnl: float = Field(0.0, description="实现盈亏 (USDT)")
    risk_check_passed: bool = Field(True, description="是否通过风控")
    risk_check_msg: Optional[str] = Field(None, description="风控说明/拒绝原因")
    reject_reason: Optional[str] = Field(None, description="别名兼容")
    copilot_mode: CopilotMode = Field(CopilotMode.MANUAL, description="协同模式")
    ai_analysis_id: Optional[Union[int, str]] = Field(None, description="关联 AI 研判 ID")
    created_at: Union[int, datetime] = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), description="创建时间戳 (ms)")
    updated_at: Union[int, datetime] = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), description="更新时间戳 (ms)")

    @model_validator(mode="before")
    @classmethod
    def sync_order_before(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "order_id" in data and "okx_order_id" not in data:
                data["okx_order_id"] = data["order_id"]
            if "size" in data and "sz" not in data:
                data["sz"] = data["size"]
            if "notional_usd" in data and "notional_usdt" not in data:
                data["notional_usdt"] = data["notional_usd"]
            if "filled_size" in data and "filled_sz" not in data:
                data["filled_sz"] = data["filled_size"]
            if "avg_px" in data and "avg_price" not in data:
                data["avg_price"] = data["avg_px"]
            if "tp_order_price" in data and "tp_ord_price" not in data:
                data["tp_ord_price"] = data["tp_order_price"]
            if "sl_order_price" in data and "sl_ord_price" not in data:
                data["sl_ord_price"] = data["sl_order_price"]
            if "reject_reason" in data and "risk_check_msg" not in data:
                data["risk_check_msg"] = data["reject_reason"]
        return data

    @model_validator(mode="after")
    def sync_order_after(self) -> "OrderModel":
        if self.order_id is None:
            self.order_id = self.okx_order_id
        if self.okx_order_id is None:
            self.okx_order_id = self.order_id

        if self.size is None:
            self.size = self.sz
        if self.sz == 0.0 and self.size:
            self.sz = self.size

        if self.notional_usd is None:
            self.notional_usd = self.notional_usdt
        if self.notional_usdt == 0.0 and self.notional_usd:
            self.notional_usdt = self.notional_usd

        if self.filled_size is None:
            self.filled_size = self.filled_sz
        if self.filled_sz == 0.0 and self.filled_size:
            self.filled_sz = self.filled_size

        if self.avg_px is None:
            self.avg_px = self.avg_price
        if self.avg_price == 0.0 and self.avg_px:
            self.avg_price = self.avg_px

        if self.tp_order_price is None:
            self.tp_order_price = self.tp_ord_price
        if self.tp_ord_price is None:
            self.tp_ord_price = self.tp_order_price

        if self.sl_order_price is None:
            self.sl_order_price = self.sl_ord_price
        if self.sl_ord_price is None:
            self.sl_ord_price = self.sl_order_price

        if self.reject_reason is None:
            self.reject_reason = self.risk_check_msg
        if self.risk_check_msg is None:
            self.risk_check_msg = self.reject_reason

        return self

    @field_serializer("created_at", "updated_at")
    def serialize_datetime(self, dt: Union[int, datetime]) -> int:
        if isinstance(dt, datetime):
            return int(dt.timestamp() * 1000)
        return dt


OrderItem = OrderModel  # 别名兼容


class SetPositionTPSLRequest(BaseModel):
    """针对现有持仓设置/修改止盈止损请求"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="标的代码")
    pos_side: PosSide = Field(..., description="持仓方向")
    tp_trigger_price: Optional[float] = Field(None, description="新止盈触发价 (传 null 或 0 表示取消)")
    tp_ord_price: Optional[float] = Field(None, alias="tp_order_price", description="止盈委托价 (默认为市价)")
    tp_order_price: Optional[float] = Field(None, description="别名兼容")
    sl_trigger_price: Optional[float] = Field(None, description="新止损触发价 (传 null 或 0 表示取消)")
    sl_ord_price: Optional[float] = Field(None, alias="sl_order_price", description="止损委托价 (默认为市价)")
    sl_order_price: Optional[float] = Field(None, description="别名兼容")


PositionTPSLRequest = SetPositionTPSLRequest  # 别名兼容


class ClosePositionRequest(BaseModel):
    """单独平仓请求"""
    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(..., description="标的代码")
    pos_side: PosSide = Field(..., description="持仓方向 (long / short / net)")
    order_type: OrderType = Field(OrderType.MARKET, description="平仓类型 (market / limit)")
    price: Optional[float] = Field(None, description="限价平仓价格")
    sz: Optional[float] = Field(None, alias="size", description="平仓数量 (若为空则全部平仓)")
    size: Optional[float] = Field(None, description="别名兼容")


class PositionModel(BaseModel):
    """当前持仓模型 (全称与缩写双向兼容映射，严格对齐前端 PositionItem)"""
    model_config = ConfigDict(populate_by_name=True)

    id: Optional[int] = Field(None, description="数据库主键")
    pos_id: Optional[str] = Field(None, description="持仓唯一标识")
    symbol: str = Field(..., description="标的代码")
    inst_type: InstType = Field(InstType.SWAP, description="产品类型")
    pos_side: PosSide = Field(..., description="持仓方向")
    pos: float = Field(0.0, description="持仓数量")
    pos_size: Optional[float] = Field(None, description="持仓数量别名")
    avail_pos: float = Field(0.0, description="可平仓数量")
    avg_px: float = Field(0.0, description="开仓均价")
    avg_price: Optional[float] = Field(None, description="均价别名")
    mark_px: float = Field(0.0, description="最新标记价格")
    mark_price: Optional[float] = Field(None, description="标记价别名")
    liq_px: Optional[float] = Field(None, description="预估强平价")
    liquidation_price: Optional[float] = Field(None, description="强平价别名")
    margin_ratio: float = Field(0.0, description="当前保证金率")
    mgn_ratio: Optional[float] = Field(None, description="保证金率别名")
    maint_margin_ratio: float = Field(0.01, description="维持保证金率")
    leverage: int = Field(1, description="持仓杠杆倍数")
    notional_usd: Optional[float] = Field(None, description="名义持仓价值 (USDT)")
    upl: float = Field(0.0, description="未实现盈亏 (USDT)")
    upl_ratio: float = Field(0.0, description="未实现盈亏率 (例如 0.12 表示 +12%)")
    realized_pnl: float = Field(0.0, description="已实现盈亏 (USDT)")
    margin: float = Field(0.0, description="占用保证金 (USDT)")
    tp_trigger_price: Optional[float] = Field(None, description="当前止盈价")
    sl_trigger_price: Optional[float] = Field(None, description="当前止损价")
    is_breakeven_locked: bool = Field(False, description="是否已触发第一止盈并锁定移动保本")
    tp_triggered_50: bool = Field(False, description="是否已完成 50% 阶梯止盈")
    scale_layers: int = Field(1, description="当前已加仓层数 (初始建仓为1)")
    last_scale_price: Optional[float] = Field(None, description="上一次加仓价格")
    last_scale_time: Optional[int] = Field(None, description="上一次加仓时间戳 (ms)")
    is_danger_alert: bool = Field(False, description="是否触发强平危险告警")
    peak_price: Optional[float] = Field(None, description="持仓期间历史最高标记价")
    peak_profit_ratio: Optional[float] = Field(None, description="持仓期间历史最高浮盈率")
    last_synced_at: Union[int, datetime] = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), description="与 OKX 同步时间")

    created_at: Optional[Union[int, datetime]] = Field(None, description="开仓时间戳")
    updated_at: Optional[Union[int, datetime]] = Field(None, description="最近更新时间戳")

    @model_validator(mode="before")
    @classmethod
    def sync_pos_before(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "avg_price" in data and "avg_px" not in data:
                data["avg_px"] = data["avg_price"]
            if "mark_price" in data and "mark_px" not in data:
                data["mark_px"] = data["mark_price"]
            if "pos_size" in data and "pos" not in data:
                data["pos"] = data["pos_size"]
            if "liquidation_price" in data and "liq_px" not in data:
                data["liq_px"] = data["liquidation_price"]
            if "mgn_ratio" in data and "margin_ratio" not in data:
                data["margin_ratio"] = data["mgn_ratio"]
        return data

    @model_validator(mode="after")
    def sync_pos_after(self) -> "PositionModel":
        if self.avg_price is None:
            self.avg_price = self.avg_px
        if self.avg_px == 0.0 and self.avg_price != 0.0:
            self.avg_px = self.avg_price

        if self.mark_price is None:
            self.mark_price = self.mark_px
        if self.mark_px == 0.0 and self.mark_price != 0.0:
            self.mark_px = self.mark_price

        if self.pos_size is None:
            self.pos_size = self.pos
        if self.pos == 0.0 and self.pos_size != 0.0:
            self.pos = self.pos_size

        if self.liquidation_price is None:
            self.liquidation_price = self.liq_px
        if self.liq_px is None and self.liquidation_price is not None:
            self.liq_px = self.liquidation_price

        if self.mgn_ratio is None:
            self.mgn_ratio = self.margin_ratio
        if self.margin_ratio == 0.0 and self.mgn_ratio != 0.0:
            self.margin_ratio = self.mgn_ratio

        if self.notional_usd is None or self.notional_usd == 0.0:
            px = self.mark_px or self.avg_px or 0.0
            sz = self.pos or self.pos_size or 0.0
            self.notional_usd = round(px * sz, 2)

        return self

    @field_serializer("last_synced_at", "created_at", "updated_at")
    def serialize_pos_dt(self, dt: Optional[Union[int, datetime]]) -> Optional[int]:
        if dt is None:
            return None
        if isinstance(dt, datetime):
            return int(dt.timestamp() * 1000)
        return dt


PositionItem = PositionModel  # 别名兼容


class AccountBalanceModel(BaseModel):
    """账户资产全景数据 (严格对齐前端 AccountBalance)"""
    model_config = ConfigDict(populate_by_name=True)

    total_equity_usdt: float = Field(..., description="账户总权益估值 (USDT)")
    total_equity_btc: Optional[float] = Field(0.0, description="账户总权益折合 (BTC)")
    available_margin_usdt: float = Field(..., description="可用保证金 (USDT)")
    available_balance_usdt: Optional[float] = Field(None, description="别名兼容")
    used_margin_usdt: float = Field(0.0, description="占用保证金 (USDT)")
    margin_used_usdt: Optional[float] = Field(None, description="别名兼容")
    frozen_balance_usdt: float = Field(0.0, description="挂单冻结金额 (USDT)")
    unrealized_pnl_usdt: float = Field(0.0, description="未实现总盈亏 (USDT)")
    today_realized_pnl_usdt: float = Field(0.0, description="当日已实现盈亏 (USDT)")
    today_pnl_usdt: Optional[float] = Field(None, description="别名兼容")
    today_pnl_ratio: float = Field(0.0, description="当日收益率")
    today_pnl_pct: Optional[float] = Field(None, description="别名兼容")
    margin_ratio: float = Field(..., description="账户整体保证金率")
    is_warning_margin: Optional[bool] = Field(False, description="是否处于强平警戒状态")
    timestamp: int = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), description="资产更新毫秒时间戳")
    updated_at: Optional[int] = Field(None, description="别名兼容")

    @model_validator(mode="before")
    @classmethod
    def sync_bal_before(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "available_balance_usdt" in data and "available_margin_usdt" not in data:
                data["available_margin_usdt"] = data["available_balance_usdt"]
            if "margin_used_usdt" in data and "used_margin_usdt" not in data:
                data["used_margin_usdt"] = data["margin_used_usdt"]
            if "today_pnl_usdt" in data and "today_realized_pnl_usdt" not in data:
                data["today_realized_pnl_usdt"] = data["today_pnl_usdt"]
            if "today_pnl_pct" in data and "today_pnl_ratio" not in data:
                data["today_pnl_ratio"] = data["today_pnl_pct"]
            if "updated_at" in data and "timestamp" not in data:
                data["timestamp"] = data["updated_at"]
        return data

    @model_validator(mode="after")
    def sync_bal_after(self) -> "AccountBalanceModel":
        if self.available_balance_usdt is None:
            self.available_balance_usdt = self.available_margin_usdt
        if self.margin_used_usdt is None:
            self.margin_used_usdt = self.used_margin_usdt
        if self.today_pnl_usdt is None:
            self.today_pnl_usdt = self.today_realized_pnl_usdt
        if self.today_pnl_pct is None:
            self.today_pnl_pct = self.today_pnl_ratio
        if self.updated_at is None:
            self.updated_at = self.timestamp
        return self


AccountBalance = AccountBalanceModel  # 别名兼容


# =============================================================================
# 9. 胜率归因与审计模型 (Attribution & Audit Models)
# =============================================================================

class AttributionLogModel(BaseModel):
    """AI 信号归因跟踪模型 (严格对齐 AIHistoryLogItem)"""
    model_config = ConfigDict(populate_by_name=True)

    id: int = Field(..., description="主键 ID")
    ai_analysis_id: Union[int, str] = Field(..., alias="analysis_uuid", description="对应的 AI 研判 ID / UUID")
    analysis_uuid: Optional[str] = Field(None, description="别名兼容")
    symbol: str = Field(..., description="标的代码")
    timeframe: Optional[str] = Field("1h", description="研判周期")
    signal_direction: SignalAction = Field(..., alias="signal_action", description="研判方向/动作")
    signal_action: Optional[SignalAction] = Field(None, description="别名兼容")
    confidence_score: Optional[int] = Field(80, description="信号置信度 (0~100)")
    signal_price: float = Field(..., alias="actual_market_price", description="发信号时基准价格")
    actual_market_price: Optional[float] = Field(None, description="别名兼容")
    suggested_entry_price: Optional[float] = Field(None, description="建议入场均价")
    target_1_price: float = Field(..., alias="take_profit_target_1", description="目标 1 价格")
    take_profit_target_1: Optional[float] = Field(None, description="别名兼容")
    target_2_price: Optional[float] = Field(0.0, description="目标 2 价格")
    stop_loss_price: float = Field(..., description="止损价格")
    is_adopted: bool = Field(False, alias="adopted", description="是否被实际采纳")
    adopted: Optional[bool] = Field(None, description="别名兼容")
    order_id: Optional[Union[int, str]] = Field(None, alias="adopted_order_id", description="关联订单 ID")
    adopted_order_id: Optional[str] = Field(None, description="别名兼容")
    entry_time: Union[int, datetime] = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), description="入场/生效时间戳 (ms)")
    exit_time: Optional[Union[int, datetime]] = Field(None, description="平仓/退出时间戳 (ms)")
    max_favorable_excursion: float = Field(0.0, alias="max_profit_after_signal", description="最大有利偏移 (MFE)")
    max_profit_after_signal: Optional[float] = Field(None, description="别名兼容")
    max_adverse_excursion: float = Field(0.0, alias="max_drawdown_after_signal", description="最大不利偏移 (MAE)")
    max_drawdown_after_signal: Optional[float] = Field(None, description="别名兼容")
    hit_target_1: bool = Field(False, description="是否命中目标1")
    hit_target_2: bool = Field(False, description="是否命中目标2")
    hit_stop_loss: bool = Field(False, description="是否命中止损")
    realized_pnl: float = Field(0.0, description="实现盈亏 (USDT)")
    realized_rr: float = Field(0.0, description="实际达成盈亏比")
    is_win: Optional[bool] = Field(None, description="是否判定为盈利单")
    outcome: Optional[AIAttributionOutcome] = Field(None, description="复盘归因结果 (WIN/LOSS/NEUTRAL/PENDING)")
    attribution_status: AttributionStatus = Field(AttributionStatus.TRACKING, description="归因状态")
    notes: Optional[str] = Field(None, description="复盘评语")
    created_at: Optional[Union[int, datetime]] = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), description="创建时间戳 (ms)")

    @field_serializer("entry_time", "exit_time", "created_at")
    def serialize_attr_dt(self, dt: Optional[Union[int, datetime]]) -> Optional[int]:
        if dt is None:
            return None
        if isinstance(dt, datetime):
            return int(dt.timestamp() * 1000)
        return dt


AIHistoryLogItem = AttributionLogModel  # 别名兼容


class RiskAuditLogModel(BaseModel):
    """风控审计日志模型 (严格对齐前端 RiskAuditLogItem)"""
    model_config = ConfigDict(populate_by_name=True)

    id: int = Field(..., description="主键 ID")
    event_type: RiskEventType = Field(..., description="事件类型")
    severity: AuditSeverity = Field(AuditSeverity.WARNING, description="严重级别")
    trigger_source: str = Field("RISK_ENGINE", description="触发源 (RISK_ENGINE, USER_MANUAL, OKX_GATEWAY)")
    rule_name: Optional[str] = Field("RISK_RULE", description="触发规则名")
    rule_limit_value: Optional[str] = Field("", description="规则设定阈值")
    current_metric_value: Optional[str] = Field("", description="触发时实际值")
    symbol: Optional[str] = Field(None, description="关联标的")
    is_blocked: bool = Field(True, description="是否被硬阻断")
    action_taken: Optional[str] = Field("BLOCKED", description="采取的动作")
    details: Optional[Dict[str, Any]] = Field(default_factory=dict, description="详细元数据 JSON")
    timestamp: int = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), alias="created_at", description="触发时间戳 (ms)")
    created_at: Optional[Union[int, datetime]] = Field(None, description="别名兼容")

    @field_serializer("created_at")
    def serialize_audit_dt(self, dt: Optional[Union[int, datetime]]) -> Optional[int]:
        if dt is None:
            return None
        if isinstance(dt, datetime):
            return int(dt.timestamp() * 1000)
        return dt


RiskAuditLogItem = RiskAuditLogModel  # 别名兼容


# =============================================================================
# 10. WebSocket 实时通信协议模型 (WebSocket Protocol Envelopes)
# 统一前后端订阅报文与广播载荷
# =============================================================================

class WSClientAction(str, Enum):
    """客户端发送给服务端的 WS 动作"""
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    PING = "ping"
    AUTH = "auth"


class WSClientMessage(BaseModel):
    """客户端发送给服务端的统一 WS 请求报文 (兼容量化通道与 OKX v5 op/args 格式)"""
    model_config = ConfigDict(populate_by_name=True)

    action: Optional[WSClientAction] = Field(None, alias="op", description="操作动作 (subscribe/unsubscribe/ping/auth)")
    op: Optional[str] = Field(None, description="OKX 兼容操作指令")
    channels: Optional[List[str]] = Field(None, description="标准订阅频道列表 (如 ['ticker:BTC-USDT-SWAP', 'positions'])")
    args: Optional[List[Dict[str, Any]]] = Field(None, description="OKX 兼容参数列表")
    token: Optional[str] = Field(None, description="鉴权 Token")
    timestamp: Optional[int] = Field(None, description="客户端时间戳 (ms)")


WSSubscribeMessage = WSClientMessage  # 别名兼容


class WSPingMessage(BaseModel):
    """客户端心跳 Ping 包"""
    model_config = ConfigDict(populate_by_name=True)

    action: WSClientAction = Field(WSClientAction.PING, alias="op", description="心跳动作")
    timestamp: int = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), description="客户端时间戳")


class WSPongMessage(BaseModel):
    """服务端心跳 Pong 响应"""
    model_config = ConfigDict(populate_by_name=True)

    event: str = Field("pong", description="心跳响应事件")
    timestamp: int = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), description="服务端时间戳")


class WSServerEvent(str, Enum):
    """服务端推送事件类型"""
    SNAPSHOT = "snapshot"
    UPDATE = "update"
    ERROR = "error"
    AUTH_SUCCESS = "auth_success"
    SUBSCRIBED = "subscribed"
    UNSUBSCRIBED = "unsubscribed"
    PONG = "pong"
    NOTICE = "notice"


class WSPushPayload(BaseModel, Generic[T]):
    """服务端标准 WebSocket 推送数据包 (100% 对齐前端 WSServerMessage / WSPushPayload)"""
    model_config = ConfigDict(populate_by_name=True)

    channel: Optional[str] = Field(None, description="所属频道 (如 ticker:BTC-USDT-SWAP, orders, positions)")
    event: WSServerEvent = Field(WSServerEvent.UPDATE, description="事件类型")
    data: Optional[Union[T, List[T]]] = Field(None, description="实时推送数据实体或数据列表")
    timestamp: int = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), description="推送时间戳 (ms)")
    code: Optional[int] = Field(0, description="状态码 (0 成功)")
    msg: Optional[str] = Field("success", description="提示信息")
    arg: Optional[Dict[str, Any]] = Field(None, description="订阅频道与参数元数据")


WSServerMessage = WSPushPayload  # 别名兼容


# =============================================================================
# 11. 模拟操盘沙盒与系统模式切换模型 (Paper Trading & Mode Switch)
# =============================================================================

class PaperAccountInfo(BaseModel):
    """模拟账户资产与统计信息"""
    initial_equity_usdt: float = Field(10000.0, description="初始虚拟本金 (USDT)")
    total_equity_usdt: float = Field(10000.0, description="当前模拟总权益 (USDT)")
    available_margin_usdt: float = Field(10000.0, description="模拟可用保证金 (USDT)")
    used_margin_usdt: float = Field(0.0, description="模拟占用保证金 (USDT)")
    unrealized_pnl_usdt: float = Field(0.0, description="当前未实现浮动盈亏 (USDT)")
    realized_pnl_usdt: float = Field(0.0, description="累计已实现盈亏 (USDT)")
    today_realized_pnl_usdt: float = Field(0.0, description="今日已实现盈亏 (USDT)")
    today_pnl_ratio: float = Field(0.0, description="今日收益率")
    total_trades_count: int = Field(0, description="累计模拟成交笔数")
    winning_trades_count: int = Field(0, description="盈利笔数")
    win_rate: float = Field(0.0, description="模拟胜率 (0.0~1.0)")
    created_at: int = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000))
    updated_at: int = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000))


class PaperResetRequest(BaseModel):
    """重置模拟账户请求"""
    initial_equity_usdt: float = Field(10000.0, ge=100.0, le=10000000.0, description="重置初始本金 (100 ~ 1,000,000 USDT)")


class SwitchModeRequest(BaseModel):
    """切换实盘 / 模拟操盘模式请求"""
    target_live_mode: bool = Field(..., description="目标模式: True 实盘, False 模拟沙盒")
    master_password: Optional[str] = Field(None, description="切换至实盘模式时需提供主密码进行凭证解密探测")


# =============================================================================
# 12. 开启自动操盘后的全景盈利与状态模型 (Auto-Pilot Session & Profit Tracking)
# =============================================================================

class AutoPilotStatusModel(BaseModel):
    """自动量化操盘实时状态与收益统计模型 (100% 契约对齐)"""
    model_config = ConfigDict(populate_by_name=True)

    is_running: bool = Field(False, description="当前是否处于自动操盘开启状态")
    session_id: Optional[str] = Field(None, description="当前自动操盘会话唯一ID")
    start_time: Optional[int] = Field(None, description="自动操盘开启时间戳 (ms)")
    running_seconds: int = Field(0, description="开启以来的累计运行时长 (秒)")
    start_equity_usdt: float = Field(10000.0, description="开启时的基准账户总权益 (USDT)")
    current_equity_usdt: float = Field(10000.0, description="当前账户总权益 (USDT)")
    total_pnl_usdt: float = Field(0.0, description="开启以来综合总盈亏 (USDT = 当前权益 - 开启基准权益)")
    total_pnl_ratio: float = Field(0.0, description="开启以来综合收益率 (例如 0.052 表示 +5.2%)")
    realized_pnl_usdt: float = Field(0.0, description="开启以来累计已实现盈亏 (USDT)")
    unrealized_pnl_usdt: float = Field(0.0, description="当前持仓未实现浮动盈亏 (USDT)")
    total_trades_count: int = Field(0, description="开启以来自动交易总笔数")
    winning_trades_count: int = Field(0, description="盈利单数")
    losing_trades_count: int = Field(0, description="亏损单数")
    win_rate: float = Field(0.0, description="开启以来胜率 (0.0 ~ 1.0)")
    profit_factor: float = Field(0.0, description="收益因子 / 盈亏比")
    max_drawdown_pct: float = Field(0.0, description="开启以来最大回撤百分比")
    active_symbol: str = Field("BTC-USDT-SWAP", description="当前主要关注标的")
    symbols: List[str] = Field(default_factory=lambda: ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "XAU-USDT-SWAP", "NVDA-USDT-SWAP", "ONDO-USDT-SWAP"], description="多标的自动监控轮巡池")
    focus_symbols: List[str] = Field(default_factory=lambda: ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "XAU-USDT-SWAP", "NVDA-USDT-SWAP", "ONDO-USDT-SWAP"], description="自选与专注标的池")
    focus_mode: str = Field("SINGLE_SYMBOL", description="操盘专注模式: SINGLE_SYMBOL 单标的专注量化 / MULTI_SYMBOL_POOL 多标的池")
    max_scale_layers: int = Field(3, description="最大加仓层数 (1~5层)")
    scale_in_strategy: str = Field("SMART_HYBRID", description="加仓策略类型: PYRAMIDING 顺势金字塔 / PULLBACK_DCA 动态回调 / SMART_HYBRID 智能混合")
    auto_reversal_enabled: bool = Field(True, description="强反向信号是否自动反手平反开")
    max_concurrent_positions: int = Field(3, description="最大同时持仓标的数 (防过度分散)")
    trailing_stop_enabled: bool = Field(True, description="是否启用动态移动保本与跟踪止损")
    breakeven_trigger_ratio: float = Field(0.5, description="浮盈达到目标位比例时移至保本")
    trailing_profit_threshold: float = Field(0.012, description="跟踪止盈激活浮盈门槛 (例如 0.012 表示浮盈达 1.2% 时激活跟踪盯市)")
    trailing_profit_callback: float = Field(0.25, description="跟踪止盈最高点回撤比例 (例如 0.25 表示从最高浮盈回撤 25% 时主动止盈平仓)")
    reentry_cooldown_seconds: int = Field(90, description="平仓后重新入场冷却时间 (秒，避免刚止盈就在同一高位假突破追高)")
    funding_fee_guard_enabled: bool = Field(True, description="是否启用资金费结算时点防收割保护 (结算前15分钟规避高费率建仓)")
    min_confidence: float = Field(0.70, description="自动跟单最低置信度门槛")
    auto_leverage_enabled: bool = Field(True, description="是否启用自适应智能杠杆")
    stop_loss_equity_floor: Optional[float] = Field(None, description="最低保留本金底线 (USDT，亏损至该值自动熔断停止操盘)")
    max_loss_limit_usdt: Optional[float] = Field(None, description="最大允许亏损金额 (USDT)")
    aggressiveness_mode: str = Field("BALANCED", description="操盘激进程度: CONSERVATIVE 保守稳健 / BALANCED 标准量化 / AGGRESSIVE 积极进攻")
    is_live_mode: bool = Field(False, description="是否为实盘环境")
    last_patrol_time: Optional[int] = Field(None, description="最近一次后端自动巡检时间戳 (ms)")
    last_patrol_log: Optional[str] = Field(None, description="最近一次后端自动巡检结果概要")
    last_updated_at: int = Field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000), description="最新统计更新时间戳 (ms)")


class AutoPilotHistoryPoint(BaseModel):
    """自动操盘时序收益曲线采样点"""
    timestamp: int = Field(..., description="采样时间戳 (ms)")
    equity: float = Field(..., description="该时刻账户总权益 (USDT)")
    realized_pnl: float = Field(0.0, description="该时刻累计已实现盈亏 (USDT)")
    unrealized_pnl: float = Field(0.0, description="该时刻持仓浮动盈亏 (USDT)")
    total_pnl: float = Field(0.0, description="该时刻开启以来综合总盈亏 (USDT)")
    pnl_ratio: float = Field(0.0, description="该时刻收益率")


class AutoPilotTradeRecord(BaseModel):
    """自动操盘产生的交易记录明细"""
    id: Optional[str] = Field(None, description="记录 ID")
    symbol: str = Field(..., description="标的代码")
    pos_side: str = Field(..., description="持仓方向 long/short")
    entry_price: float = Field(..., description="开仓均价")
    exit_price: float = Field(..., description="平仓价格")
    size: float = Field(..., description="成交数量 (币数)")
    leverage: int = Field(1, description="杠杆倍数")
    realized_pnl: float = Field(..., description="该笔已实现盈亏 (USDT)")
    roi_pct: float = Field(0.0, description="该笔收益率 (%)")
    reason: str = Field("AI策略自动平仓", description="平仓触发原因")
    timestamp: int = Field(..., description="平仓完成时间戳 (ms)")


class AutoPilotHistoryResponse(BaseModel):
    """自动操盘历史收益与交易流水响应体"""
    session_id: Optional[str] = Field(None, description="会话ID")
    is_running: bool = Field(False, description="当前是否在运行")
    curve: List[AutoPilotHistoryPoint] = Field(default_factory=list, description="时序收益走势采样点列表")
    trades: List[AutoPilotTradeRecord] = Field(default_factory=list, description="自动操盘产生的平仓明细列表")


class AutoPilotStartRequest(BaseModel):
    """开启自动操盘请求"""
    aggressiveness_mode: Optional[str] = Field("BALANCED", description="操盘激进程度: CONSERVATIVE / BALANCED / AGGRESSIVE")
    min_confidence: Optional[float] = Field(0.70, ge=0.50, le=0.99, description="自动跟单最低置信度门槛")
    auto_leverage_enabled: Optional[bool] = Field(True, description="是否启用自适应智能杠杆")
    stop_loss_equity_floor: Optional[float] = Field(None, description="最低保留本金底线 (USDT，如当前8000U设7000U，亏损至该值自动停止操盘)")
    max_loss_limit_usdt: Optional[float] = Field(None, description="最大允许亏损金额 (USDT)")
    symbol: Optional[str] = Field("BTC-USDT-SWAP", description="当前主标的")
    symbols: Optional[List[str]] = Field(None, description="多标的自动监控轮巡池")
    focus_symbols: Optional[List[str]] = Field(None, description="自选与专注标的列表")
    focus_mode: Optional[str] = Field("SINGLE_SYMBOL", description="操盘专注模式: SINGLE_SYMBOL / MULTI_SYMBOL_POOL")
    max_scale_layers: Optional[int] = Field(3, ge=1, le=5, description="最大加仓层数 (1~5)")
    scale_in_strategy: Optional[str] = Field("SMART_HYBRID", description="加仓策略类型")
    auto_reversal_enabled: Optional[bool] = Field(True, description="强反向信号是否自动反手")
    max_concurrent_positions: Optional[int] = Field(3, ge=1, le=10, description="最大同时持仓标的数")
    trailing_stop_enabled: Optional[bool] = Field(True, description="是否启用动态移动保本与跟踪止损")
    breakeven_trigger_ratio: Optional[float] = Field(0.5, ge=0.1, le=0.9, description="浮盈达到目标位比例时移至保本")
    trailing_profit_threshold: Optional[float] = Field(0.012, description="跟踪止盈激活浮盈门槛")
    trailing_profit_callback: Optional[float] = Field(0.25, description="跟踪止盈最高点回撤比例")
    reentry_cooldown_seconds: Optional[int] = Field(90, description="平仓后重新入场冷却时间 (秒)")
    funding_fee_guard_enabled: Optional[bool] = Field(True, description="是否启用资金费结算时点防收割保护")


class AutoPilotConfigUpdateRequest(BaseModel):
    """更新自动操盘策略参数请求"""
    aggressiveness_mode: Optional[str] = Field(None, description="操盘激进程度: CONSERVATIVE / BALANCED / AGGRESSIVE")
    min_confidence: Optional[float] = Field(None, ge=0.50, le=0.99, description="自动跟单最低置信度门槛")
    auto_leverage_enabled: Optional[bool] = Field(None, description="是否启用自适应智能杠杆")
    stop_loss_equity_floor: Optional[float] = Field(None, description="最低保留本金底线 (USDT，亏损至该值自动熔断停止操盘)")
    max_loss_limit_usdt: Optional[float] = Field(None, description="最大允许亏损金额 (USDT)")
    symbol: Optional[str] = Field(None, description="当前主标的")
    symbols: Optional[List[str]] = Field(None, description="多标的自动监控轮巡池")
    focus_symbols: Optional[List[str]] = Field(None, description="自选与专注标的列表")
    focus_mode: Optional[str] = Field(None, description="操盘专注模式: SINGLE_SYMBOL / MULTI_SYMBOL_POOL")
    max_scale_layers: Optional[int] = Field(None, ge=1, le=5, description="最大加仓层数 (1~5)")
    scale_in_strategy: Optional[str] = Field(None, description="加仓策略类型")
    auto_reversal_enabled: Optional[bool] = Field(None, description="强反向信号是否自动反手")
    max_concurrent_positions: Optional[int] = Field(None, ge=1, le=10, description="最大同时持仓标的数")
    trailing_stop_enabled: Optional[bool] = Field(None, description="是否启用动态移动保本与跟踪止损")
    breakeven_trigger_ratio: Optional[float] = Field(None, ge=0.1, le=0.9, description="浮盈达到目标位比例时移至保本")
    trailing_profit_threshold: Optional[float] = Field(None, description="跟踪止盈激活浮盈门槛")
    trailing_profit_callback: Optional[float] = Field(None, description="跟踪止盈最高点回撤比例")
    reentry_cooldown_seconds: Optional[int] = Field(None, description="平仓后重新入场冷却时间 (秒)")
    funding_fee_guard_enabled: Optional[bool] = Field(None, description="是否启用资金费结算时点防收割保护")


# =============================================================================
# 11. 标的元数据与自选分类模型 (Instrument & Symbol Metadata)
# =============================================================================

class InstrumentCategory(str, Enum):
    """标的资产分类"""
    TRADFI = "tradfi"            # 传统金融 / 美股代币 / 黄金白银大宗
    RWA = "rwa"                  # 现实世界资产 (Real World Assets)
    CRYPTO = "crypto"            # 主流加密资产
    COMMODITIES = "commodities"  # 大宗商品 (黄金/白银/原油等)
    EQUITIES = "equities"        # 美股/指数类代币化标的
    POSITIONS = "positions"      # 用户当前持仓标的
    CUSTOM = "custom"            # 自定义添加


class InstrumentInfo(BaseModel):
    """交易标的元数据信息"""
    symbol: str = Field(..., description="标的代码 (如 BTC-USDT-SWAP, XAU-USDT-SWAP, NVDA-USDT-SWAP)")
    inst_id: str = Field(..., description="OKX产品代码")
    inst_type: InstType = Field(InstType.SWAP, description="产品类型 SPOT/SWAP/FUTURES")
    base_ccy: str = Field(..., description="交易基础币种 (如 BTC, XAU, NVDA, ONDO)")
    quote_ccy: str = Field("USDT", description="计价币种 (如 USDT, USD)")
    settle_ccy: Optional[str] = Field("USDT", description="盈亏结算币种")
    name: str = Field(..., description="中文展示全称 (如 黄金永续, 英伟达代币化合约, 比特币永续)")
    category: InstrumentCategory = Field(InstrumentCategory.CRYPTO, description="标的资产分类")
    is_tradfi: bool = Field(False, description="是否属于 TradFi / RWA / 大宗 / 美股类标的")
    ct_val: Optional[float] = Field(1.0, description="合约面值")
    ct_mult: Optional[float] = Field(None, description="合约乘数")
    ct_val_ccy: Optional[str] = Field(None, description="合约面值计价货币")
    tick_sz: Optional[float] = Field(0.1, description="价格下单最小变动单位 (步长)")
    lot_sz: Optional[float] = Field(0.01, description="数量下单最小变动单位 (步长)")
    min_sz: Optional[float] = Field(0.01, description="最小下单数量")
    lever: Optional[float] = Field(None, description="最大可用杠杆倍数")
    state: Optional[str] = Field("live", description="交易状态 (live 正常交易)")


class InstrumentSearchResponse(BaseModel):
    """标的搜索响应模型"""
    instruments: List[InstrumentInfo] = Field(default_factory=list, description="匹配的OKX交易标的列表")
    total_count: int = Field(0, description="匹配结果总数")
    query: str = Field("", description="检索关键词")
    cached: bool = Field(True, description="是否来自本地内存索引缓存")


class SubscribeFocusRequest(BaseModel):
    """批量订阅专注标的请求"""
    symbols: List[str] = Field(..., description="标的代码列表 (如 ['BTC-USDT-SWAP', 'PEPE-USDT-SWAP'])")


class SubscribeFocusResponse(BaseModel):
    """批量订阅专注标的响应"""
    subscribed_symbols: List[str] = Field(default_factory=list, description="成功订阅的标的列表")


class SymbolCategoryGroup(BaseModel):
    """标的分组列表"""
    category: InstrumentCategory = Field(..., description="分类枚举")
    category_name: str = Field(..., description="分类中文名")
    icon: str = Field("📊", description="分类图标")
    symbols: List[InstrumentInfo] = Field(default_factory=list, description="该分类下的标的列表")


class SymbolListResponse(BaseModel):
    """标的列表全景响应体"""
    categories: List[SymbolCategoryGroup] = Field(default_factory=list, description="分类推荐标的池")
    total_count: int = Field(0, description="标的总数")


# =============================================================================
# 12. 模块 A：历史时序数据湖与回测沙盒模型 (Data Lake & Backtest Models)
# =============================================================================

class HistoricalKlineRecord(BaseModel):
    """单根历史K线与衍生品特征记录"""
    symbol: str = Field(..., description="交易标的 (如 BTC-USDT-SWAP)")
    timeframe: str = Field(..., description="周期 (15m, 1h, 4h, 1d)")
    timestamp: int = Field(..., description="毫秒时间戳")
    open_price: float = Field(..., description="开盘价")
    high_price: float = Field(..., description="最高价")
    low_price: float = Field(..., description="最低价")
    close_price: float = Field(..., description="收盘价")
    volume: float = Field(..., description="成交量")
    funding_rate: float = Field(0.0, description="资金费率")
    open_interest: float = Field(0.0, description="持仓量")


class DataLakeSyncRequest(BaseModel):
    """数据湖同步拉取历史K线请求"""
    symbol: str = Field(..., description="交易标的代码")
    timeframes: List[str] = Field(default=["15m", "1h", "4h", "1d"], description="拉取周期列表")
    days_back: int = Field(default=30, ge=1, le=1000, description="同步历史天数")


class DataLakeStatusItem(BaseModel):
    """单标的各周期数据湖存储现状"""
    symbol: str = Field(..., description="标的代码")
    timeframe: str = Field(..., description="周期")
    count: int = Field(0, description="K线总根数")
    start_time: Optional[int] = Field(None, description="最早时间戳")
    end_time: Optional[int] = Field(None, description="最新时间戳")


class DataLakeStatusResponse(BaseModel):
    """数据湖状态全景响应体"""
    total_klines: int = Field(0, description="全库K线总数")
    items: List[DataLakeStatusItem] = Field(default_factory=list, description="各标的覆盖明细")


class BacktestRunRequest(BaseModel):
    """回测沙盒单次运行请求"""
    strategy_name: str = Field(default="EMA_BREAKOUT", description="回测策略名称")
    symbol: str = Field(default="BTC-USDT-SWAP", description="标的代码")
    timeframe: str = Field(default="1h", description="回测主周期")
    days_back: int = Field(default=60, ge=3, le=1000, description="回测回溯天数")
    initial_balance: float = Field(default=10000.0, ge=100.0, description="回测初始本金(USDT)")
    leverage: int = Field(default=3, ge=1, le=20, description="杠杆倍数")
    taker_fee_rate: float = Field(default=0.0005, description="Taker 手续费率 (默认 0.05%)")
    maker_fee_rate: float = Field(default=0.0002, description="Maker 手续费率 (默认 0.02%)")
    slippage_pct: float = Field(default=0.0005, description="保守滑点模型 (默认 0.05%)")
    atr_sl_multiplier: float = Field(default=2.0, ge=0.5, le=5.0, description="ATR 动态止损倍数")
    breakeven_ratio: float = Field(default=0.5, ge=0.1, le=0.9, description="移动保本触发浮盈比例")
    funding_rate_8h: float = Field(default=0.0001, description="8小时资金费率基准 (默认 0.01%)")
    params: Dict[str, Any] = Field(default_factory=dict, description="额外策略参数")


class BacktestTradeRecord(BaseModel):
    """回测逐笔交易详情"""
    trade_id: int = Field(..., description="交易序号")
    entry_time: int = Field(..., description="入场时间戳")
    exit_time: int = Field(..., description="出场时间戳")
    pos_side: str = Field(..., description="long/short")
    entry_price: float = Field(..., description="开仓价")
    exit_price: float = Field(..., description="平仓价")
    size: float = Field(..., description="开仓规模")
    pnl: float = Field(..., description="该笔盈亏(USDT)")
    roi_pct: float = Field(..., description="收益率%")
    fee: float = Field(..., description="扣除手续费与滑点")
    exit_reason: str = Field(..., description="平仓原因: TP/SL/BREAKEVEN/SIGNAL")
    mfe_pct: float = Field(0.0, description="最大有利偏移 MFE%")
    mae_pct: float = Field(0.0, description="最大不利偏移 MAE%")


class BacktestEquityPoint(BaseModel):
    """回测资金曲线采样点"""
    timestamp: int = Field(..., description="毫秒时间戳")
    equity: float = Field(..., description="账户净值")
    drawdown_pct: float = Field(0.0, description="当时回撤百分比")


class BacktestResultResponse(BaseModel):
    """回测实验完整指标响应体"""
    experiment_id: str = Field(..., description="实验UUID")
    strategy_name: str = Field(..., description="策略名称")
    symbol: str = Field(..., description="标的代码")
    timeframe: str = Field(..., description="回测周期")
    total_candles: int = Field(..., description="测试K线总根数")
    initial_balance: float = Field(..., description="初始本金")
    final_balance: float = Field(..., description="最终净值")
    total_return_pct: float = Field(..., description="总收益率%")
    annualized_return_pct: float = Field(..., description="年化收益率%")
    max_drawdown_pct: float = Field(..., description="最大回撤%")
    sharpe_ratio: float = Field(..., description="夏普比率")
    calmar_ratio: float = Field(..., description="卡尔玛比率")
    win_rate: float = Field(..., description="胜率")
    profit_factor: float = Field(..., description="盈亏比 / Profit Factor")
    total_trades: int = Field(..., description="总交易笔数")
    winning_trades: int = Field(..., description="胜单数")
    losing_trades: int = Field(..., description="负单数")
    avg_trade_pnl: float = Field(..., description="单笔平均盈亏")
    total_fee_paid: float = Field(..., description="累计摩擦成本(手续费+滑点)")
    equity_curve: List[BacktestEquityPoint] = Field(default_factory=list, description="资金权益走势曲线")
    trades: List[BacktestTradeRecord] = Field(default_factory=list, description="逐笔成交流水")
    execution_time_ms: int = Field(..., description="回测执行耗时(毫秒)")


class GridSearchRequest(BaseModel):
    """参数网格搜索与优化请求"""
    strategy_name: str = Field(default="EMA_BREAKOUT", description="策略名")
    symbol: str = Field(default="BTC-USDT-SWAP", description="标的代码")
    timeframe: str = Field(default="1h", description="周期")
    days_back: int = Field(default=60, ge=7, le=365, description="优化回溯天数")
    atr_multipliers: List[float] = Field(default=[1.5, 2.0, 2.5, 3.0], description="ATR止损乘数网格")
    breakeven_ratios: List[float] = Field(default=[0.3, 0.5, 0.7], description="保本触发比例网格")
    leverages: List[int] = Field(default=[2, 3, 5], description="杠杆网格")


class GridSearchResultItem(BaseModel):
    """网格搜索单项指标"""
    params: Dict[str, Any] = Field(..., description="参数组合")
    total_return_pct: float = Field(..., description="总收益率%")
    sharpe_ratio: float = Field(..., description="夏普比率")
    max_drawdown_pct: float = Field(..., description="最大回撤%")
    win_rate: float = Field(..., description="胜率")
    profit_factor: float = Field(..., description="盈亏比")
    total_trades: int = Field(..., description="成交笔数")


class GridSearchResponse(BaseModel):
    """网格搜索全量结果"""
    best_params: Dict[str, Any] = Field(..., description="最优参数组合")
    items: List[GridSearchResultItem] = Field(default_factory=list, description="全部参数组合表现排序")


# =============================================================================
# 13. 模块 B：全网情报与热点新闻感知模型 (News & Intelligence Models)
# =============================================================================

class NewsEventItem(BaseModel):
    """单条新闻资讯实体"""
    id: str = Field(..., description="新闻哈希ID")
    source: str = Field(..., description="来源: cryptopanic, rss, coindesk, manual")
    title: str = Field(..., description="新闻标题")
    content: Optional[str] = Field(None, description="摘要/正文")
    url: Optional[str] = Field(None, description="原文链接")
    published_at: int = Field(..., description="发布毫秒时间戳")
    sentiment_score: float = Field(0.0, description="AI 情感倾向: -1.0(极度利空) ~ +1.0(极度利好)")
    urgency: str = Field("P2", description="时效与等级: P0(突发黑天鹅/暴利好), P1(重要), P2(一般)")
    related_symbols: List[str] = Field(default_factory=list, description="关联标的列表")
    is_fud_or_rumor: bool = Field(False, description="是否为谣言或未经证实爆料")
    summary_zh: Optional[str] = Field(None, description="AI 中文一句话核心提炼")


class NewsFeedResponse(BaseModel):
    """新闻情报流响应"""
    news: List[NewsEventItem] = Field(default_factory=list, description="热点资讯列表")
    total_count: int = Field(0, description="总数")


class MacroCalendarEvent(BaseModel):
    """宏观经济日历事件"""
    event_id: str = Field(..., description="事件ID")
    title: str = Field(..., description="事件名称 (如: 美联储利率决议 FOMC, 美国核心CPI, 非农就业)")
    country: str = Field("US", description="国家代码")
    impact_level: str = Field("HIGH", description="影响等级: HIGH/MEDIUM/LOW")
    event_time: int = Field(..., description="发布时间戳(毫秒)")
    previous_value: Optional[str] = Field(None, description="前值")
    forecast_value: Optional[str] = Field(None, description="预测值")
    actual_value: Optional[str] = Field(None, description="公布值")
    is_imminent_risk: bool = Field(False, description="是否处于 15 分钟高危预警窗口内")


class MacroCalendarResponse(BaseModel):
    """宏观日历响应体"""
    events: List[MacroCalendarEvent] = Field(default_factory=list, description="宏观日历事件列表")


# =============================================================================
# 14. 模块 C：每日自动复盘与知识演化模型 (Daily Review & Knowledge Base)
# =============================================================================

class DailyReviewDetail(BaseModel):
    """每日深度复盘报告模型"""
    review_date: str = Field(..., description="复盘日期 YYYY-MM-DD")
    total_trades: int = Field(0, description="当日交易总笔数")
    winning_trades: int = Field(0, description="胜单数")
    losing_trades: int = Field(0, description="负单数")
    win_rate: float = Field(0.0, description="胜率")
    realized_pnl: float = Field(0.0, description="当日累计已实现盈亏(USDT)")
    fee_paid: float = Field(0.0, description="当日手续费磨损(USDT)")
    max_drawdown: float = Field(0.0, description="当日最大回撤%")
    best_trade_pnl: float = Field(0.0, description="最佳单笔盈利(USDT)")
    worst_trade_pnl: float = Field(0.0, description="最差单笔亏损(USDT)")
    summary_markdown: str = Field(..., description="AI 生成的 Markdown 结构化深度复盘报告")
    lessons_learned: List[str] = Field(default_factory=list, description="提炼的结构化避坑教训")
    strategy_adjustments: Optional[Dict[str, Any]] = Field(None, description="策略参数权重优化建议")
    created_at: str = Field(..., description="创建时间")


class DailyReviewListResponse(BaseModel):
    """复盘历史列表响应体"""
    reviews: List[DailyReviewDetail] = Field(default_factory=list, description="历史复盘列表")


class TradingKnowledgeRule(BaseModel):
    """交易经验避坑规则模型"""
    id: str = Field(..., description="规则UUID")
    rule_type: str = Field(default="MISTAKE_AVOIDANCE", description="MISTAKE_AVOIDANCE/BEST_PRACTICE/MACRO_REGIME")
    symbol_scope: str = Field(default="*", description="适用标的代码，'*' 为全部")
    trigger_condition: str = Field(..., description="触发前置场景 (如: 4H RSI > 75 且 资金费率 > 0.08%)")
    action_rule: str = Field(..., description="约束行动准则 (如: 坚决严禁追多，仅允许等待背离右侧空)")
    confidence_score: float = Field(default=1.0, ge=0.1, le=1.0, description="置信权重")
    source_review_date: Optional[str] = Field(None, description="来源复盘日期")
    times_triggered: int = Field(default=0, description="命中拦截次数")
    is_active: bool = Field(default=True, description="是否生效")
    created_at: Optional[str] = Field(None, description="创建时间")


class TradingKnowledgeListResponse(BaseModel):
    """避坑规则库列表响应"""
    rules: List[TradingKnowledgeRule] = Field(default_factory=list, description="避坑规则列表")
    total_count: int = Field(0, description="总数")


# =============================================================================
# 15. 模块 D：多智能体对抗博弈与 Telegram 协同模型 (Consensus & Telegram)
# =============================================================================

class AgentDebateOpinion(BaseModel):
    """单智能体辩论陈述"""
    role_name: str = Field(..., description="角色名称 (如: 多头辩护专家, 空头风控专家, 宏观舆情专家)")
    stance: str = Field(..., description="立场: BULLISH(看多), BEARISH(看空), NEUTRAL(中立观望)")
    confidence: float = Field(..., description="置信度评分 (0.0~1.0)")
    key_arguments: List[str] = Field(default_factory=list, description="核心论据列表")
    risk_warnings: List[str] = Field(default_factory=list, description="识别的潜在风险点")


class MultiAgentConsensusResponse(BaseModel):
    """多智能体对抗博弈综合仲裁决策"""
    consensus_id: str = Field(..., description="仲裁决策ID")
    symbol: str = Field(..., description="研判标的")
    timestamp: int = Field(..., description="研判时间戳")
    consensus_score: int = Field(..., ge=0, le=100, description="首席量化仲裁官共识得分 (0~100)")
    final_action: SignalAction = Field(..., description="最终执行动作 (BUY_LONG, SELL_SHORT, HOLD_WAIT, CLOSE_POSITION)")
    is_approved_to_execute: bool = Field(..., description="是否达到准入红线 (分>=75 且 盈亏比>=1.8 且 无冷却锁)")
    rejection_reason: Optional[str] = Field(None, description="未通过执行原因")
    bull_opinion: AgentDebateOpinion = Field(..., description="多头辩护专家观点")
    bear_opinion: AgentDebateOpinion = Field(..., description="空头风控专家观点")
    macro_opinion: AgentDebateOpinion = Field(..., description="宏观舆情专家观点")
    arbitration_summary: str = Field(..., description="首席仲裁官综合研判词")
    injected_knowledge_rules: List[str] = Field(default_factory=list, description="本次研判动态注入的前车之鉴避坑规则")
    suggested_trade_plan: Optional[Dict[str, Any]] = Field(None, description="建议交易计划 (含入场、止损、止盈与盈亏比)")


class TelegramConfig(BaseModel):
    """Telegram Bot 配置契约"""
    bot_token_masked: Optional[str] = Field(None, description="脱敏的 Bot Token")
    chat_id_masked: Optional[str] = Field(None, description="脱敏的 Chat ID")
    is_enabled: bool = Field(False, description="是否启用 TG 实时通知")
    is_panic_enabled: bool = Field(True, description="是否允许通过 TG 发送 /panic 紧急平仓")


class TelegramConfigSaveRequest(BaseModel):
    """保存 Telegram Bot 配置请求"""
    bot_token: str = Field(..., description="Telegram Bot Token")
    chat_id: str = Field(..., description="接收消息的 Chat ID")
    is_enabled: bool = Field(True, description="是否启用")
    is_panic_enabled: bool = Field(True, description="是否允许远程 Panic")


class TelegramTestMessageRequest(BaseModel):
    """发送 TG 测试消息请求"""
    message: Optional[str] = Field("🚀 OKX-Dog v3.0 交易大脑移动端已成功连通！", description="测试消息文本")


class TiltProtectionStatus(BaseModel):
    """交易心理防上头与连续亏损冷却状态"""
    is_in_cooldown: bool = Field(False, description="当前是否处于强制冷静期")
    cooldown_until_ts: Optional[int] = Field(None, description="冷却锁解除毫秒时间戳")
    cooldown_remaining_mins: int = Field(0, description="冷却剩余分钟数")
    consecutive_loss_count: int = Field(0, description="2小时内连续止损笔数")
    leverage_penalty_applied: bool = Field(False, description="是否已触发降杠杆惩罚(最大2x)")
    reason: Optional[str] = Field(None, description="冷却或惩罚原因")


# =============================================================================
# 16. 高级数理量化与风控分析契约 (源自 oke_auto_trade 优势融合)
# =============================================================================

class DcaLiquidationAnalysis(BaseModel):
    """DCA 与网格动态强平线穿透预判与保证金精算结果"""
    model_config = ConfigDict(populate_by_name=True)

    is_safe: bool = Field(..., description="加仓链条是否安全 (是否存在未决穿仓死穴)")
    max_safe_dca_orders: int = Field(..., description="最大安全可加仓轮数")
    breakeven_price: float = Field(..., description="加仓后的持仓保本均价")
    current_liq_price: float = Field(..., description="当前持仓强平价")
    next_dca_liq_price: float = Field(..., description="下一档加仓后的推导强平价")
    next_order_price: float = Field(..., description="下一档计划加仓触发价")
    liq_safety_buffer_pct: float = Field(..., description="距离强平价的安全缓冲百分比 (%)")
    required_extreme_margin: float = Field(..., description="承受 30% 极端逆势波动所需总隔离保证金 (USDT)")
    violation_reason: Optional[str] = Field(None, description="强平穿透拦截原因说明")


class KalmanPairMetrics(BaseModel):
    """卡尔曼滤波动态协整配对与对冲套利指标"""
    model_config = ConfigDict(populate_by_name=True)

    main_symbol: str = Field(..., description="配对主资产 (例如 BTC-USDT-SWAP)")
    sub_symbol: str = Field(..., description="配对从属资产 (例如 ETH-USDT-SWAP)")
    dynamic_beta: float = Field(..., description="卡尔曼滤波动态对冲斜率 Beta")
    dynamic_alpha: float = Field(..., description="卡尔曼滤波动态截距 Alpha")
    spread_z_score: float = Field(..., description="当前残差价差归一化 Z-Score")
    covariance_trace: float = Field(..., description="协方差矩阵迹 Tr(P)，监控协整健康度")
    is_divergent: bool = Field(..., description="是否协整破裂发散 (触发对冲熔断)")
    suggested_arbitrage_action: str = Field("NONE", description="配对套利建议动作: NONE / LONG_SUB_SHORT_MAIN / SHORT_SUB_LONG_MAIN / CLOSE")


class StrategyPlateauScore(BaseModel):
    """策略参数平原与生死线抗过拟合质量评分"""
    model_config = ConfigDict(populate_by_name=True)

    is_plateau_qualified: bool = Field(..., description="是否通过参数平原与生死线过滤")
    drop_top3_decay: float = Field(..., description="剔除 Top3 暴利单后的收益保留率 (要求 >= 0.50)")
    fee_to_gross_ratio: float = Field(..., description="手续费占毛利润比例 (要求 < 0.40)")
    friction_stress_pnl: float = Field(..., description="双边 40bps 滑点摩擦压力测试后的净期望 (USDT)")
    neighbor_stability_score: float = Field(..., description="邻域 +/-20% 参数平原稳定性得分 (0-100)")





