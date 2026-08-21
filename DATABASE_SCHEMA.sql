-- =============================================================================
-- OKX-Dog 个人量化智能交易系统 - 生产级 SQLite 数据库 DDL
-- 数据库类型: SQLite 3.35+ (支持 RETURNING, 严格外键检查, WAL 模式)
-- 字符编码: UTF-8
-- 模块: okx-dog-backend/DATABASE_SCHEMA.sql
-- 100% 严格对齐 AI_SCHEMA.json, models.py, API_SPEC.md 与 TYPES_CONTRACT.ts
-- =============================================================================

-- 开启外键约束支持与性能优化指令 (应用层连接初始化时自动执行)
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -64000; -- 64MB 缓存
PRAGMA temp_store = MEMORY;

-- -----------------------------------------------------------------------------
-- 1. 凭证加密密盒表 (credentials_vault)
-- 用于安全加密存储 OKX API Key、Secret、Passphrase 及权限掩码
-- 采用 AES-256-GCM 算法加密，绝不存储明文凭证
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS credentials_vault (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    credential_type TEXT NOT NULL DEFAULT 'OKX_V5' CHECK (credential_type IN ('OKX_V5', 'OPENAI', 'ANTIGRAVITY', 'CUSTOM_LLM')),
    is_live_mode INTEGER NOT NULL DEFAULT 0 CHECK (is_live_mode IN (0, 1)), -- 0: 模拟盘(Demo), 1: 实盘(Live)
    encrypted_api_key TEXT NOT NULL,                                       -- AES-256-GCM 加密后的 Base64 密文
    encrypted_secret_key TEXT NOT NULL,                                    -- AES-256-GCM 加密后的 Base64 密文
    encrypted_passphrase TEXT NOT NULL,                                    -- AES-256-GCM 加密后的 Base64 密文
    api_key_masked TEXT NOT NULL,                                          -- 脱敏字符串 (例如: "6b2a****8e91")
    salt TEXT NOT NULL,                                                    -- PBKDF2/Argon2 派生盐 (Base64)
    nonce TEXT NOT NULL,                                                   -- AES-GCM 96-bit 随机 IV/Nonce (Base64)
    tag TEXT NOT NULL,                                                     -- AES-GCM 128-bit 身份认证 Tag (Base64)
    permissions TEXT NOT NULL DEFAULT '["read", "trade"]',                 -- 校验出的 API 权限列表 (JSON 字符串，禁止包含 withdraw)
    has_withdraw_permission INTEGER NOT NULL DEFAULT 0 CHECK (has_withdraw_permission IN (0, 1)), -- 若为 1 则强制系统锁死
    last_tested_at TIMESTAMP NULL,                                         -- 上次连通性测试时间
    is_valid INTEGER NOT NULL DEFAULT 1 CHECK (is_valid IN (0, 1)),        -- 凭证是否有效
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_credentials_type_mode ON credentials_vault(credential_type, is_live_mode);
CREATE INDEX IF NOT EXISTS idx_credentials_valid ON credentials_vault(is_valid, is_live_mode);

-- -----------------------------------------------------------------------------
-- 2. 系统运行与人机协同模式配置表 (system_config)
-- 包含协同模式(手动/半自动/全自动)、当前交易对、LLM配置及全局开关
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS system_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_version INTEGER NOT NULL DEFAULT 1,
    copilot_mode TEXT NOT NULL DEFAULT 'MANUAL' CHECK (copilot_mode IN ('MANUAL', 'SEMI_AUTO', 'FULL_AUTO')),
    active_symbol TEXT NOT NULL DEFAULT 'BTC-USDT-SWAP',                    -- 当前监控与主交易标的
    supported_timeframes TEXT NOT NULL DEFAULT '["15m", "1h", "4h", "1d"]', -- 启用的多周期列表
    llm_provider TEXT NOT NULL DEFAULT 'antigravity' CHECK (llm_provider IN ('antigravity', 'deepseek', 'openai', 'anthropic', 'custom')),
    llm_base_url TEXT NOT NULL DEFAULT 'http://127.0.0.1:8001',
    llm_model TEXT NOT NULL DEFAULT 'gemini-3.7-flash',                   -- 默认模型 (支持 gemini-3.7-flash, deepseek-reasoner, gpt-4o 等)
    encrypted_llm_api_key TEXT NULL,                                       -- AES-256 加密的 LLM 密钥
    llm_api_key_masked TEXT NULL,                                          -- LLM 密钥脱敏显示
    llm_temperature REAL NOT NULL DEFAULT 0.2 CHECK (llm_temperature >= 0.0 AND llm_temperature <= 2.0),
    llm_max_tokens INTEGER NOT NULL DEFAULT 4096,
    is_trading_enabled INTEGER NOT NULL DEFAULT 1 CHECK (is_trading_enabled IN (0, 1)), -- 全局交易总开关
    is_ai_auto_trigger INTEGER NOT NULL DEFAULT 0 CHECK (is_ai_auto_trigger IN (0, 1)), -- 是否开启 AI 定时自动研判
    auto_trigger_interval_mins INTEGER NOT NULL DEFAULT 60,                -- 自动研判周期(分钟)
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_system_config_version ON system_config(config_version);

-- -----------------------------------------------------------------------------
-- 3. 硬风控规则与熔断状态表 (risk_limits)
-- 单笔限额、单日亏损熔断、杠杆上限、滑点保护硬拦截状态
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk_limits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    max_order_usdt REAL NOT NULL DEFAULT 500.0 CHECK (max_order_usdt > 0),       -- 单笔最大委托金额上限 (USDT)
    max_daily_loss_usdt REAL NOT NULL DEFAULT 200.0 CHECK (max_daily_loss_usdt > 0), -- 单日累计已实现+浮动最大亏损熔断阈值 (USDT)
    max_leverage INTEGER NOT NULL DEFAULT 5 CHECK (max_leverage >= 1 AND max_leverage <= 20), -- 强制锁死最高杠杆倍数
    max_slippage_pct REAL NOT NULL DEFAULT 0.5 CHECK (max_slippage_pct > 0.0 AND max_slippage_pct <= 5.0), -- 最大允许滑点百分比 (0.5%)
    max_open_positions INTEGER NOT NULL DEFAULT 3 CHECK (max_open_positions >= 1), -- 最大并发持仓数量
    max_position_usdt REAL NOT NULL DEFAULT 2000.0 CHECK (max_position_usdt > 0), -- 单标的最大持仓规模 (USDT)
    is_circuit_broken INTEGER NOT NULL DEFAULT 0 CHECK (is_circuit_broken IN (0, 1)), -- 是否处于熔断锁定状态
    circuit_break_reason TEXT NULL,                                        -- 熔断原因描述
    circuit_break_at TIMESTAMP NULL,                                       -- 熔断触发时间
    daily_loss_current REAL NOT NULL DEFAULT 0.0,                          -- 当日当前累计亏损 (USDT, 实时计算)
    daily_loss_reset_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,      -- 每日盈亏重置时间 (UTC 00:00)
    is_safe_mode INTEGER NOT NULL DEFAULT 0 CHECK (is_safe_mode IN (0, 1)), -- Panic Button 后进入的安全只读模式
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_risk_circuit_break ON risk_limits(is_circuit_broken, is_safe_mode);

-- -----------------------------------------------------------------------------
-- 4. 行情与多周期指标快照表 (market_snapshots)
-- 记录每一次行情感知、K线及技术指标(EMA/MACD/RSI/布林带/ATR/OI/资金费率)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,                                                  -- 交易标的 (如 BTC-USDT-SWAP)
    timestamp INTEGER NOT NULL,                                            -- 行情毫秒时间戳 (Unix ms)
    price REAL NOT NULL CHECK (price > 0),                                 -- 最新成交价
    high_24h REAL NOT NULL,                                                -- 24小时最高价
    low_24h REAL NOT NULL,                                                 -- 24小时最低价
    volume_24h REAL NOT NULL,                                              -- 24小时成交量
    funding_rate REAL NOT NULL DEFAULT 0.0,                                -- 当前资金费率 (例如 0.0001)
    next_funding_time INTEGER NULL,                                        -- 下次结算资金费率时间戳 (ms)
    open_interest REAL NOT NULL DEFAULT 0.0,                               -- 持仓量 (OI)
    open_interest_change_24h REAL NOT NULL DEFAULT 0.0,                    -- 24h OI 变化率
    indicators_json TEXT NOT NULL,                                         -- 各周期指标字典 JSON (EMA20/50/200, MACD, RSI, BB, ATR)
    depth_snapshot_json TEXT NULL,                                         -- 盘口前5档买卖盘快照 JSON
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_market_snapshots_symbol_ts ON market_snapshots(symbol, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_market_snapshots_created ON market_snapshots(created_at DESC);

-- -----------------------------------------------------------------------------
-- 5. AI 研判记录与决策日志表 (ai_analyses)
-- 严格对齐 AI_SCHEMA.json 生产基准 (包含 market_regime, timeframe_analysis, derivatives_sentiment, signal, trade_plan, risk_assessment)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_id TEXT NOT NULL UNIQUE,                                      -- 研判唯一业务追踪 ID (UUID v4)
    symbol TEXT NOT NULL,                                                  -- 研判标的
    timestamp INTEGER NOT NULL,                                            -- 研判生成毫秒时间戳 (ms)
    timeframe TEXT NOT NULL DEFAULT '1h',                                  -- 主研判周期
    trigger_type TEXT NOT NULL DEFAULT 'MANUAL' CHECK (trigger_type IN ('MANUAL', 'SCHEDULED', 'PRICE_ALERT', 'INDICATOR_CROSS')),
    market_snapshot_id INTEGER NULL,                                       -- 关联的行情快照 ID
    prompt_snapshot TEXT NOT NULL,                                         -- 发送给 LLM 的完整 Prompt 上下文
    thinking_process TEXT NULL,                                            -- 推理模型输出的 <think> 思维链内容
    market_regime TEXT NOT NULL CHECK (market_regime IN ('TRENDING_UP', 'TRENDING_DOWN', 'RANGING', 'VOLATILE_BREAKOUT')), -- 宏观盘面结构
    timeframe_analysis_json TEXT NOT NULL,                                 -- 多周期技术面共振 JSON (tf_15m, tf_1h, tf_4h, tf_1d)
    derivatives_sentiment_json TEXT NOT NULL,                              -- 衍生品情绪研判 JSON (funding_rate_bias, oi, sentiment_score)
    signal_action TEXT NOT NULL CHECK (signal_action IN ('BUY_LONG', 'SELL_SHORT', 'CLOSE_POSITION', 'HOLD_WAIT')), -- 交易信号动作
    signal_confidence REAL NOT NULL CHECK (signal_confidence >= 0.0 AND signal_confidence <= 1.0), -- 置信度评分 (0.0 - 1.0)
    signal_urgency TEXT NOT NULL DEFAULT 'MEDIUM' CHECK (signal_urgency IN ('LOW', 'MEDIUM', 'HIGH')), -- 紧迫度
    trade_plan_json TEXT NOT NULL,                                         -- 建议交易计划 JSON (entry_range, sl, tp_levels, rr, leverage, order_type)
    risk_assessment_json TEXT NOT NULL,                                    -- 风险评估 JSON (key_risks, invalidation_condition, max_holding_hours)
    reasoning_summary TEXT NOT NULL,                                       -- 精炼中文研判结论 (150字内)
    reasoning_details TEXT NOT NULL,                                       -- 详尽推导逻辑
    raw_llm_response TEXT NOT NULL,                                        -- LLM 原始响应内容
    llm_model_used TEXT NOT NULL,                                          -- 实际调用的模型名称
    latency_ms INTEGER NOT NULL DEFAULT 0,                                 -- 模型推理耗时 (毫秒)
    status TEXT NOT NULL DEFAULT 'SUCCESS' CHECK (status IN ('SUCCESS', 'FAILED', 'TIMEOUT', 'SCHEMA_ERROR')),
    error_message TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (market_snapshot_id) REFERENCES market_snapshots(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_ai_analyses_symbol ON ai_analyses(symbol, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ai_analyses_id ON ai_analyses(analysis_id);
CREATE INDEX IF NOT EXISTS idx_ai_analyses_status ON ai_analyses(status);

-- -----------------------------------------------------------------------------
-- 5.1 AI 研判决策分析日志扁平表 (ai_analysis_logs)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_analysis_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_uuid TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    market_regime TEXT NOT NULL,
    signal_action TEXT NOT NULL,
    confidence_score INTEGER NOT NULL,
    recommended_leverage INTEGER NOT NULL DEFAULT 3,
    entry_range_low REAL NOT NULL,
    entry_range_high REAL NOT NULL,
    stop_loss_price REAL NOT NULL,
    take_profit_1 REAL NOT NULL,
    risk_reward_ratio REAL NOT NULL,
    reasoning_summary TEXT NOT NULL,
    thinking_process TEXT NULL,
    model_used TEXT NOT NULL,
    latency_ms INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ai_analysis_logs_symbol ON ai_analysis_logs(symbol, id DESC);
CREATE INDEX IF NOT EXISTS idx_ai_analysis_logs_uuid ON ai_analysis_logs(analysis_uuid);


-- -----------------------------------------------------------------------------
-- 6. 委托订单表 (orders)
-- 记录所有挂单、市价单、附带止盈止损、风控审核结果与 OKX 状态同步
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT NOT NULL UNIQUE,                                  -- 客户端生成的唯一订单号 (如 "OKXD_20260815_xxxx")
    okx_order_id TEXT NULL,                                                -- OKX v5 返回的 ordId
    symbol TEXT NOT NULL,                                                  -- 标的代码 (如 BTC-USDT-SWAP)
    inst_type TEXT NOT NULL DEFAULT 'SWAP' CHECK (inst_type IN ('SPOT', 'SWAP', 'FUTURES', 'MARGIN')),
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),                    -- buy: 买入, sell: 卖出
    pos_side TEXT NOT NULL DEFAULT 'net' CHECK (pos_side IN ('long', 'short', 'net')), -- long: 开平多, short: 开平空, net: 单向持仓
    order_type TEXT NOT NULL DEFAULT 'limit' CHECK (order_type IN ('limit', 'market', 'trigger', 'post_only', 'fok', 'ioc', 'optimal_limit_ioc')),
    price REAL NULL,                                                       -- 委托价格 (市价单为 NULL)
    sz REAL NOT NULL CHECK (sz > 0),                                       -- 委托数量 (张数或币数)
    notional_usdt REAL NOT NULL DEFAULT 0.0 CHECK (notional_usdt >= 0),    -- 委托名义价值 (折合 USDT)
    filled_sz REAL NOT NULL DEFAULT 0.0 CHECK (filled_sz >= 0),            -- 已成交数量
    avg_price REAL NOT NULL DEFAULT 0.0,                                   -- 成交均价
    state TEXT NOT NULL DEFAULT 'live' CHECK (state IN ('live', 'partially_filled', 'filled', 'canceled', 'mmp_canceled', 'rejected', 'risk_rejected', 'expired', 'fail')),
    leverage INTEGER NOT NULL DEFAULT 1 CHECK (leverage >= 1),             -- 杠杆倍数
    -- 附带止盈止损定义 (OCO / TP/SL)
    tp_trigger_price REAL NULL,                                            -- 止盈触发价
    tp_ord_price REAL NULL,                                                -- 止盈委托价 (-1 表示市价)
    sl_trigger_price REAL NULL,                                            -- 止损触发价
    sl_ord_price REAL NULL,                                                -- 止损委托价 (-1 表示市价)
    fee REAL NOT NULL DEFAULT 0.0,                                         -- 累计手续费 (USDT)
    fee_currency TEXT NOT NULL DEFAULT 'USDT',                             -- 手续费币种
    pnl REAL NOT NULL DEFAULT 0.0,                                         -- 已实现盈亏 (USDT)
    -- 风控审核与来源标记
    risk_check_passed INTEGER NOT NULL DEFAULT 1 CHECK (risk_check_passed IN (0, 1)),
    risk_check_msg TEXT NULL,                                              -- 风控审核通过或拦截原因
    copilot_mode TEXT NOT NULL DEFAULT 'MANUAL' CHECK (copilot_mode IN ('MANUAL', 'SEMI_AUTO', 'FULL_AUTO')),
    ai_analysis_id INTEGER NULL,                                           -- 触发此订单的 AI 研判记录 ID
    is_panic_order INTEGER NOT NULL DEFAULT 0 CHECK (is_panic_order IN (0, 1)), -- 是否为 Panic Button 紧急全平单
    raw_okx_response TEXT NULL,                                            -- OKX 下单原始响应报文 (JSON)
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ai_analysis_id) REFERENCES ai_analyses(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_client_id ON orders(client_order_id);
CREATE INDEX IF NOT EXISTS idx_orders_okx_id ON orders(okx_order_id);
CREATE INDEX IF NOT EXISTS idx_orders_symbol_state ON orders(symbol, state, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_ai_analysis ON orders(ai_analysis_id);

-- -----------------------------------------------------------------------------
-- 7. 持仓快照与强平风险监控表 (positions)
-- 实时同步 OKX 当前持仓状态、未实现盈亏、保证金率及预估强平价
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pos_id TEXT NULL,                                                      -- 持仓业务 ID
    symbol TEXT NOT NULL,                                                  -- 标的代码 (如 BTC-USDT-SWAP)
    inst_type TEXT NOT NULL DEFAULT 'SWAP' CHECK (inst_type IN ('SPOT', 'SWAP', 'FUTURES', 'MARGIN')),
    pos_side TEXT NOT NULL CHECK (pos_side IN ('long', 'short', 'net')),   -- 持仓方向
    pos REAL NOT NULL DEFAULT 0.0,                                         -- 持仓数量 (正数)
    avail_pos REAL NOT NULL DEFAULT 0.0,                                   -- 可平仓数量
    avg_px REAL NOT NULL DEFAULT 0.0,                                      -- 开仓均价
    mark_px REAL NOT NULL DEFAULT 0.0,                                     -- 最新标记价格
    liq_px REAL NULL,                                                      -- 预估强平价格
    margin_ratio REAL NOT NULL DEFAULT 0.0,                                -- 保证金率 (例如 1.85 表示 185%)
    maint_margin_ratio REAL NOT NULL DEFAULT 0.01,                          -- 维持保证金率 (例如 0.01)
    leverage INTEGER NOT NULL DEFAULT 1,                                   -- 当前杠杆倍数
    notional_usd REAL NOT NULL DEFAULT 0.0,                                -- 名义持仓价值 (USDT)
    upl REAL NOT NULL DEFAULT 0.0,                                         -- 未实现盈亏 (USDT)
    upl_ratio REAL NOT NULL DEFAULT 0.0,                                   -- 未实现收益率 (例如 0.052 表示 +5.2%)
    realized_pnl REAL NOT NULL DEFAULT 0.0,                                -- 已实现盈亏 (USDT)
    margin REAL NOT NULL DEFAULT 0.0,                                      -- 占用保证金 (USDT)
    tp_trigger_price REAL NULL,                                            -- 当前设置的止盈触发价
    sl_trigger_price REAL NULL,                                            -- 当前设置的止损触发价
    is_danger_alert INTEGER NOT NULL DEFAULT 0 CHECK (is_danger_alert IN (0, 1)), -- 是否触发强平危险预警
    last_synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,           -- 与 OKX WebSocket 同步时间
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_positions_symbol_side ON positions(symbol, pos_side);
CREATE INDEX IF NOT EXISTS idx_positions_danger ON positions(is_danger_alert);

-- -----------------------------------------------------------------------------
-- 8. AI 信号盈亏比与胜率历史归因跟踪表 (attribution_logs)
-- 追踪 AI 推荐信号在后续时间窗口内的最大浮盈(MFE)、最大浮亏(MAE)、最终实现盈亏比与胜负归因
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS attribution_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ai_analysis_id INTEGER NOT NULL UNIQUE,                                -- 对应的 AI 研判 ID
    symbol TEXT NOT NULL,                                                  -- 标的代码
    signal_direction TEXT NOT NULL CHECK (signal_direction IN ('BUY_LONG', 'SELL_SHORT', 'CLOSE_POSITION', 'HOLD_WAIT')),
    signal_price REAL NOT NULL,                                            -- 信号发出时的基准价格
    target_1_price REAL NOT NULL,                                          -- 推荐第一止盈位
    target_2_price REAL NOT NULL DEFAULT 0.0,                              -- 推荐第二止盈位
    stop_loss_price REAL NOT NULL,                                         -- 推荐止损位
    is_adopted INTEGER NOT NULL DEFAULT 0 CHECK (is_adopted IN (0, 1)),     -- 用户或系统是否实际采纳开仓
    order_id INTEGER NULL,                                                 -- 若采纳，关联的订单 ID
    entry_time TIMESTAMP NOT NULL,                                         -- 信号生效/建仓时间
    exit_time TIMESTAMP NULL,                                              -- 平仓/评估结束时间
    max_favorable_excursion REAL NOT NULL DEFAULT 0.0,                     -- 最大有利偏移 MFE (USDT 或 价格点数)
    max_adverse_excursion REAL NOT NULL DEFAULT 0.0,                       -- 最大不利偏移 MAE (USDT 或 价格点数)
    hit_target_1 INTEGER NOT NULL DEFAULT 0 CHECK (hit_target_1 IN (0, 1)), -- 是否触达目标 1
    hit_target_2 INTEGER NOT NULL DEFAULT 0 CHECK (hit_target_2 IN (0, 1)), -- 是否触达目标 2
    hit_stop_loss INTEGER NOT NULL DEFAULT 0 CHECK (hit_stop_loss IN (0, 1)), -- 是否触达止损
    realized_pnl REAL NOT NULL DEFAULT 0.0,                                -- 实际或模拟实现盈亏 (USDT)
    realized_rr REAL NOT NULL DEFAULT 0.0,                                 -- 实际达成盈亏比 (R:R Ratio)
    is_win INTEGER NULL CHECK (is_win IN (0, 1)),                          -- 最终判定胜(1)负(0)
    outcome TEXT NULL CHECK (outcome IN ('WIN', 'LOSS', 'NEUTRAL', 'PENDING')), -- 归因结果枚举
    attribution_status TEXT NOT NULL DEFAULT 'TRACKING' CHECK (attribution_status IN ('TRACKING', 'COMPLETED', 'EXPIRED', 'INVALIDATED')),
    notes TEXT NULL,                                                       -- 归因分析评语 (用于Prompt微调反馈)
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ai_analysis_id) REFERENCES ai_analyses(id) ON DELETE CASCADE,
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_attribution_symbol ON attribution_logs(symbol, is_win);
CREATE INDEX IF NOT EXISTS idx_attribution_status ON attribution_logs(attribution_status);

-- -----------------------------------------------------------------------------
-- 9. 风控拦截审计与 Panic Button 触发日志表 (risk_audit_logs)
-- 记录每一次风控拦截详情、熔断触发、提现权限非法探测及紧急一键平仓事件
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk_audit_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'MAX_ORDER_EXCEEDED', 
        'DAILY_LOSS_LIMIT', 
        'LEVERAGE_EXCEEDED', 
        'SLIPPAGE_EXCEEDED', 
        'POSITION_LIMIT_EXCEEDED',
        'UNAUTHORIZED_WITHDRAWAL_ATTEMPT', 
        'PANIC_BUTTON_TRIGGERED', 
        'CIRCUIT_BREAK_TRIGGERED', 
        'CIRCUIT_BREAK_MANUAL_RESET',
        'ORDER_REJECTED',
        'DAILY_LOSS_TRIGGERED',
        'PANIC_TRIGGERED',
        'UNAUTHORIZED_WITHDRAWAL_DETECTED',
        'SLIPPAGE_PROTECTION'
    )),
    severity TEXT NOT NULL DEFAULT 'WARNING' CHECK (severity IN ('INFO', 'WARNING', 'WARN', 'CRITICAL', 'FATAL')),
    trigger_source TEXT NOT NULL DEFAULT 'RISK_ENGINE' CHECK (trigger_source IN ('RISK_ENGINE', 'USER_MANUAL', 'OKX_GATEWAY', 'AI_ENGINE')),
    rule_name TEXT NOT NULL DEFAULT 'RISK_RULE',                           -- 触发的风控规则名
    rule_limit_value TEXT NOT NULL DEFAULT '',                             -- 规则设定的阈值 (字符串格式)
    current_metric_value TEXT NOT NULL DEFAULT '',                         -- 触发时的实际指标值 (字符串格式)
    symbol TEXT NULL,                                                      -- 相关交易标的
    order_payload_json TEXT NULL,                                          -- 尝试下单的完整载荷快照
    is_blocked INTEGER NOT NULL DEFAULT 1 CHECK (is_blocked IN (0, 1)),    -- 是否已被后端硬阻断 (1: 是)
    action_taken TEXT NOT NULL DEFAULT 'BLOCKED',                          -- 后端采取的处置动作 (如 "REJECTED_ORDER", "LOCK_SYSTEM", "PANIC_CLOSE_ALL")
    details_json TEXT NULL,                                                -- 额外审计元数据 JSON
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_risk_audit_event ON risk_audit_logs(event_type, severity, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_risk_audit_created ON risk_audit_logs(created_at DESC);

-- -----------------------------------------------------------------------------
-- 10. 自动量化操盘会话与收益追踪表 (auto_pilot_sessions, snapshots & trades)
-- 精准记录从开启操盘以来的初始本金、收益曲线、胜率、盈亏比与自动成交流水
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS auto_pilot_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL UNIQUE,                                       -- 会话追踪 UUID
    is_live_mode INTEGER NOT NULL DEFAULT 0 CHECK (is_live_mode IN (0, 1)), -- 0: 模拟盘, 1: 实盘
    is_running INTEGER NOT NULL DEFAULT 1 CHECK (is_running IN (0, 1)),    -- 是否运行中
    start_time INTEGER NOT NULL,                                           -- 开启时间戳 (ms)
    stop_time INTEGER NULL,                                                -- 停止时间戳 (ms)
    start_equity REAL NOT NULL DEFAULT 10000.0,                            -- 开启时的基准账户净值 (USDT)
    current_equity REAL NOT NULL DEFAULT 10000.0,                          -- 最新净值 (USDT)
    realized_pnl REAL NOT NULL DEFAULT 0.0,                                -- 累计已实现盈亏 (USDT)
    total_trades INTEGER NOT NULL DEFAULT 0,                               -- 总成交单数
    win_trades INTEGER NOT NULL DEFAULT 0,                                 -- 胜单数
    loss_trades INTEGER NOT NULL DEFAULT 0,                                -- 负单数
    max_drawdown REAL NOT NULL DEFAULT 0.0,                                -- 最大回撤 (%)
    symbol TEXT NOT NULL DEFAULT 'BTC-USDT-SWAP',                          -- 操盘标的
    min_confidence REAL NOT NULL DEFAULT 0.70,                             -- 触发置信度
    auto_leverage INTEGER NOT NULL DEFAULT 1 CHECK (auto_leverage IN (0, 1)), -- 是否开启自适应杠杆
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_auto_pilot_session_id ON auto_pilot_sessions(session_id);
CREATE INDEX IF NOT EXISTS idx_auto_pilot_running ON auto_pilot_sessions(is_running, is_live_mode);

CREATE TABLE IF NOT EXISTS auto_pilot_pnl_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,                                              -- 关联的操盘会话 ID
    timestamp INTEGER NOT NULL,                                            -- 采样时间戳 (ms)
    equity REAL NOT NULL,                                                  -- 该时刻账户权益 (USDT)
    realized_pnl REAL NOT NULL DEFAULT 0.0,                                -- 该时刻已实现盈亏 (USDT)
    unrealized_pnl REAL NOT NULL DEFAULT 0.0,                              -- 该时刻持仓浮动盈亏 (USDT)
    total_pnl REAL NOT NULL DEFAULT 0.0,                                   -- 开启以来综合盈亏 (USDT)
    pnl_ratio REAL NOT NULL DEFAULT 0.0,                                   -- 开启以来收益率
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES auto_pilot_sessions(session_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_auto_pilot_snapshots_sess ON auto_pilot_pnl_snapshots(session_id, timestamp ASC);

CREATE TABLE IF NOT EXISTS auto_pilot_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,                                              -- 关联的操盘会话 ID
    order_id TEXT NULL,                                                    -- 关联的订单 ID
    symbol TEXT NOT NULL,                                                  -- 标的代码
    pos_side TEXT NOT NULL,                                                -- long/short
    entry_price REAL NOT NULL,                                             -- 开仓均价
    exit_price REAL NOT NULL,                                              -- 平仓价格
    size REAL NOT NULL,                                                    -- 币数
    leverage INTEGER NOT NULL DEFAULT 1,                                   -- 杠杆倍数
    realized_pnl REAL NOT NULL DEFAULT 0.0,                                -- 该笔实现盈亏 (USDT)
    roi_pct REAL NOT NULL DEFAULT 0.0,                                     -- 收益率 (%)
    reason TEXT NOT NULL DEFAULT 'AI策略自动平仓',                         -- 平仓原因
    timestamp INTEGER NOT NULL,                                            -- 平仓时间戳 (ms)
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES auto_pilot_sessions(session_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_auto_pilot_trades_sess ON auto_pilot_trades(session_id, timestamp DESC);

-- -----------------------------------------------------------------------------
-- 11. 历史 K 线与衍生品时序数据湖表 (historical_klines)
-- 支持多周期历史行情批量归档、断点续传与高精度回测检索
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS historical_klines (
    symbol TEXT NOT NULL,                                                  -- 标的代码 (如 BTC-USDT-SWAP)
    timeframe TEXT NOT NULL,                                               -- 周期 (15m, 1h, 4h, 1d)
    timestamp INTEGER NOT NULL,                                            -- 毫秒时间戳 (Unix ms)
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    close_price REAL NOT NULL,
    volume REAL NOT NULL,
    funding_rate REAL NOT NULL DEFAULT 0.0,
    open_interest REAL NOT NULL DEFAULT 0.0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, timeframe, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_hist_klines_search ON historical_klines (symbol, timeframe, timestamp DESC);

-- -----------------------------------------------------------------------------
-- 12. 全网宏观与行业热点资讯表 (news_events)
-- 汇总 CryptoPanic / RSS / 宏观日历，并记录 AI 结构化情感与时效评级
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS news_events (
    id TEXT PRIMARY KEY,                                                   -- 资讯唯一哈希 ID
    source TEXT NOT NULL,                                                  -- 来源: cryptopanic, rss, coindesk, manual
    title TEXT NOT NULL,                                                   -- 新闻标题
    content TEXT NULL,                                                     -- 正文/摘要
    url TEXT NULL,                                                         -- 原文链接
    published_at INTEGER NOT NULL,                                         -- 发布时间戳 (ms)
    sentiment_score REAL NOT NULL DEFAULT 0.0,                             -- 情感偏向: -1.0(极度利空) ~ +1.0(极度利好)
    urgency TEXT NOT NULL DEFAULT 'P2' CHECK (urgency IN ('P0', 'P1', 'P2')), -- P0: 黑天鹅/暴利好, P1: 重要, P2: 一般
    related_symbols TEXT NULL,                                             -- 关联标的 JSON: '["BTC", "ETH"]'
    is_fud_or_rumor INTEGER NOT NULL DEFAULT 0 CHECK (is_fud_or_rumor IN (0, 1)), -- 是否谣言/FUD
    summary_zh TEXT NULL,                                                  -- AI 中文一句话核心提炼
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_news_published ON news_events (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_urgency ON news_events (urgency, published_at DESC);

-- -----------------------------------------------------------------------------
-- 13. 每日盘后复盘报告表 (daily_reviews)
-- 记录每日 UTC 00:00 全自动生成的战报统计、逐笔解剖、得失归因与教训提炼
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_reviews (
    review_date TEXT PRIMARY KEY,                                          -- 复盘日期 (格式: 'YYYY-MM-DD')
    total_trades INTEGER NOT NULL DEFAULT 0,                               -- 当日总交易单数
    winning_trades INTEGER NOT NULL DEFAULT 0,                             -- 胜单数
    losing_trades INTEGER NOT NULL DEFAULT 0,                              -- 负单数
    win_rate REAL NOT NULL DEFAULT 0.0,                                    -- 当日胜率
    realized_pnl REAL NOT NULL DEFAULT 0.0,                                -- 当日累计已实现盈亏 (USDT)
    fee_paid REAL NOT NULL DEFAULT 0.0,                                    -- 当日手续费磨损 (USDT)
    max_drawdown REAL NOT NULL DEFAULT 0.0,                                -- 当日最大回撤 (%)
    best_trade_pnl REAL NOT NULL DEFAULT 0.0,                              -- 当日最佳单笔盈利 (USDT)
    worst_trade_pnl REAL NOT NULL DEFAULT 0.0,                             -- 当日最差单笔亏损 (USDT)
    summary_markdown TEXT NOT NULL,                                        -- AI 自动生成的 Markdown 深度复盘报告
    lessons_learned TEXT NULL,                                             -- 结构化教训 JSON: '["教训1", "教训2"]'
    strategy_adjustments TEXT NULL,                                        -- 策略参数权重自适应调整建议 JSON
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_daily_reviews_date ON daily_reviews (review_date DESC);

-- -----------------------------------------------------------------------------
-- 14. 交易员经验与避坑规则知识库 (trading_knowledge_base)
-- 沉淀长期交易智慧与反向避坑规则，用于在 AI 研判时反向注入 (Negative Few-Shot)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trading_knowledge_base (
    id TEXT PRIMARY KEY,                                                   -- 规则 UUID
    rule_type TEXT NOT NULL CHECK (rule_type IN ('MISTAKE_AVOIDANCE', 'BEST_PRACTICE', 'MACRO_REGIME')),
    symbol_scope TEXT NOT NULL DEFAULT '*',                                -- 适用标的，'*' 为全品类
    trigger_condition TEXT NOT NULL,                                       -- 触发场景/前置条件
    action_rule TEXT NOT NULL,                                             -- 约束规则/避坑准则
    confidence_score REAL NOT NULL DEFAULT 1.0,                            -- 规则置信度 (0.0~1.0)
    source_review_date TEXT NULL,                                          -- 来源于哪一天的复盘
    times_triggered INTEGER NOT NULL DEFAULT 0,                            -- 历史命中拦截次数
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),      -- 是否激活
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_kb_rule_active ON trading_knowledge_base (rule_type, is_active);
CREATE INDEX IF NOT EXISTS idx_kb_symbol ON trading_knowledge_base (symbol_scope, is_active);

-- -----------------------------------------------------------------------------
-- 15. 本地策略回测实验记录表 (backtest_experiments)
-- 记录每一次向量化回测实验的参数、收益率、夏普比率与压力测试结果
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS backtest_experiments (
    id TEXT PRIMARY KEY,                                                   -- 实验 UUID
    strategy_name TEXT NOT NULL,                                           -- 策略名称
    symbol TEXT NOT NULL,                                                  -- 标的代码
    timeframe TEXT NOT NULL,                                               -- 回测周期
    start_time INTEGER NOT NULL,                                           -- 回测起始时间戳 (ms)
    end_time INTEGER NOT NULL,                                             -- 回测结束时间戳 (ms)
    parameters TEXT NOT NULL,                                              -- 回测入参 JSON
    total_return_pct REAL NOT NULL,                                        -- 总收益率 (%)
    sharpe_ratio REAL NOT NULL,                                            -- 夏普比率
    max_drawdown_pct REAL NOT NULL,                                        -- 最大回撤 (%)
    win_rate REAL NOT NULL,                                                -- 胜率
    profit_factor REAL NOT NULL,                                           -- 盈亏比 / Profit Factor
    total_trades INTEGER NOT NULL,                                         -- 回测交易总笔数
    metrics_detail TEXT NOT NULL,                                          -- 详尽多维统计指标 JSON
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_backtest_strategy ON backtest_experiments (strategy_name, symbol, created_at DESC);


