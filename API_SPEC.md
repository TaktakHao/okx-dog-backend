# OKX-Dog 接口契约与通信协议规范说明书 (API & Protocol Specification)

| 版本 | 编写日期 | 状态 | 规范对象 |
| :--- | :--- | :--- | :--- |
| **v1.0.0** | 2026-08-15 | 生产就绪 (契约完全统合) | 后端 FastAPI / 前端 React / AI Engine |

---

## 1. 协议规范概述

### 1.1 通信协议分类
OKX-Dog 采用 **RESTful API + Server-Sent Events (SSE) + WebSocket** 混合通信架构：
1. **RESTful API (`/api/v1/*`)**：用于控制指令、配置管理、同步查询、订单提交与风控重置。
2. **Server-Sent Events (`/api/v1/ai/analyze/stream`)**：用于 AI 研判生成过程中实时流式输出思维链 (`<think>`) 与结构化 JSON。
3. **WebSocket (`/ws/v1/stream`)**：用于全双工毫秒级推送公共行情（Ticker、深度盘口、多周期指标）与私有状态（持仓、订单成交、硬风控熔断事件）。

### 1.2 统一响应外壳 (Generic REST Envelope)
所有 REST API（除二进制或 SSE 流式响应外）均采用统一的 JSON 包装：
```json
{
  "code": 0,
  "msg": "success",
  "data": { ... },
  "timestamp": 1755216000000
}
```
* **`code`**：业务状态码。`0` 表示成功；非 `0` 表示业务异常，详见第 4 节《系统错误码字典》。
* **`msg`**：操作结果提示或错误原因描述（中文）。
* **`data`**：业务载荷数据。查询无数据时返回 `null` 或空列表 `[]`。
* **`timestamp`**：服务器响应时的毫秒时间戳 (Unix ms, UTC)。

---

## 2. RESTful API 路由详细规范

### 2.1 安全密盒与凭证管理 (Vault & Security)

#### 1. 保存/更新 OKX 凭证
* **接口路径**：`POST /api/v1/vault/credentials`
* **接口说明**：接收前端提交的 API Key、Secret、Passphrase，在服务端使用交易员主密码（PBKDF2 派生密钥）经 AES-256-GCM 本地加密存储。
* **请求体** (`SaveCredentialsRequest`)：
```json
{
  "master_password": "MyStrongMasterPassword123!",
  "is_live_mode": false,
  "api_key": "6b2a8e91-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "secret_key": "9A8B7C6D5E4F...",
  "passphrase": "MyOkxPassphrase"
}
```
* **响应体** (`CredentialsStatusResponse`)：
```json
{
  "code": 0,
  "msg": "凭证已成功安全加密存储",
  "data": {
    "has_configured_okx": true,
    "is_live_mode": false,
    "api_key_masked": "6b2a****8e91",
    "permissions": ["read", "trade"],
    "has_withdraw_permission": false,
    "is_valid": true,
    "last_tested_at": 1755216000000
  },
  "timestamp": 1755216000000
}
```

#### 2. 查询当前凭证状态 (脱敏)
* **接口路径**：`GET /api/v1/vault/credentials/status`
* **响应体** (`CredentialsStatusResponse`)：返回已脱敏的当前凭证信息与权限状态。

#### 3. 测试 OKX 凭证连通性与权限探测
* **接口路径**：`POST /api/v1/vault/test-connection`
* **请求体** (`TestConnectionRequest`)：
```json
{
  "master_password": "MyStrongMasterPassword123!",
  "is_live_mode": false
}
```
* **响应体** (`TestConnectionResponse`)：
```json
{
  "code": 0,
  "msg": "OKX API 连通性测试通过",
  "data": {
    "connected": true,
    "latency_ms": 48,
    "permissions": ["read", "trade"],
    "security_warning": null
  },
  "timestamp": 1755216000000
}
```

#### 4. 配置/更新大模型参数 (LLM Config)
* **接口路径**：`POST /api/v1/vault/llm-config`
* **请求体** (`LLMConfigRequest`)：
```json
{
  "master_password": "MyStrongMasterPassword123!",
  "llm_provider": "deepseek",
  "llm_base_url": "https://api.deepseek.com",
  "llm_api_key": "sk-xxxxxxxxxxxxxxxxxxxxxxxx",
  "llm_model": "deepseek-reasoner",
  "llm_temperature": 0.2,
  "llm_max_tokens": 4096
}
```
* **响应体** (`LLMConfigResponse`)：
```json
{
  "code": 0,
  "msg": "大模型参数配置已更新",
  "data": {
    "llm_provider": "deepseek",
    "llm_base_url": "https://api.deepseek.com",
    "llm_model": "deepseek-reasoner",
    "llm_api_key_masked": "sk-****abcd",
    "llm_temperature": 0.2,
    "llm_max_tokens": 4096
  },
  "timestamp": 1755216000000
}
```

---

### 2.2 行情与多周期指标 (Market Data & Indicators)

#### 1. 获取最新行情 Ticker
* **接口路径**：`GET /api/v1/market/ticker?symbol=BTC-USDT-SWAP`
* **响应体** (`TickerData`)：
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "symbol": "BTC-USDT-SWAP",
    "last_price": 95240.5,
    "last_sz": 1.2,
    "high_24h": 96800.0,
    "low_24h": 93500.0,
    "open_24h": 94100.0,
    "change_24h_pct": 1.21,
    "volume_24h": 128450.0,
    "vol_currency_24h": 1223358000.0,
    "timestamp": 1755216000000
  },
  "timestamp": 1755216000000
}
```

#### 2. 获取历史 K 线数据
* **接口路径**：`GET /api/v1/market/kline?symbol=BTC-USDT-SWAP&timeframe=1h&limit=100`
* **响应体** (`List[KlineCandle]`)：
```json
{
  "code": 0,
  "msg": "success",
  "data": [
    {
      "timestamp": 1755212400000,
      "open": 94800.0,
      "high": 95400.0,
      "low": 94750.0,
      "close": 95240.5,
      "volume": 3200.5,
      "vol_currency": 304800000.0,
      "is_closed": true
    }
  ],
  "timestamp": 1755216000000
}
```

#### 3. 获取订单簿深度盘口 (Level-2)
* **接口路径**：`GET /api/v1/market/depth?symbol=BTC-USDT-SWAP&depth=20`
* **响应体** (`OrderBookDepth`)：
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "symbol": "BTC-USDT-SWAP",
    "timestamp": 1755216000000,
    "bids": [
      { "price": 95240.0, "size": 15.2, "order_count": 4 },
      { "price": 95239.5, "size": 22.0, "order_count": 8 }
    ],
    "asks": [
      { "price": 95240.5, "size": 8.5, "order_count": 2 },
      { "price": 95241.0, "size": 18.0, "order_count": 5 }
    ]
  },
  "timestamp": 1755216000000
}
```

#### 4. 获取衍生品专项指标 (资金费率 / OI)
* **接口路径**：`GET /api/v1/market/derivatives?symbol=BTC-USDT-SWAP`
* **响应体** (`DerivativesMetrics`)：
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "symbol": "BTC-USDT-SWAP",
    "funding_rate": 0.0001,
    "funding_rate_annualized_pct": 10.95,
    "next_funding_time": 1755244800000,
    "open_interest": 850000.0,
    "open_interest_usdt": 80954425000.0,
    "oi_change_24h_pct": 3.42,
    "long_short_ratio": 1.25,
    "top_trader_long_short_ratio": 1.45,
    "sentiment_score": 0.75
  },
  "timestamp": 1755216000000
}
```

#### 5. 获取多周期量化技术指标 (EMA / MACD / RSI / BB / ATR)
* **接口路径**：`GET /api/v1/market/indicators?symbol=BTC-USDT-SWAP`
* **响应体** (`MultiPeriodIndicators`)：
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "symbol": "BTC-USDT-SWAP",
    "timestamp": 1755216000000,
    "indicators": {
      "15m": {
        "timeframe": "15m",
        "ema_20": 95100.0,
        "ema_50": 94850.0,
        "ema_200": 94200.0,
        "macd_dif": 45.2,
        "macd_dea": 38.1,
        "macd_hist": 7.1,
        "rsi_14": 58.4,
        "bb_upper": 95600.0,
        "bb_middle": 95100.0,
        "bb_lower": 94600.0,
        "atr_14": 185.0
      },
      "1h": {
        "timeframe": "1h",
        "ema_20": 94900.0,
        "ema_50": 94300.0,
        "ema_200": 92500.0,
        "macd_dif": 120.5,
        "macd_dea": 98.0,
        "macd_hist": 22.5,
        "rsi_14": 62.1,
        "bb_upper": 96200.0,
        "bb_middle": 94900.0,
        "bb_lower": 93600.0,
        "atr_14": 420.0
      }
    }
  },
  "timestamp": 1755216000000
}
```

---

### 2.3 AI 智能研判与决策中心 (LLM Decision Engine)
*严格对齐 `okx-dog-ai/AI_SCHEMA.json`*

#### 1. 手动触发 AI 结构化研判 (同步)
* **接口路径**：`POST /api/v1/ai/analyze`
* **请求体** (`TriggerAIAnalysisRequest`)：
```json
{
  "symbol": "BTC-USDT-SWAP",
  "timeframe": "1h",
  "include_thinking": true
}
```
* **响应体** (`AIAnalysisResponse` - 100% 对齐 AI_SCHEMA.json 生产基准)：
```json
{
  "code": 0,
  "msg": "AI 研判完成",
  "data": {
    "analysis_id": "c1f7a8b2-3e4d-4a11-8f92-5b9e7c102a01",
    "symbol": "BTC-USDT-SWAP",
    "timestamp": 1755216000000,
    "market_regime": "TRENDING_UP",
    "timeframe_analysis": {
      "tf_15m": {
        "trend": "BULLISH",
        "key_indicators_summary": "EMA20>50>200发散多头排列；RSI=58.2健康运行；布林通道向上倾斜，价格稳定在中轨上方。",
        "support_level": 94380.0,
        "resistance_level": 95200.0
      },
      "tf_1h": {
        "trend": "BULLISH",
        "key_indicators_summary": "EMA20上穿EMA50金叉，MACD零轴上方红柱再次放量，RSI=63.5动能强劲。",
        "support_level": 94200.0,
        "resistance_level": 96500.0
      },
      "tf_4h": {
        "trend": "BULLISH",
        "key_indicators_summary": "成功放量突破94200平台阻力并缩量回踩确认，均线组呈标准多头排列，上升结构完好。",
        "support_level": 94200.0,
        "resistance_level": 97000.0
      },
      "tf_1d": {
        "trend": "BULLISH",
        "key_indicators_summary": "日线级别处于大上升通道主升浪，EMA200位于84000提供坚实宏观底部支撑。",
        "support_level": 91500.0,
        "resistance_level": 98500.0
      }
    },
    "derivatives_sentiment": {
      "funding_rate_bias": "MODERATE_POSITIVE",
      "open_interest_interpretation": "价格破位上行伴随全网持仓量(OI)稳步增长6.8%，体现主力资金主动建仓做多意愿强烈，行情非虚假轧空。",
      "long_short_ratio_state": "多空人数比为1.12，大户持仓比1.25，多头持仓结构健康，无散户极端狂热追多情绪。",
      "sentiment_score": 0.75
    },
    "signal": {
      "action": "BUY_LONG",
      "confidence": 0.88,
      "urgency": "MEDIUM"
    },
    "trade_plan": {
      "entry_range": [94500.0, 94700.0],
      "take_profit_levels": [
        {
          "price": 96800.0,
          "percentage": 0.5,
          "description": "TP1: 触及前期高点强阻力区平仓50%锁定利润，并将剩余仓位止损上移至开仓价保本。"
        },
        {
          "price": 98500.0,
          "percentage": 0.5,
          "description": "TP2: 日线波段斐波那契1.618扩展目标位，全平离场。"
        }
      ],
      "stop_loss_price": 93800.0,
      "risk_reward_ratio": 2.75,
      "suggested_leverage": 5,
      "order_type": "LIMIT"
    },
    "risk_assessment": {
      "key_risks": [
        "美股开盘时段可能的宏观流动性扰动",
        "若94200回踩被放量下穿则演变为假突破陷阱"
      ],
      "invalidation_condition": "若1小时K线实体收盘跌破93800支撑位，则突破多头逻辑彻底失效，应立即止损离场。",
      "max_holding_time_hours": 24.0
    },
    "reasoning_summary": "4H突破关键阻力位并回踩确认，1H金叉共振，OI增仓且费率健康，建议在94500-94700分批做多，盈亏比2.75。",
    "reasoning_details": "日线与4小时周期大趋势向上，4H放量突破94200箱体上沿后出现缩量整理，构成经典突破回踩确认形态。1小时级别EMA均线呈多头排列，MACD零轴上方再次金叉放量，RSI=63.5动能充足。衍生品端OI上升6.8%验证增量资金入场，资金费率处于0.008%健康区间。综合建议在94500-94700挂限价多单，止损设于破位结构下方的93800，第一止盈看96800，理论盈亏比高达2.75。",
    "thinking_process": "<think>首先分析BTC多周期形态，1h与4h保持均线发散，资金费率0.0001未见过热...</think>",
    "latency_ms": 1420
  },
  "timestamp": 1755216000000
}
```

#### 2. 实时流式触发 AI 研判 (Server-Sent Events)
* **接口路径**：`GET /api/v1/ai/analyze/stream?symbol=BTC-USDT-SWAP&timeframe=1h`
* **响应类型**：`text/event-stream`
* **流式事件序列**：
```
event: thinking
data: {"chunk": "正在分析多周期 EMA 与 MACD 指标分布...\n"}

event: thinking
data: {"chunk": "当前资金费率为 0.0001，多头持仓并未过度拥挤，4h 级别支撑位于 94500。\n"}

event: complete
data: {"analysis_id": "c1f7a8b2-3e4d-4a11-8f92-5b9e7c102a01", "symbol": "BTC-USDT-SWAP", "market_regime": "TRENDING_UP", ...}
```

#### 3. 查询历史 AI 研判与胜率归因
* **接口路径**：`GET /api/v1/ai/history?symbol=BTC-USDT-SWAP&page=1&page_size=20`
* **响应体** (`PaginatedData[AttributionLogModel]`)：返回包含最大浮盈 (MFE)、最大浮亏 (MAE)、实际盈亏比与胜负归因的历史研判记录。

---

### 2.4 交易执行与持仓管理 (Trade & Positions)

#### 1. 提交委托下单 (经硬风控中枢拦截)
* **接口路径**：`POST /api/v1/trade/orders`
* **请求体** (`PlaceOrderRequest` / `OrderCreateRequest`)：
```json
{
  "symbol": "BTC-USDT-SWAP",
  "inst_type": "SWAP",
  "side": "buy",
  "pos_side": "net",
  "order_type": "limit",
  "price": 94800.0,
  "size": 1.0,
  "leverage": 3,
  "tp_trigger_price": 96500.0,
  "tp_order_price": -1,
  "sl_trigger_price": 93800.0,
  "sl_order_price": -1,
  "copilot_mode": "MANUAL",
  "ai_analysis_id": "c1f7a8b2-3e4d-4a11-8f92-5b9e7c102a01"
}
```
* **响应体** (`OrderModel` / `OrderItem`)：
```json
{
  "code": 0,
  "msg": "订单委托已提交 OKX",
  "data": {
    "id": 1052,
    "client_order_id": "OKXD_20260815_99812",
    "okx_order_id": "189283719283719",
    "symbol": "BTC-USDT-SWAP",
    "inst_type": "SWAP",
    "side": "buy",
    "pos_side": "net",
    "order_type": "limit",
    "price": 94800.0,
    "size": 1.0,
    "notional_usdt": 94.8,
    "filled_size": 0.0,
    "avg_price": 0.0,
    "state": "live",
    "leverage": 3,
    "tp_trigger_price": 96500.0,
    "sl_trigger_price": 93800.0,
    "fee": 0.0,
    "fee_currency": "USDT",
    "pnl": 0.0,
    "risk_check_passed": true,
    "risk_check_msg": "通过全部硬风控检查",
    "copilot_mode": "MANUAL",
    "ai_analysis_id": "c1f7a8b2-3e4d-4a11-8f92-5b9e7c102a01",
    "created_at": 1755216000000,
    "updated_at": 1755216000000
  },
  "timestamp": 1755216000000
}
```

#### 2. 一键采纳 AI 交易计划下单
* **接口路径**：`POST /api/v1/trade/orders/adopt-ai-plan`
* **请求体** (`AdoptAIPlanRequest` / `AIAdoptPayload`)：
```json
{
  "symbol": "BTC-USDT-SWAP",
  "action": "BUY_LONG",
  "analysis_id": "c1f7a8b2-3e4d-4a11-8f92-5b9e7c102a01",
  "entry_price": 94650.0,
  "stop_loss_price": 93800.0,
  "take_profit_target_1": 96800.0,
  "order_type": "limit",
  "suggested_leverage": 5,
  "notional_usdt": 500.0
}
```

#### 3. 撤销指定订单
* **接口路径**：`POST /api/v1/trade/orders/cancel`
* **请求体** (`CancelOrderRequest`)：
```json
{
  "symbol": "BTC-USDT-SWAP",
  "client_order_id": "OKXD_20260815_99812",
  "okx_order_id": "189283719283719"
}
```

#### 4. 批量撤单
* **接口路径**：`POST /api/v1/trade/orders/batch-cancel`
* **请求体** (`BatchCancelOrdersRequest`)：
```json
{
  "symbol": "BTC-USDT-SWAP",
  "order_ids": ["189283719283719", "189283719283720"]
}
```

#### 5. 查询当前持仓列表
* **接口路径**：`GET /api/v1/trade/positions`
* **响应体** (`List[PositionModel]` / `List[PositionItem]`)：
```json
{
  "code": 0,
  "msg": "success",
  "data": [
    {
      "id": 1,
      "pos_id": "POS_BTC_001",
      "symbol": "BTC-USDT-SWAP",
      "inst_type": "SWAP",
      "pos_side": "net",
      "pos_size": 2.0,
      "avail_pos": 2.0,
      "avg_price": 94500.0,
      "mark_price": 95240.5,
      "liquidation_price": 63200.0,
      "mgn_ratio": 2.45,
      "maint_margin_ratio": 0.01,
      "leverage": 3,
      "notional_usd": 190481.0,
      "upl": 14.81,
      "upl_ratio": 0.0784,
      "realized_pnl": 0.0,
      "margin": 63.0,
      "tp_trigger_price": 96500.0,
      "sl_trigger_price": 93800.0,
      "is_danger_alert": false,
      "last_synced_at": 1755216000000
    }
  ],
  "timestamp": 1755216000000
}
```

#### 6. 为现有持仓设置/修改止盈止损
* **接口路径**：`POST /api/v1/trade/positions/tpsl`
* **请求体** (`PositionTPSLRequest`)：
```json
{
  "symbol": "BTC-USDT-SWAP",
  "pos_side": "net",
  "tp_trigger_price": 96800.0,
  "sl_trigger_price": 94000.0
}
```

#### 7. 快捷平仓 (市价/限价)
* **接口路径**：`POST /api/v1/trade/positions/close`
* **请求体** (`ClosePositionRequest`)：
```json
{
  "symbol": "BTC-USDT-SWAP",
  "pos_side": "net",
  "order_type": "market",
  "price": null,
  "size": null
}
```

#### 8. 获取账户资产全景
* **接口路径**：`GET /api/v1/trade/account/balance`
* **响应体** (`AccountBalanceModel` / `AccountBalance`)：
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "total_equity_usdt": 12500.8,
    "available_margin_usdt": 10420.3,
    "used_margin_usdt": 2080.5,
    "frozen_balance_usdt": 0.0,
    "unrealized_pnl_usdt": 154.2,
    "today_realized_pnl_usdt": 68.5,
    "today_pnl_ratio": 0.0055,
    "margin_ratio": 5.82,
    "is_warning_margin": false,
    "timestamp": 1755216000000
  },
  "timestamp": 1755216000000
}
```

---

### 2.5 硬风控中枢与 Panic Button (Risk & Emergency)

#### 1. 查询风控中枢实时状态
* **接口路径**：`GET /api/v1/risk/status`
* **响应体** (`RiskStatusResponse`)：
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "risk_status": "NORMAL",
    "is_circuit_broken": false,
    "circuit_break_reason": null,
    "circuit_break_at": null,
    "daily_loss_current": 42.5,
    "max_daily_loss_usdt": 200.0,
    "daily_loss_ratio": 0.2125,
    "is_safe_mode": false,
    "rules": {
      "max_order_usdt": 500.0,
      "max_daily_loss_usdt": 200.0,
      "max_leverage": 5,
      "max_slippage_pct": 0.5,
      "max_open_positions": 3,
      "max_position_usdt": 2000.0
    }
  },
  "timestamp": 1755216000000
}
```

#### 2. 更新硬风控规则参数
* **接口路径**：`PUT /api/v1/risk/config`
* **请求体** (`RiskRulesConfig` / `HardRiskRules`)：
```json
{
  "max_order_usdt": 500.0,
  "max_daily_loss_usdt": 200.0,
  "max_leverage": 5,
  "max_slippage_pct": 0.5,
  "max_open_positions": 3,
  "max_position_usdt": 2000.0
}
```

#### 3. 手动解除熔断锁定
* **接口路径**：`POST /api/v1/risk/reset-circuit-breaker`
* **请求体** (`ResetCircuitBreakerRequest`)：
```json
{
  "master_password": "MyStrongMasterPassword123!",
  "reason": "已核对行情剧烈波动结束，确认恢复正常交易权限"
}
```

#### 4. Panic Button 紧急全平与撤单 (最高优先级熔断)
* **接口路径**：`POST /api/v1/risk/panic-button`
* **请求体** (`PanicButtonRequest`)：
```json
{
  "master_password": "MyStrongMasterPassword123!",
  "confirm_phrase": "PANIC_CLOSE_ALL",
  "target": "ALL",
  "cancel_all_orders": true,
  "close_all_positions": true,
  "reason": "PANIC_BUTTON_TRIGGERED"
}
```
* **响应体** (`PanicButtonResponse`)：
```json
{
  "code": 0,
  "msg": "紧急处置指令已执行完成",
  "data": {
    "success": true,
    "canceled_orders_count": 4,
    "closed_positions_count": 2,
    "failed_operations": [],
    "execution_details": [
      {
        "symbol": "BTC-USDT-SWAP",
        "action": "CANCEL_ORDER",
        "target_id": "OKXD_20260815_99812",
        "status": "SUCCESS",
        "message": "已撤单"
      },
      {
        "symbol": "BTC-USDT-SWAP",
        "action": "CLOSE_POSITION",
        "target_id": "POS_BTC_001",
        "status": "SUCCESS",
        "message": "市价全平成功"
      }
    ],
    "system_state": "LOCKED_SAFE_MODE",
    "circuit_broken_at": 1755216000000,
    "message": "已成功撤销 4 笔挂单，市价全平 2 个持仓，系统已锁死进入只读安全模式"
  },
  "timestamp": 1755216000000
}
```

#### 5. 查询风控拦截与审计日志
* **接口路径**：`GET /api/v1/risk/audit-logs?page=1&page_size=20`
* **响应体** (`PaginatedData[RiskAuditLogModel]` / `ApiPaginatedResponse[RiskAuditLogItem]`)

---

## 3. WebSocket 实时通信协议规范

### 3.1 握手与连接
* **连接端点**：`ws://127.0.0.1:8000/ws/v1/stream` (支持 WSS)
* **鉴权方式**：连接后首条消息发送 `auth`，或在 URL query 中携带 `?token=jwt_token`。

### 3.2 心跳保活机制
* **周期**：客户端每 15 秒发送一次 `ping` 包；服务端在 1 秒内响应 `pong` 包。
* **超时断开**：若连续 30 秒未收到任何数据包，连接自动断开并触发客户端指数退避重连。
* **Ping 包示例**：
```json
{
  "action": "ping",
  "timestamp": 1755216000000
}
```
* **Pong 响应示例**：
```json
{
  "event": "pong",
  "timestamp": 1755216000010
}
```

### 3.3 频道订阅与取消订阅
* **标准频道订阅消息 (`WSClientMessage`)**：
```json
{
  "action": "subscribe",
  "channels": [
    "ticker:BTC-USDT-SWAP",
    "depth20:BTC-USDT-SWAP",
    "kline:BTC-USDT-SWAP:15m",
    "positions",
    "orders",
    "risk_events",
    "ai_signals"
  ]
}
```
* **OKX 兼容格式订阅消息 (`WSClientMessage`)**：
```json
{
  "op": "subscribe",
  "args": [
    { "channel": "tickers", "symbol": "BTC-USDT-SWAP" },
    { "channel": "positions", "symbol": "BTC-USDT-SWAP" }
  ]
}
```
* **服务端订阅确认响应**：
```json
{
  "channel": "ticker:BTC-USDT-SWAP",
  "event": "subscribed",
  "timestamp": 1755216000000
}
```

### 3.4 订阅频道定义与推送载荷格式

| 频道名称 (Channel) | 作用与说明 | 推送事件类型 | 载荷数据模型 (Data Model) |
| :--- | :--- | :--- | :--- |
| `ticker:{symbol}` | 实时最新价、24h 高低成交量推送 | `update` | `TickerData` |
| `depth20:{symbol}` | 买卖前20档深度盘口实时变动 | `snapshot` / `update` | `OrderBookDepth` |
| `kline:{symbol}:{interval}` | K线 Bar 实时更新与收线确认 | `update` | `KlineCandle` |
| `positions` | 交易员所有持仓变化、未实现盈亏与强平风险 | `snapshot` / `update` | `List[PositionModel]` |
| `orders` | 订单生命周期变动 (成交/撤单/风控拒单) | `update` | `OrderModel` |
| `risk_events` | 触发风控拦截、熔断或 Panic Button 广播 | `update` | `RiskAuditLogModel` |
| `ai_signals` | 定时或自动 AI 研判生成的交易建议 | `update` | `AIAnalysisResponse` |

* **标准推送数据包结构 (`WSPushPayload[T]` / `WSServerMessage[T]`)**：
```json
{
  "channel": "ticker:BTC-USDT-SWAP",
  "event": "update",
  "data": {
    "symbol": "BTC-USDT-SWAP",
    "last_price": 95240.5,
    "last_sz": 1.2,
    "high_24h": 96800.0,
    "low_24h": 93500.0,
    "open_24h": 94100.0,
    "change_24h_pct": 1.21,
    "volume_24h": 128450.0,
### 2.11 AI 量化策略实验室与 Codex Harness 沙箱 (Quant Studio)

#### 1. 获取经典量化策略模板库
* **接口路径**：`GET /api/v1/studio/templates`
* **接口说明**：获取系统预置的高质量、通过 AST 安全审查的经典量化因子与策略模板（布林带突破、RSI+资金费率动量、EMA+持仓量OI金叉）。
* **响应体**：
```json
{
  "code": 0,
  "msg": "获取模板库成功",
  "data": [
    {
      "id": "bollinger_breakout",
      "name": "自适应布林带通道突破与挤压策略",
      "description": "计算动态布林带上下轨，突破上轨且带宽高位扩张时开多，跌破下轨开空",
      "code": "import numpy as np\n..."
    }
  ]
}
```

#### 2. AI 自然语言生成策略与自愈回测闭环
* **接口路径**：`POST /api/v1/studio/generate-strategy`
* **接口说明**：接收交易员自然语言需求，驱动 Codex 生成 Python 策略，进行 AST 安全审计与受限沙箱回测，报错自动自愈重试（上限3轮）。
* **请求体** (`GenerateStrategyReq`)：
```json
{
  "prompt": "基于 15m RSI 超卖且资金费率转正时开多",
  "symbol": "BTC-USDT-SWAP",
  "timeframe": "15m",
  "max_retries": 3
}
```
* **响应体**：
```json
{
  "code": 0,
  "msg": "策略生成与沙盒回测执行完成",
  "data": {
    "strategy_id": "uuid-v4",
    "strategy_name": "AI-Quant-BTC-USDT-SWAP-15m-1",
    "code": "import numpy as np\n...",
    "ast_passed": true,
    "status": "VERIFIED",
    "metrics": {
      "total_return_pct": 12.5,
      "annualized_return_pct": 50.0,
      "max_drawdown_pct": 4.2,
      "sharpe_ratio": 1.85,
      "calmar_ratio": 2.97,
      "win_rate_pct": 62.5,
      "profit_factor": 1.9,
      "total_trades": 35,
      "total_bars": 1000
    },
    "equity_curve": [{"timestamp": 1700000000000, "equity": 1.0, "price": 65000.0}],
    "trade_signals": [{"timestamp": 1700000900000, "action": "BUY", "price": 65120.0}]
  }
}
```

#### 3. 自定义 Python 策略代码 AST 审计与沙箱回测
* **接口路径**：`POST /api/v1/studio/backtest-custom`
* **接口说明**：对交易员编写或修改的 Python 源码进行 AST 白名单静态扫描，通过后在独立进程沙箱中运行并返回回测指标。
* **请求体** (`CustomBacktestReq`)：
```json
{
  "code": "import numpy as np\n...",
  "symbol": "BTC-USDT-SWAP",
  "timeframe": "15m"
}
```

#### 4. 黄金测试集模型防退化评测
* **接口路径**：`POST /api/v1/studio/benchmark-eval`
* **接口说明**：针对 500+ 极端行情典型用例批量压测 Prompt 与底层模型的生成合规率与回测评分。

---

## 3. WebSocket 实时双向通信协议规范


| 错误码 (Code) | 错误枚举 (Error Identifier) | 错误原因与含义 | HTTP 状态码 | 处置建议与前端交互 |
| :--- | :--- | :--- | :--- | :--- |
| **0** | `SUCCESS` | 请求成功执行 | 200 | 正常渲染业务数据 |
| **1001** | `INVALID_PARAMETER` | 请求参数格式或范围不合法 | 400 | 表单高亮错误字段并提示用户 |
| **1002** | `SERVICE_UNAVAILABLE` | 系统内部服务暂时不可用 | 503 | 提示“服务准备中，请稍后重试” |
| **1003** | `RATE_LIMIT_EXCEEDED` | 触发本地或上游接口限频 | 429 | 提示“操作过于频繁，自动冷却中” |
| **1004** | `INTERNAL_SERVER_ERROR` | 后端服务未捕获的内部异常 | 500 | 弹出全局错误提示，记录错误追踪栈 |
| **2001** | `AUTH_TOKEN_EXPIRED` | 登录鉴权 Token 已过期 | 401 | 弹出重新登录浮窗 |
| **2002** | `INVALID_MASTER_PASSWORD` | 主密码错误，无法解密密盒凭证 | 403 | 提示“主密码不正确，请重新输入” |
| **2003** | `CREDENTIALS_NOT_FOUND` | 尚未配置 OKX 凭证 | 400 | 引导用户前往凭证配置页面 |
| **2004** | `WITHDRAW_PERMISSION_DETECTED` | **严重安全拦截**：检测到提现权限 | 403 | **强阻断**，弹窗警告并拒绝启动实盘 |
| **2005** | `OKX_AUTH_FAILED` | OKX API Key/Secret 签名认证失败 | 400 | 提示“OKX凭证无效，请检查Key与密码” |
| **3001** | `RISK_MAX_ORDER_EXCEEDED` | 单笔下单名义金额超出硬限额 | 400 | 拦截下单，提示“超出单笔最大限额” |
| **3002** | `RISK_DAILY_LOSS_TRIGGERED` | 触发单日最大亏损硬熔断 | 403 | 锁定交易面板，提示“触发单日止损熔断” |
| **3003** | `RISK_MAX_LEVERAGE_EXCEEDED` | 杠杆倍数超出系统硬锁上限 | 400 | 拦截下单，提示“杠杆倍数超过最高限制” |
| **3004** | `RISK_MAX_SLIPPAGE_EXCEEDED` | 市价单预估滑点超出保护阈值 | 400 | 拦截市价单，保护资金免遭滑点磨损 |
| **3005** | `RISK_POSITION_LIMIT_EXCEEDED` | 达到最大持仓标的数或单币种规模 | 400 | 拦截开仓，提示“持仓规模已达上限” |
| **3006** | `RISK_SYSTEM_IN_SAFE_MODE` | 系统处于 Panic Button 安全只读模式 | 403 | 拒绝任何开仓操作，仅允许平仓与查询 |
| **3007** | `RISK_TRADING_DISABLED` | 全局交易总开关已关闭 | 403 | 提示“交易总开关已关闭” |
| **4001** | `OKX_INSUFFICIENT_BALANCE` | 账户可用保证金不足 | 400 | 提示“账户可用保证金不足，无法开仓” |
| **4002** | `OKX_ORDER_NOT_FOUND` | 目标订单不存在或已被撤销 | 404 | 刷新订单列表 |
| **4003** | `OKX_POSITION_NOT_FOUND` | 平仓目标持仓不存在 | 404 | 刷新持仓列表 |
| **4004** | `OKX_GATEWAY_TIMEOUT` | 请求 OKX 服务器响应超时 | 504 | 提示网络抖动，启动幂等状态校对 |
| **5001** | `LLM_API_KEY_INVALID` | 大模型 API Key 无效或额度耗尽 | 400 | 提示“大模型 API 密钥异常，请检查配置” |
| **5002** | `LLM_TIMEOUT` | 大模型推理生成超时 (>30s) | 504 | 提示“AI 推理超时，建议稍后重试” |
| **5003** | `LLM_SCHEMA_PARSE_ERROR` | 大模型输出未符合 JSON Schema 契约 | 502 | 自动触发一次重试，失败则降级提示 |
| **5004** | `LLM_CONTEXT_TOO_LONG` | 行情提示词长度超出模型 Token 上限 | 400 | 自动裁剪历史 K 线周期数据并重新组装 |
