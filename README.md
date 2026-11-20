# OKX-Dog Backend - 量化执行、硬风控与同构回测服务端

OKX-Dog Backend 是基于 FastAPI 与异步协程构建的量化交易与投研中枢，负责直连 OKX v5 REST/WebSocket 网关、执行不可绕过的硬风控拦截、运行 7x24h 脱机操盘与对账守护、管理多年时序数据湖及驱动同构 Walk-Forward 回测。

---

## 技术栈

- **服务框架**：FastAPI + Uvicorn（异步非阻塞事件循环）
- **持久化存储**：SQLite 3（WAL 模式）+ DuckDB / 内存环形缓存自适应数据湖
- **交易所网关**：OKX v5 REST 签名客户端（限频退避）+ WebSocket 公有/业务/私有长连接池
- **安全加密**：AES-256-GCM 凭证密盒、PBKDF2 密钥派生与全链路日志脱敏

---

## 核心模块说明

| 模块路径 | 核心职责 |
| :--- | :--- |
| `config.py` | 全局运行配置、默认 Top 5 核心标的池（`BTC/ETH/SOL/BNB/DOGE`）、永久禁开仓合成合约清单与安全开关（`AUTO_BOOT_LIVE_MODE`、`USE_LIVE_PUBLIC_MARKET_DATA`、`AUTO_TAKEOVER_EXTERNAL_POSITIONS`、`EVOLUTION_AUTO_MUTATE_ENABLED`） |
| `okx_rest.py` | OKX v5 REST 网关：公共行情与深历史接口（`history-candles`、`funding-rate-history` 分页回溯）、订单执行与强制止损校验（附带止损失败直接拒单禁止裸单）、零裸仓安全改损协议（`replace_position_stop_loss_safely` 先挂后撤） |
| `okx_ws.py` | OKX v5 WebSocket 长连接中枢：自动心跳、指数退避重连与订阅自愈；模拟盘公共行情与实时资金费率默认直连 OKX 实盘公共流 |
| `risk_controller.py` | 金融级硬风控中枢：单笔上限、杠杆上限、滑点保护、单日亏损硬熔断（默认 `300 USDT`，SQLite 持久化并按 UTC 00:00 重置）、高水位线回撤自适应控仓、DCA 强平穿透拦截与 Cancel-Before-Close Panic Button |
| `auto_pilot.py` | 7x24h 脱机常驻操盘引擎：默认挂载双引擎冻结参数影子盘驱动，支持全局 18% 回撤停机底线、外部手动仓位隔离跳过与会话状态持久化 |
| `services/dual_engine_driver.py` | 冻结参数双引擎影子盘/实盘统一驱动器：100% 复用 `KeltnerTrendStrategy` (4H) 与 `DualEMAChandelierStrategy` (1D) 同构策略类，严格按 K 线收盘门控执行、维护宽幅 ATR 吊灯止损，并联动引擎 A 每 30 分钟巡检费率套利与休眠状态 |
| `paper_trading.py` | 高精度模拟/影子盘撮合引擎：接入实盘公共行情、双边 Maker/Taker 手续费、市价滑点惩罚、每 8h 真实资金费率结算与真实跳空撮合，单笔净盈亏与现金流变动严格闭环 |
| `core/strategy_base.py` | 三位一体策略统一抽象基类（`BaseStrategy`、`StrategyContext`、`STRATEGY_REGISTRY`），内置主引擎 B 双周期策略 `KeltnerTrendStrategy`（4H 凯尔特纳通道 + 宽幅 ATR 吊灯止损）与 `DualEMAChandelierStrategy`（1D 极简双均线 + ATR 吊灯止损），以及 `DonchianTrendStrategy`、`EMABreakoutStrategy` |
| `services/data_lake_service.py` | 本地多维时序数据湖：支持永续与现货多年历史 K 线批量同步、`bisect` 精准对齐历史 8h 资金费率与高速查询 |
| `services/backtest_engine.py` | 高保真事件驱动回测引擎：Next-Bar 零前瞻撮合、真实资金费率结算、日频年化夏普、多标的组合回测、滚动样本外 Walk-Forward 检验与参数平原门禁，结果持久化至 `backtest_experiments` |
| `services/reconciliation_service.py` | 60 秒常驻自动化对账中枢：以 OKX 远端为唯一事实源，默认隔离外部手动持仓仅告警不接管，同步真实亏损触发风控保护 |
| `services/execution_engine.py` | Smart Pegging 被动做市追单引擎与原子化双腿防跛腿执行器（`execute_atomic_dual_leg`：现货+永续 Delta 中性协同挂单、跛腿自动回滚与跨币种保证金率健康守护） |
| `services/funding_carry_service.py` | 底座引擎 A（期现资金费率套利）：72h 滚动真实费率与基差机会扫描、熊市自动休眠切 USDT 活期（滞回防换手）、`SEMI_AUTO` Telegram 半自动提案确认 / `FULL_AUTO` 切换及 Gate 2B 验真 |
| `services/portfolio_allocator.py` | 60/30/10 双引擎资金槽位分配器：硬隔离主引擎 B（30% 4H 凯尔特纳 + 30% 1D 双均线）、底座引擎 A（30% 期现套利）与 10% USDT 安全缓冲，联动单日 3% 熔断与全局 18% 回撤停机 |
| `services/position_sizer.py` | ATR 波动率自适应固定风险头寸模型：单笔止损风险严格锁定总权益 1%，自动对齐 OKX 合约面值与最小下单张数 |
| `services/telegram_bot.py` | Telegram 移动端推送网关：开平仓通知、挂损失败/外部仓位漂移/熔断触发紧急告警与每日复盘简报 |
| `vault.py` | AES-256-GCM 本地密盒管理与提现权限物理阻断校验 |
| `routers/` | RESTful API 路由群（行情、交易、风控、模拟盘、自动操盘、回测/数据湖、资金流雷达、运维健康、密盒、Telegram 等）；解除熔断接口强制校验 `MASTER_PASSWORD` |

---

## 快速启动与验证

```bash
# 安装依赖
pip install -r requirements.txt

# 启动 FastAPI 服务 (默认锁定模拟/影子盘启动，不自动切入实盘)
uvicorn main:app --host 127.0.0.1 --port 8000 --reload

# 运行核心单元测试
pytest test_backend_core.py -o asyncio_mode=auto
```
