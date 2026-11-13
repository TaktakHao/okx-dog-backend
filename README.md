# OKX-Dog Backend - 高并发交易与硬风控拦截中枢后端

OKX-Dog 后端是基于 Python 异步架构构建的高并发量化交易与 7x24h 脱机操盘中枢，负责直连 OKX v5 交易网关、执行多重硬风控拦截、驱动 7x24h 脱机自主巡检引擎、动态移动保本与模拟操盘撮合。

## 技术栈

- **Web 框架**: FastAPI + Uvicorn (ASGI 高并发异步事件循环)
- **常驻守护**: 异步常驻协程 (`auto_pilot_worker_loop`) 7x24h 脱机自主盯盘与下单
- **持久化**: SQLite 3 (WAL 模式优化) + 事务索引
- **通信网关**: OKX v5 WebSocket 长连接断线自愈网关 + REST 签名客户端
- **安全加固**: AES-256-GCM 凭证密盒（内存解密，明文物理隔离，严禁提现权限）
- **风控体系**: 5 维度硬风控拦截层 (Hard Risk Lock)

## 核心服务与模块

1. **7x24h 脱机常驻自主操盘引擎 (`auto_pilot.py` / `core/background_tasks.py`)**:
   - 后台协程每 15 秒自主轮巡多标的池（TradFi 黄金/美股代币/RWA/主流加密）；
   - 最大并发持仓组合敞口控制 (`max_concurrent_positions`)；
    - **两阶段阶梯止盈体系 (Two-Stage Partial Scale-Out TP)**：
      - **阶段 1 (确定性落袋与超额保本)**：当持仓最高浮盈达到激活门槛（默认 `trailing_profit_threshold = 0.012` 或 ATR 波动率自适应门槛）且从最高点回撤达比例（默认 25%）或 AI 超买信号触发时，执行 **减半平仓 50% 锁定确定性胜率**；同时执行 **Cancel-Before-Close** 清空该标的挂单，并将剩余 50% 仓位止损线强制上移至超额保本价（`entry_px * (1 ± 0.0012)`，覆盖双边手续费与滑点磨损），实现真正的「绝对零风险长尾博取」；
      - **阶段 2 (宽幅移动吊灯博取长尾 Fat-Tail Alpha)**：剩余 50% 仓位清除固定硬止盈，放宽回调容忍度至 40%（移动吊灯止盈），直到触及策略终极波段目标 TP2 或回撤超 40% 或超额保本线触发时终极出清，最大化捕捉单边大趋势利润；
    - **ATR 波动率自适应跟踪门槛 (Volatility-Adaptive TP)**：
      - 结合 15m 周期 ATR 动态计算动态门槛 `dynamic_tp_threshold = clamp(1.6 * (ATR_15m / Price), 1.0%, 3.5%)`；高波动剧烈行情自动放宽止盈空间防止早退，低波动收敛震荡收窄门槛防利润回吐；
    - **OKX 资金费率结算时点防收割保护 (Funding Fee Rollover Guard)**：
      - 针对 OKX 永续合约 UTC 00:00 / 08:00 / 16:00 资金费结算周期，在结算前 15 分钟（如 UTC 07:45-07:59）监测到极端高费率时，自动拦截顺费率开仓，避免开单即遭扣费收割；
    - **POST_ONLY Maker 被动挂单深度优化与防踏空**：
      - 首仓与加仓默认启用被动做市挂单（POST_ONLY），买单挂在买一档微观下方、卖单挂在卖一档微观上方，享受负费率/极低手续费（节省 60% 手续费成本）；
      - 当 AI 或量化研判置信度达超高等级（`confidence >= 0.85`）时，自动从 Maker 挂单升级为市价单（MARKET）快速上车，防止单边爆破行情因 Maker 挂单逆向选择踏空；
      - 在所有平仓、减半、反手操作前，强制执行 `cancel_all_orders` 清空挂单，杜绝残留订单被撮合变成“幽灵仓位”；
    - **持仓感知主动止盈离场 (Position-Aware AI Close)**：
      - 量化研判结合当前持仓收益与多周期动量，多单浮盈 $\ge 0.6\%$ 且 15m RSI $\ge 70$ 或触及布林上轨动能衰减时，输出 `CLOSE_POSITION` 提前获利了结落袋为安；空单同理；
    - **冷静期重入场轮动 (Reentry Cooldown & Adaptive Re-entry)**：
      - 平仓后记录毫秒时间戳并自动进入冷静期（默认 `reentry_cooldown_seconds = 90` 秒），杜绝在同一点位假突破追高；
      - 冷静期结束后，若多周期指标顺势看好，自动以最优价格重新发起新一轮建仓开单，实现“盈利主动离场，继续看好重新开单”的良性轮动；
    - **超额移动保本与金字塔加仓**：
      - 浮盈 $\ge 0.8\%$ 自动上移止损线至超额保本均价（`entry_px * (1 ± 0.0012)`，覆盖双边手续费与滑点磨损）；
      - 顺势加仓前校验无见顶冲高回落（回撤 $< 0.5\%$）；加仓止盈点位锚定波段扩展止盈位，不反向压缩既有利好止盈目标。
2. **OKX v5 长连接集群 (`okx_ws.py`)**:
   - 自动维护行情、订单簿深度、逐笔成交的 WebSocket 订阅与本地快照；
   - 毫秒级断线自动重连与订阅自愈。
3. **硬风控拦截中枢 (`risk_controller.py`)**:
   - 单笔订单金额上限、单日亏损熔断、杠杆上限、滑点保护、一键 Panic 撤单全平。
4. **模拟操盘沙盒引擎 (`paper_trading.py`)**:
   - 支持全真模拟撮合、动态盯市计算浮动盈亏（UPL）、自动止盈止损触发。
5. **REST API 网关 (`routers/`)**:
   - 多标的检索过滤 (`/api/v1/market/symbols`)、订单流转、持仓管理、AI 决策触发、SSE 流式接口。
6. **智能体自演化、AI 员工看板与微调数据中心网关 (`routers/evolution.py`)**:
   - 获取 6 位 AI 员工档案、专属规则与动态话语权 (`/api/v1/evolution/team`)；
   - 触发大模型量化总监自驱反思问诊与战法进化 (`POST /api/v1/evolution/diagnose`)；
   - 获取最新大模型反思诊断长文报告 (`GET /api/v1/evolution/diagnosis-report`)；
   - 历史演进版本快照查询 (`/api/v1/evolution/history`)；
   - 一键回滚出厂黄金基准 (`/api/v1/evolution/rollback`) 与恢复出厂初始设置 (`/api/v1/evolution/reset`)；
   - 微调数据集沉淀统计 (`/api/v1/evolution/dataset/stats`)；
   - 前端一键生成与导出 Colab 训练集 (`/api/v1/evolution/dataset/export`)；
   - 数据集安全下载通道 (`/api/v1/evolution/dataset/download/{filename}`)。
7. **资深财务分析师与资金效率服务 (`services/finance_service.py` / `routers/finance.py`)**:
   - 映射专家角色: `agency-financial-analyst`；
   - 对外接口: `GET /api/v1/finance/health`；
   - 核心量化指标: 资本利用率 (Capital Utilization)、年化夏普比率 (Sharpe Ratio)、索提诺比率 (Sortino Ratio)、卡玛比率 (Calmar Ratio)；
   - 费用摩擦与损益归因 (PnL Attribution): 深度统计累计毛利 (Gross PnL)、累计手续费支出与手续费磨损比率 (`Fee Drag Ratio = Fees / Gross Profit`)，拒绝当交易所手续费打工仔；
   - 资深财务分析师全自动诊断报告与资金优化建议。
8. **FP&A 滚动风险预算与高水位线回撤自适应降额 (`risk_controller.py`)**:
   - 映射专家角色: `agency-fp-a-analyst`；
   - 高水位线追踪 (High Water Mark, HWM) 与当前净值回撤深度计算；
   - 阶梯式回撤自适应头寸模型 (`Drawdown-Adaptive Risk Sizing`)：
     - 回撤 $< 3\%$: 保持基准 $1.0\%$ 满额风险预算；
     - 回撤 $3\% \sim 6\%$: 降额至 $0.70\%$（轻度防守）；
     - 回撤 $6\% \sim 10\%$: 降额至 $0.40\%$（中度锁定）；
     - 回撤 $\ge 10\%$: 急冻至 $0.20\%$（极限资本保护），消除爆仓与大幅连环回撤隐患。
9. **资深投资研究员资产尽调与体制动态优选排序 (`auto_pilot.py`)**:
   - 映射专家角色: `agency-investment-researcher`；
   - 标的深度尽调评分模型 (`rank_investment_targets_by_research`)：结合市场体制（多头顺势动量、空头顺势破位、震荡窄幅整理）与资金费率对未持仓标的动态打分；
   - 颠覆死板的固定顺序遍历，优先为综合尽调评分 Top 标的分配并发开仓名额。
10. **自动化实时对账中枢 (`reconciliation_service.py` / `routers/health.py`)**:
   - 60 秒常驻后台异步轮巡，以远端 OKX 真实交易所账本为唯一事实源 (Remote SSOT)；
   - 自动监测外部平仓、强制清算或非系统发起的手工订单，秒级消除本地状态漂移与幽灵持仓；
   - 对外提供立即对账指令端点 `POST /api/v1/health/reconcile-now` 与对账健康度查询。
11. **Smart Pegging 动态追单与被动做市执行引擎 (`execution_engine.py`)**:
   - 毫秒级贴合盘口买一/卖一档发出 `POST_ONLY` 被动挂单，最高享受交易所 Maker 手续费优惠（节约 60%+ 交易成本）；
   - 盘口价差自适应与微观深度检验；
   - 智能追单看门狗：盘口价格漂移超过阈值 (0.15%) 或挂单超时时，自动并发撤单并重新贴盘，严禁裸露追单；
   - 紧急行情（置信度 $\ge 0.85$）或市价平仓时平滑回退，保障极端行情下的即时成交能力。
12. **ATR 波动率自适应固定风险头寸管理 (`position_sizer.py`)**:
   - 严格遵循 Fixed Fractional Risk 金融级资金模型，单笔交易风险额度严格锁定在总资金的 1.0%；
   - 基于 Wilder's RMA 算法精确计算 ATR(14) 动态波动率；
   - 极端行情防插针惩罚收缩因子：当短期波动超过基准 2.5 倍时，自动收缩 50% 仓位敞口；
   - 自动对齐 OKX 真实合约面值、最小张数与步长，统一投研、回测与实盘的一致性。
13. **高保真事件驱动回测引擎升级 (`services/backtest_engine.py`)**:
   - 修正名义本金（$\text{Notional} = \text{Margin} \times \text{Leverage}$）与双向 Taker 手续费（杜绝 10 倍偏差虚假繁荣）；
   - UTC 00:00 / 08:00 / 16:00 真实资金费率 8 小时动态结算与沉淀；
   - 严格消除前瞻未来函数（Look-ahead Bias）：Bar $i$ 产生意图并在 Bar $i+1$ Open 撮合成交；
   - 单根 Bar 内部最高价/最低价根据阳线/阴线子路径严格模拟。
14. **“三位一体”策略基类统一架构 (`core/strategy_base.py`)**:
   - 抽象标准策略接口 `BaseStrategy`，提供 `on_bar`, `generate_signals`, `calculate_indicators`；
   - 统一实盘上下文 `StrategyContext` 与回测上下文 `BacktestContext`；
   - 策略核心逻辑一次编写，实盘与回测 100% 同构复用，彻底终结“回测一套逻辑、实盘重新实现”的双轨制隐患。
15. **时序数据湖引擎与真实行情扩展 (`services/data_lake_service.py`)**:
   - DuckDB 列式引擎自适应兼容模式 + 零依赖高性能内存环形缓存 (RingBuffer)；
   - 支持毫秒级 Parquet/列式数据落盘与海量 K 线点位分析；
   - 行情同步自动并发拉取 OKX 真实历史资金费率 (Funding Rate) 与全网持仓量 (Open Interest)。
16. **企业级量化运维监控网关 (`routers/health.py`)**:
   - 提供全景运维指标接口 `GET /api/v1/health/ops-metrics`；
   - 实时采集 OKX WebSocket 毫秒级延时、REST API 往返时延 (RTT)、Maker 成交比例、对账状态及数据湖健康度。
17. **7x24h 自动操盘状态机与动态运行时长修正 (`auto_pilot.py`)**:
   - 修复未启动/重载状态下计时器累计虚假运行时长的缺陷，保证未启停状态 `running_seconds = 0`；
   - 增强全生命周期状态一致性，与前端一键启动 API 强联锁，杜绝前端空转阻断。
18. **DCA 动态强平线穿透预判与极限承压保证金精算 (`core/dca_liquidation_calculator.py` / `risk_controller.py`)**:
   - 融合自 `oke_auto_trade` (`calculate_dca.py` 与 `calculate_multi_group_margin.py`) 实战数理优势；
   - 逐层前瞻推导 DCA 加仓链条的持仓均价、累计保证金、以及下一档推导强平价格；
   - **强平穿透硬拦截 (Liquidation Penetration Check)**：做多加仓委托价 $\le$ 上一轮持仓强平价，或做空加仓委托价 $\ge$ 上一轮持仓强平价时，判定价格未跌至补仓点即已先行穿仓，在风控拦截器与自动驾驶加仓分支中一票否决；
   - **30% 极端逆势极限承压保证金精算**：推导在承受极端暴跌/暴涨时所需锁定的最低隔离保证金总额。
19. **两状态自适应卡尔曼滤波协整配对与对冲套利中枢 (`core/kalman_pair_engine.py`)**:
   - 融合自 `oke_auto_trade` (`run_pair_backest.py` & `run_pair.py`)；
   - 递归动态追踪标的对（如 BTC-USDT-SWAP vs ETH-USDT-SWAP）对数价格的协整斜率 $\beta$ 与截距 $\alpha$；
   - 滚动计算残差价差动态归一化 $Z\text{-Score}$，结合协方差矩阵迹 $\text{Tr}(P)$ 发散监控，生成低风险市场中性对冲建议。

20. **Paperclip 虚拟量化公司与 CFO 战绩配额网关 (`routers/paperclip.py` & `auto_pilot.py`)**:
    - 对外暴露 `/api/v1/paperclip/org`（公司组织架构）、`/api/v1/paperclip/quota/overview`（员工剩余可用奖励额度）；
    - 异动任务派发端点 `/api/v1/paperclip/task/anomaly`，与 Paperclip 控制平面实现 Webhook 双向交互；
    - 自动操盘平仓盈利或排雷避险成功时，自动向 CFO 战绩激励引擎推送事件，实现算力奖励与深度思考特权发放闭环。

21. **欧易全量真实标的检索与一键订阅引擎 (`services/instrument_service.py` & `routers/market.py`) (v7.0)**:
    - **全量真实在架标的库**：启动时自动异步同步 OKX 官方 `/api/v5/public/instruments` 全量 SWAP (永续) 与 SPOT (现货) 交易对，支持本地磁盘文件快速持久化与 6 小时自动静默刷新；
    - **高性能二级索引与模糊搜索**：暴露 `GET /api/v1/market/instruments/search`，支持关键词前缀、币种名、全称多维检索，耗时 < 5ms；
    - **长连接动态订阅**：端点 `POST /api/v1/market/subscribe-focus` 支持批量向 WebSocket 建立实时推流，杜绝用户手工输入错误代码。

22. **7x24h 自动操盘多标的巡检容错与实时价格同步修复 (`services/ai_service.py` / `auto_pilot.py` / `services/market_service.py`)**:
    - 修复 `SinglePeriodIndicators` 的 `macd_hist` / `macd_histogram` 属性兼容，消除巡检循环中的 `AttributeError` 静默跳过问题；
    - 在多标的自动巡检（`MULTI_SYMBOL`）的持仓盯市与新标的首仓扫描阶段，自动对非主图订阅标的执行 OKX 实时 Ticker / 15m 最新收盘价按需刷新，杜绝回退到静态默认价格导致信号失真。

23. **模拟盘状态无缝持久化、防假插针止损保护与自驱进化决策闭环 (`paper_trading.py` / `auto_pilot.py` / `services/ai_service.py`)**:
    - **模拟盘与自动操盘状态跨重启无缝续跑 (`paper_runtime_state.json` & `_restore_from_sqlite_sync`)**：自动持久化并恢复模拟盘可用现金、活跃持仓、历史成交流水及 `auto_pilot_sessions` 收益曲线，服务重启不重置净值；
    - **测试网单 Tick 断层滑点保护与真实保本校验**：在 `paper_trading.py` 中对单 Tick 跳空超过 $2.5\%$ 的异常插针实施止损滑点上限保护，并严格校验 `is_breakeven_locked` 与加仓后新均价的数学关系；
    - **达尔文标的隔离、盘口点差硬拦截与顺势金字塔加仓修正**：接入 `AgentEvolutionManager.get_active_strategy_parameters()`，自动屏蔽低流动性合成合约与盘口点差 $>0.25\%$ 的毒性标的，禁用浮亏死扛补仓 (`PULLBACK_DCA`)，统一由 `PositionSizer` 控制顺势金字塔加仓头寸并修正加仓止损方向与反转冷静期。

## 快速启动与测试

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 运行全量核心测试 (10 组核心测试)
pytest test_backend_core.py -o asyncio_mode=auto

# 3. 启动 FastAPI 服务 (端口 8000)
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
