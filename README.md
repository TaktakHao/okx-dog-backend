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
   - 动态移动保本与跟踪止损（浮盈 $\ge 0.8\%$ 自动上移止损线至保本价）；
   - 传统金融美股/外汇交易时段过滤。
2. **OKX v5 长连接集群 (`okx_ws.py`)**:
   - 自动维护行情、订单簿深度、逐笔成交的 WebSocket 订阅与本地快照；
   - 毫秒级断线自动重连与订阅自愈。
3. **硬风控拦截中枢 (`risk_controller.py`)**:
   - 单笔订单金额上限、单日亏损熔断、杠杆上限、滑点保护、一键 Panic 撤单全平。
4. **模拟操盘沙盒引擎 (`paper_trading.py`)**:
   - 支持全真模拟撮合、动态盯市计算浮动盈亏（UPL）、自动止盈止损触发。
5. **REST API 网关 (`routers/`)**:
   - 多标的检索过滤 (`/api/v1/market/symbols`)、订单流转、持仓管理、AI 决策触发、SSE 流式接口。

## 快速启动与测试

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 运行全量核心测试 (9 组核心测试)
pytest test_backend_core.py -o asyncio_mode=auto

# 3. 启动 FastAPI 服务 (端口 8000)
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
