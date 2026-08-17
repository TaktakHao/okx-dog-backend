# OKX-Dog Backend - 高并发交易与硬风控拦截中枢后端

OKX-Dog 后端是基于 Python 异步架构构建的高并发量化交易中枢，负责直连 OKX v5 交易网关、执行多重硬风控拦截、驱动模拟操盘撮合引擎及聚合行情推流。

## 技术栈

- **Web 框架**: FastAPI + Uvicorn (ASGI 高并发异步事件循环)
- **持久化**: SQLite 3 (WAL 模式优化) + SQLAlchemy 2.0 异步驱动
- **通信网关**: OKX v5 WebSocket 长连接断线自愈网关 + REST 客户端
- **安全加固**: AES-256-GCM 凭证密盒（内存解密，明文物理隔离）
- **风控体系**: 5 维度硬风控拦截层 (Hard Risk Lock)

## 核心服务

1. **OKX v5 长连接集群 (`okx_ws.py`)**:
   - 自动维护行情、订单簿深度、逐笔成交的 WebSocket 订阅与本地快照
   - 毫秒级断线自动重连与订阅自愈
2. **硬风控拦截中枢 (`risk_controller.py`)**:
   - 单笔订单金额上限、单日亏损熔断、杠杆上限、滑点保护、一键 Panic 撤单全平
3. **模拟操盘沙盒引擎 (`paper_trading.py`)**:
   - 支持全真模拟撮合、动态盯市计算浮动盈亏（UPL）、自动止盈止损触发
4. **REST API 网关 (`main.py`)**:
   - 行情聚合、订单流转、持仓管理、AI 决策触发、SSE 流式接口

## 快速启动

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 启动 FastAPI 服务 (端口 8000)
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
