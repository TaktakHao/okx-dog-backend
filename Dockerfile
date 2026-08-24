# =============================================================================
# OKX-Dog 后端 FastAPI 生产级容器构建镜像
# =============================================================================
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Shanghai \
    PYTHONPATH=/app/okx-dog-backend:/app/okx-dog-ai

WORKDIR /app

# 安装底层编译与网络工具
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 先复制依赖清单以充分利用 Docker 缓存层
COPY okx-dog-backend/requirements.txt ./okx-dog-backend/
COPY okx-dog-ai/requirements.txt ./okx-dog-ai/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r okx-dog-backend/requirements.txt && \
    pip install --no-cache-dir -r okx-dog-ai/requirements.txt && \
    pip install --no-cache-dir pytest pytest-asyncio

# 复制源码
COPY okx-dog-backend ./okx-dog-backend
COPY okx-dog-ai ./okx-dog-ai

# 创建数据存储目录
RUN mkdir -p /app/okx-dog-backend/data

WORKDIR /app/okx-dog-backend

EXPOSE 8000

# 健康检查探针
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://127.0.0.1:8000/api/v1/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
