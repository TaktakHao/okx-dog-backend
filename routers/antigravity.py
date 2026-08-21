"""
OKX-Dog 个人量化智能交易系统 - Antigravity CLI 驱动专用路由
模块: okx-dog-backend/routers/antigravity.py
处理 Antigravity CLI 免 Key 结构化研判推理、状态健康检查
"""

import time
from typing import Any, Dict
from fastapi import APIRouter, HTTPException

from state import antigravity_bridge

router = APIRouter(prefix="/api/v1/antigravity", tags=["Antigravity 本地驱动"])


@router.get("/health", summary="Antigravity CLI 驱动健康检查")
async def antigravity_health():
    cli_ready = bool(antigravity_bridge and antigravity_bridge.is_available())
    return {
        "status": "healthy" if cli_ready else "degraded",
        "service": "Antigravity Embedded Gateway",
        "cli_path": getattr(antigravity_bridge, "cli_path", "unknown") if antigravity_bridge else "none",
        "cli_available": cli_ready,
        "timestamp": int(time.time()),
    }


@router.post("/generate", summary="Antigravity 原生结构化研判接口")
async def antigravity_generate_endpoint(req: Dict[str, Any]):
    if not (antigravity_bridge and antigravity_bridge.is_available()):
        raise HTTPException(status_code=503, detail="Antigravity CLI 驱动未就绪")

    prompt = req.get("prompt", "")
    if not prompt:
        raise HTTPException(status_code=400, detail="prompt 字段为必填项")
    schema = req.get("schema")
    effort = req.get("effort", "medium")
    model = req.get("model")
    timeout = req.get("timeout_seconds", 60.0)

    try:
        content, thinking, latency_ms, model_used = await antigravity_bridge.generate(
            prompt=prompt,
            response_schema=schema,
            effort=effort,
            model=model,
            timeout_seconds=timeout,
        )
        return {
            "code": 0,
            "msg": "success",
            "data": {
                "content": content,
                "thinking": thinking,
                "latency_ms": latency_ms,
                "model_used": model_used,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
