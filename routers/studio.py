"""
OKX-Dog AI Quant Studio & Codex Harness REST API 路由
模块: okx-dog-backend/routers/studio.py
角色: 后端架构师 (agency-backend-architect)
"""

import logging
import os
import sys
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

# 导入 okx-dog-ai/studio 模块
ai_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "okx-dog-ai"))
if ai_path not in sys.path:
    sys.path.insert(0, ai_path)

from studio.ast_guard import validate_python_code_security
from studio.sandbox_runner import run_strategy_in_sandbox
from studio.codex_client import CodexQuantStudioClient, DEFAULT_TEMPLATES
from studio.benchmark.eval_runner import BenchmarkRunner
from state import antigravity_bridge

logger = logging.getLogger("okx_dog.backend.routers.studio")
router = APIRouter(prefix="/api/v1/studio", tags=["AI Quant Studio"])


# =============================================================================
# 1. 契约模型定义 (Request / Response Models)
# =============================================================================

class GenerateStrategyReq(BaseModel):
    prompt: str = Field(..., description="交易员自然语言策略描述", example="15m RSI超卖且资金费率转正时开多")
    symbol: str = Field(default="BTC-USDT-SWAP", description="回测交易标的")
    timeframe: str = Field(default="15m", description="K线周期")
    max_retries: int = Field(default=3, ge=1, le=5, description="报错自动自愈最大重试次数")


class CustomBacktestReq(BaseModel):
    code: str = Field(..., description="Python 策略源代码")
    symbol: str = Field(default="BTC-USDT-SWAP", description="回测交易标的")
    timeframe: str = Field(default="15m", description="K线周期")


class StudioApiResponse(BaseModel):
    code: int = 0
    msg: str = "success"
    data: Optional[Any] = None


# =============================================================================
# 2. REST API 路由端点
# =============================================================================

@router.get("/templates", response_model=StudioApiResponse, summary="获取经典量化策略与因子模板库")
async def get_strategy_templates():
    """获取预设经典策略模板清单"""
    templates_list = [
        {
            "id": "bollinger_breakout",
            "name": "自适应布林带通道突破与挤压策略",
            "description": "计算动态布林带上下轨，突破上轨且带宽高位扩张时开多，跌破下轨开空",
            "code": DEFAULT_TEMPLATES["bollinger_breakout"]
        },
        {
            "id": "rsi_divergence_funding",
            "name": "RSI 超卖反弹共振资金费率策略",
            "description": "RSI 处于超卖区间 (<35) 向上反弹，且资金费率转正时捕捉多头动量",
            "code": DEFAULT_TEMPLATES["rsi_divergence_funding"]
        },
        {
            "id": "ema_cross_oi",
            "name": "双均线金叉共振未平仓量(OI)突破策略",
            "description": "EMA10 上穿 EMA30，伴随全网持仓量(OI)增加 >1% 时确立主力建仓信号",
            "code": DEFAULT_TEMPLATES["ema_cross_oi"]
        }
    ]
    return StudioApiResponse(code=0, msg="获取模板库成功", data=templates_list)


@router.post("/generate-strategy", response_model=StudioApiResponse, summary="AI 编写量化策略 + AST 审查 + 沙箱回测闭环")
async def generate_strategy_endpoint(req: GenerateStrategyReq):
    """
    接收交易员自然语言需求，驱动 Codex 生成 Python 策略，进行 AST 审查与沙箱回测，遇错自动自愈修复
    """
    try:
        # 复用现有 LLM Bridge 或初始化 Client
        llm = getattr(antigravity_bridge, "llm_client", None) if antigravity_bridge else None
        client = CodexQuantStudioClient(llm)
        
        result = await client.generate_and_verify_strategy(
            prompt=req.prompt,
            symbol=req.symbol,
            timeframe=req.timeframe,
            max_retries=req.max_retries
        )
        return StudioApiResponse(code=0, msg="策略生成与沙盒回测执行完成", data=result)
    except Exception as e:
        logger.exception("策略生成端点异常: %s", e)
        raise HTTPException(status_code=500, detail=f"策略实验室生成失败: {str(e)}")


@router.post("/backtest-custom", response_model=StudioApiResponse, summary="自定义 Python 策略代码 AST 安全审查与沙箱回测")
async def backtest_custom_endpoint(req: CustomBacktestReq):
    """
    对交易员手写或微调的 Python 代码进行 AST 静态安全审计，通过后在沙箱中执行回测
    """
    # 1. AST 静态安全审计
    ast_passed, ast_msg = validate_python_code_security(req.code)
    if not ast_passed:
        return StudioApiResponse(
            code=400,
            msg=f"AST 静态安全审查未通过: {ast_msg}",
            data={
                "ast_passed": False,
                "error_message": ast_msg,
                "metrics": None
            }
        )

    # 2. 受限沙箱回测
    res = await run_strategy_in_sandbox(
        code=req.code,
        symbol=req.symbol,
        timeframe=req.timeframe,
        timeout_sec=5.0
    )

    if not res.get("success"):
        return StudioApiResponse(
            code=400,
            msg=f"沙箱运行时错误: {res.get('error')}",
            data={
                "ast_passed": True,
                "error_message": res.get("error"),
                "metrics": None
            }
        )

    return StudioApiResponse(
        code=0,
        msg="自定义策略回测完成",
        data={
            "ast_passed": True,
            "status": "VERIFIED",
            "metrics": res.get("metrics"),
            "equity_curve": res.get("equity_curve", []),
            "trade_signals": res.get("trade_signals", []),
            "error_message": None
        }
    )


@router.post("/benchmark-eval", response_model=StudioApiResponse, summary="运行黄金基准测试集防退化评测")
async def benchmark_eval_endpoint():
    """批量评测 Prompt 与策略生成的合规率与回测评分报表"""
    try:
        llm = getattr(antigravity_bridge, "llm_client", None) if antigravity_bridge else None
        client = CodexQuantStudioClient(llm)
        runner = BenchmarkRunner(client)
        report = await runner.run_benchmark_suite()
        return StudioApiResponse(code=0, msg="黄金基准套件评测完成", data=report)
    except Exception as e:
        logger.exception("基准评测失败: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
