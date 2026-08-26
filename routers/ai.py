"""
OKX-Dog 个人量化智能交易系统 - AI 决策与研判路由
模块: okx-dog-backend/routers/ai.py
处理大模型 / LangGraph 智能研判、SSE 实时思考流输出以及历史研判日志与胜率归因
"""

import asyncio
import json
import time
from typing import Any, Dict, Optional
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import (
    AIAnalysisResponse,
    ApiResponse,
    SignalAction,
    TriggerAIAnalysisRequest,
)
from services.ai_service import generate_quantitative_rule_decision
from services.market_service import fetch_full_market_context_snapshot
from state import antigravity_bridge, logger

try:
    from okx_dog_ai import (
        LLMClient,
        MarketPromptBuilder,
        QuantTraderAgentRunner,
        RobustJSONParser,
    )
except ImportError:
    LLMClient = Any
    MarketPromptBuilder = Any
    QuantTraderAgentRunner = Any
    RobustJSONParser = Any

router = APIRouter(prefix="/api/v1/ai", tags=["AI 智能研判与决策中心"])


@router.post("/analyze", response_model=ApiResponse[AIAnalysisResponse], summary="触发 AI 智能交易研判")
async def trigger_ai_analysis(
    req: TriggerAIAnalysisRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    结合实时行情、多周期 K 线指标与衍生品盘口，调用 LangGraph 资深交易员状态机或量化规则引擎生成决策
    """
    now_ms = int(time.time() * 1000)
    symbol = req.symbol or "BTC-USDT-SWAP"
    agent_mode = (req.mode or "agent").lower()

    # 1. 聚合全景实时市场数据快照
    (
        current_price,
        snapshot,
        multi_ind,
        derivatives_metrics,
        llm_key,
        base_url,
        model_name,
        temperature,
        max_tokens,
        llm_provider
    ) = await fetch_full_market_context_snapshot(symbol, db)

    analysis_res: Optional[AIAnalysisResponse] = None
    llm_cfg = {
        "provider": llm_provider,
        "base_url": base_url,
        "api_key": llm_key,
        "model_name": model_name,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    # 2. 优先发起真实 AI 深度推理 (Antigravity CLI / DeepSeek / OpenAI)
    should_run_llm = (llm_provider == "antigravity" and antigravity_bridge and antigravity_bridge.is_available()) or bool(llm_key and llm_key.strip())
    if should_run_llm and LLMClient is not Any:
        try:
            logger.info(f"正在向 AI 引擎 ({llm_provider}) 发起全景深度交易研判: model={model_name}, symbol={symbol}")
            builder = MarketPromptBuilder()
            messages = builder.build_messages(snapshot, scenario="standard", max_tokens=max_tokens)

            llm_client = LLMClient(
                provider=llm_provider,
                base_url=base_url,
                api_key=llm_key,
                model=model_name,
                temperature=temperature,
                max_tokens=max_tokens,
                timeout_seconds=60.0,
                max_retries=1
            )
            content, thinking, latency_ms, actual_model = await llm_client.generate(messages)
            await llm_client.close()

            if content and RobustJSONParser is not Any:
                analysis_res = RobustJSONParser.parse(
                    content,
                    symbol=symbol,
                    model_used=actual_model or model_name,
                    latency_ms=latency_ms,
                    preset_thinking=thinking
                )
                logger.info(f"AI 引擎 ({actual_model or model_name}) 研判成功返回: action={analysis_res.signal.action}, 置信度={analysis_res.signal.confidence}, 耗时={latency_ms}ms")
        except Exception as e:
            logger.warning(f"AI 引擎调用或解析异常，平滑降级至 LangGraph 量化反思状态机: {e}")
            analysis_res = None

    # 3. 若未配置 LLM 或大模型调用异常，执行 LangGraph 资深量化交易员反思状态机
    if not analysis_res and agent_mode == "agent" and QuantTraderAgentRunner is not Any:
        try:
            logger.info(f"启动 LangGraph 资深量化交易员多阶段反思 Agent: symbol={symbol}")
            agent_runner = QuantTraderAgentRunner(default_llm_config=llm_cfg)
            analysis_res = await agent_runner.run(snapshot, llm_config=llm_cfg)
            logger.info(f"LangGraph Agent 研判成功: symbol={symbol}, action={analysis_res.signal.action}, 耗时={analysis_res.latency_ms}ms")
        except Exception as e:
            logger.warning(f"LangGraph Agent 执行异常，平滑降级至高保真规则引擎: {e}")
            analysis_res = None

    # 4. 极端兜底：纯量化规则决策引擎
    if not analysis_res or (analysis_res.signal.action == SignalAction.HOLD_WAIT and analysis_res.signal.confidence == 0):
        avail_u = getattr(snapshot, "account_balance_usdt", None) if snapshot else None
        analysis_res = generate_quantitative_rule_decision(
            symbol=symbol,
            current_price=current_price,
            multi_indicators=multi_ind,
            derivatives=derivatives_metrics,
            risk_limits=getattr(snapshot, "risk_limits", None) if snapshot else None,
            available_funds=avail_u,
            model_name=model_name if (llm_key and llm_key.strip()) else "quantitative-rule-v2"
        )

    # 5. 异步记录研判日志到 ai_analysis_logs 表
    try:
        sql = text("""
            INSERT INTO ai_analysis_logs (
                analysis_uuid, symbol, timestamp, market_regime, signal_action,
                confidence_score, recommended_leverage, entry_range_low, entry_range_high,
                stop_loss_price, take_profit_1, risk_reward_ratio, reasoning_summary,
                thinking_process, model_used, latency_ms, created_at
            ) VALUES (
                :uuid, :sym, :ts, :regime, :action,
                :conf, :lever, :entry_l, :entry_h,
                :sl, :tp, :rr, :summary,
                :thinking, :model, :latency, CURRENT_TIMESTAMP
            )
        """)
        await db.execute(sql, {
            "uuid": analysis_res.analysis_id,
            "sym": symbol,
            "ts": now_ms,
            "regime": analysis_res.market_regime.value if hasattr(analysis_res.market_regime, "value") else str(analysis_res.market_regime),
            "action": analysis_res.signal.action.value if hasattr(analysis_res.signal.action, "value") else str(analysis_res.signal.action),
            "conf": int(analysis_res.signal.confidence * 100),
            "lever": analysis_res.trade_plan.suggested_leverage,
            "entry_l": analysis_res.trade_plan.entry_range[0],
            "entry_h": analysis_res.trade_plan.entry_range[1],
            "sl": analysis_res.trade_plan.stop_loss_price,
            "tp": analysis_res.trade_plan.take_profit_levels[0].price if analysis_res.trade_plan.take_profit_levels else 0.0,
            "rr": analysis_res.trade_plan.risk_reward_ratio,
            "summary": analysis_res.reasoning_summary,
            "thinking": analysis_res.thinking_process,
            "model": analysis_res.model_used,
            "latency": analysis_res.latency_ms
        })
        await db.commit()
    except Exception as e:
        logger.warning(f"写入 AI 研判日志失败: {e}")

    # 6. 自动沉淀到模型微调训练数据集 (SFT/DPO) + 异步触发实习生影子推演
    try:
        from dataset.collector import dataset_collector
        from agent.evolution.intern_slot import intern_slot_manager

        snap_dict = snapshot.dict() if hasattr(snapshot, "dict") else dict(snapshot)
        resp_clean = {
            "analysis_id": analysis_res.analysis_id,
            "action": analysis_res.signal.action.value if hasattr(analysis_res.signal.action, "value") else str(analysis_res.signal.action),
            "confidence": analysis_res.signal.confidence,
            "reasoning": analysis_res.reasoning_summary,
            "suggested_entry_price": analysis_res.trade_plan.entry_range[0] if analysis_res.trade_plan.entry_range else current_price,
            "stop_loss_price": analysis_res.trade_plan.stop_loss_price,
            "take_profit_price": analysis_res.trade_plan.take_profit_levels[0].price if analysis_res.trade_plan.take_profit_levels else 0.0,
            "target_leverage": analysis_res.trade_plan.suggested_leverage,
            "order_type": analysis_res.trade_plan.recommended_order_type.value if hasattr(analysis_res.trade_plan.recommended_order_type, "value") else str(analysis_res.trade_plan.recommended_order_type),
            "raw_thinking": getattr(analysis_res, "thinking_process", "")
        }
        sample_id = dataset_collector.record_decision_sample(
            symbol=symbol,
            current_price=current_price,
            market_snapshot=snap_dict,
            final_response=resp_clean,
            analysis_id=analysis_res.analysis_id
        )
        if sample_id:
            logger.info(f"📦 [训练集沉淀] 成功捕获一条微调决策样本: sample_id={sample_id}, symbol={symbol}, action={resp_clean['action']}")

        if intern_slot_manager and intern_slot_manager.is_enabled():
            asyncio.create_task(
                intern_slot_manager.trigger_shadow_inference(
                    symbol=symbol,
                    current_price=current_price,
                    market_snapshot=snap_dict,
                    senior_response=resp_clean,
                    analysis_id=analysis_res.analysis_id
                )
            )
    except Exception as d_err:
        logger.warning(f"沉淀训练集数据或触发实习生推演异常 (不影响主交易链路): {d_err}")

    return ApiResponse(code=0, msg="AI 智能研判生成成功", data=analysis_res, timestamp=now_ms)


@router.get("/analyze/stream", summary="流式触发 AI 智能交易研判 (SSE)")
async def trigger_ai_analysis_stream(
    symbol: str = Query("BTC-USDT-SWAP", description="标的代码"),
    timeframe: str = Query("1h", description="核心分析周期"),
    mode: str = Query("agent", description="研判模式: 'agent' (LangGraph 资深交易员状态机) 或 'fast' (快速生成)"),
    db: AsyncSession = Depends(get_db)
):
    """
    通过 Server-Sent Events (SSE) 实时流式输出大模型 / LangGraph 思考链与结构化决策结果
    """
    now_ms = int(time.time() * 1000)

    # 1. 聚合全景实时市场数据快照
    (
        current_price,
        snapshot,
        multi_ind,
        derivatives_metrics,
        llm_key,
        base_url,
        model_name,
        temperature,
        max_tokens,
        llm_provider
    ) = await fetch_full_market_context_snapshot(symbol, db)

    llm_cfg = {
        "provider": llm_provider,
        "base_url": base_url,
        "api_key": llm_key,
        "model_name": model_name,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    async def sse_event_generator():
        # 发送起始事件
        yield f"event: start\ndata: {json.dumps({'symbol': symbol, 'model': model_name, 'mode': mode, 'timestamp': now_ms})}\n\n"

        analysis_res: Optional[AIAnalysisResponse] = None

        # 1. 优先使用真实 AI 引擎 (Antigravity CLI / DeepSeek / OpenAI) SSE 流式推送
        should_run_stream = (llm_provider == "antigravity" and antigravity_bridge and antigravity_bridge.is_available()) or bool(llm_key and llm_key.strip())
        if should_run_stream and LLMClient is not Any:
            try:
                builder = MarketPromptBuilder()
                messages = builder.build_messages(snapshot, scenario="standard", max_tokens=max_tokens)

                llm_client = LLMClient(
                    provider=llm_provider,
                    base_url=base_url,
                    api_key=llm_key,
                    model=model_name,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    timeout_seconds=60.0
                )

                async for event in llm_client.generate_stream(messages):
                    ev_type = event.get("type")
                    if ev_type == "think":
                        yield f"event: think\ndata: {json.dumps({'delta': event.get('delta', '')})}\n\n"
                    elif ev_type == "content":
                        yield f"event: content\ndata: {json.dumps({'delta': event.get('delta', '')})}\n\n"
                    elif ev_type == "done":
                        full_content = event.get("full_content", "")
                        full_thinking = event.get("full_thinking", "")
                        if RobustJSONParser is not Any:
                            analysis_res = RobustJSONParser.parse(
                                full_content,
                                symbol=symbol,
                                model_used=model_name,
                                latency_ms=event.get("latency_ms", 0),
                                preset_thinking=full_thinking
                            )
                await llm_client.close()
            except Exception as e:
                logger.warning(f"流式 LLM 生成异常，切换到 LangGraph / 量化流推演: {e}")
                analysis_res = None

        # 2. 若未配置 LLM 或 LLM 异常，通过 LangGraph QuantTraderAgentRunner 进行流式推送
        if not analysis_res and mode == "agent" and QuantTraderAgentRunner is not Any:
            try:
                agent_runner = QuantTraderAgentRunner(default_llm_config=llm_cfg)
                async for chunk in agent_runner.run_stream(snapshot, llm_config=llm_cfg):
                    if chunk.event == "think":
                        yield f"event: think\ndata: {json.dumps({'delta': chunk.data})}\n\n"
                    elif chunk.event == "done" and chunk.structured_output:
                        analysis_res = chunk.structured_output
                        res_data = analysis_res.model_dump() if hasattr(analysis_res, "model_dump") else analysis_res.dict()
                        yield f"event: done\ndata: {json.dumps(res_data)}\n\n"
                        return
            except Exception as e:
                logger.warning(f"LangGraph SSE 流式生成异常，切换到量化推演: {e}")
                analysis_res = None

        # 3. 兜底量化推演
        if not analysis_res:
            analysis_res = generate_quantitative_rule_decision(
                symbol=symbol,
                current_price=current_price,
                multi_indicators=multi_ind,
                derivatives=derivatives_metrics,
                risk_limits=getattr(snapshot, "risk_limits", None) if snapshot else None,
                model_name=model_name if (llm_key and llm_key.strip()) else "quantitative-rule-v2"
            )
            # 渐显推送思维链
            if analysis_res.thinking_process:
                clean_think = analysis_res.thinking_process.replace("<think>", "").replace("</think>", "").strip()
                chunk_sz = 16
                for i in range(0, len(clean_think), chunk_sz):
                    yield f"event: think\ndata: {json.dumps({'delta': clean_think[i:i+chunk_sz]})}\n\n"
                    await asyncio.sleep(0.02)

        # 发送完成事件并附带结构化结果
        res_data = analysis_res.model_dump() if hasattr(analysis_res, "model_dump") else analysis_res.dict()
        yield f"event: done\ndata: {json.dumps(res_data)}\n\n"

    return StreamingResponse(sse_event_generator(), media_type="text/event-stream")


@router.get("/history", response_model=ApiResponse[Dict[str, Any]], summary="查询历史 AI 研判日志与胜率归因")
async def get_ai_history(
    symbol: str = Query("BTC-USDT-SWAP", description="标的代码"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    db: AsyncSession = Depends(get_db)
):
    """分页查询历史 AI 决策快照与归因"""
    now_ms = int(time.time() * 1000)
    offset = (page - 1) * page_size

    res = await db.execute(
        text("SELECT * FROM ai_analysis_logs WHERE symbol = :sym ORDER BY id DESC LIMIT :limit OFFSET :offset"),
        {"sym": symbol, "limit": page_size, "offset": offset}
    )
    rows = [dict(r) for r in res.mappings().all()]

    cnt_res = await db.execute(
        text("SELECT COUNT(*) as total FROM ai_analysis_logs WHERE symbol = :sym"),
        {"sym": symbol}
    )
    total = cnt_res.scalar() or 0

    return ApiResponse(
        code=0,
        msg="success",
        data={
            "items": rows,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size if total > 0 else 1
        },
        timestamp=now_ms
    )
