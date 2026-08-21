"""
OKX-Dog 避坑规则知识库 REST API 路由
模块: okx-dog-backend/routers/knowledge_base.py
"""

from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from models import ApiResponse, TradingKnowledgeListResponse, TradingKnowledgeRule
from state import knowledge_base_service

router = APIRouter(prefix="/api/v1/knowledge-base", tags=["Knowledge Base"])

@router.get("/rules", response_model=ApiResponse[TradingKnowledgeListResponse], summary="查询避坑规则列表")
async def get_rules(rule_type: Optional[str] = Query(None), is_active: bool = Query(True)):
    try:
        data = await knowledge_base_service.get_rules(rule_type=rule_type, is_active_only=is_active)
        return ApiResponse(code=0, msg="获取避坑规则成功", data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/rules", response_model=ApiResponse[dict], summary="添加或更新避坑规则")
async def save_rule(rule: TradingKnowledgeRule):
    try:
        r_id = await knowledge_base_service.add_or_update_rule(
            rule_type=rule.rule_type,
            trigger_condition=rule.trigger_condition,
            action_rule=rule.action_rule,
            symbol_scope=rule.symbol_scope,
            confidence_score=rule.confidence_score,
            source_review_date=rule.source_review_date,
            rule_id=rule.id
        )
        return ApiResponse(code=0, msg="保存避坑规则成功", data={"rule_id": r_id})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))