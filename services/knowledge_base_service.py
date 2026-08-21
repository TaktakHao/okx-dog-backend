"""
OKX-Dog 个人量化智能交易系统 - 交易经验与避坑规则知识库中枢 (RAG Knowledge Base)
模块: okx-dog-backend/services/knowledge_base_service.py
角色: 后端架构师 (agency-backend-architect) & AI工程师 (agency-ai-engineer)
功能:
1. 沉淀每日复盘提炼的失误教训与最佳实践 (trading_knowledge_base)
2. Negative Few-Shot 场景检索: 根据当前标的与盘面特征召回最相关的历史避坑规则
3. 命中与拦截计数统计 (times_triggered 累加)
4. 规则的增删改查与启停管理
"""

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from database import get_db_session
from models import TradingKnowledgeListResponse, TradingKnowledgeRule

logger = logging.getLogger("okx_dog.knowledge_base")


class KnowledgeBaseService:
    """交易避坑知识库服务"""

    def __init__(self):
        pass

    async def get_rules(self, rule_type: Optional[str] = None, is_active_only: bool = True) -> TradingKnowledgeListResponse:
        """获取避坑规则列表"""
        async with get_db_session() as db:
            sql = "SELECT * FROM trading_knowledge_base"
            clauses = []
            params: Dict[str, Any] = {}

            if is_active_only:
                clauses.append("is_active = 1")
            if rule_type:
                clauses.append("rule_type = :rule_type")
                params["rule_type"] = rule_type

            if clauses:
                sql += " WHERE " + " AND ".join(clauses)

            sql += " ORDER BY times_triggered DESC, created_at DESC"

            res = await db.execute(text(sql), params)
            rows = res.mappings().all()

            if not rows and is_active_only:
                # 初始预置高价值实战避坑规则
                await self._seed_initial_rules()
                return await self.get_rules(rule_type, is_active_only)

            rules = [
                TradingKnowledgeRule(
                    id=r["id"],
                    rule_type=r["rule_type"],
                    symbol_scope=r["symbol_scope"],
                    trigger_condition=r["trigger_condition"],
                    action_rule=r["action_rule"],
                    confidence_score=float(r["confidence_score"] or 1.0),
                    source_review_date=r["source_review_date"],
                    times_triggered=int(r["times_triggered"] or 0),
                    is_active=bool(r["is_active"]),
                    created_at=str(r["created_at"])
                )
                for r in rows
            ]
            return TradingKnowledgeListResponse(rules=rules, total_count=len(rules))

    async def add_or_update_rule(
        self,
        rule_type: str,
        trigger_condition: str,
        action_rule: str,
        symbol_scope: str = "*",
        confidence_score: float = 1.0,
        source_review_date: Optional[str] = None,
        rule_id: Optional[str] = None
    ) -> str:
        """新增或更新避坑规则"""
        r_id = rule_id or str(uuid.uuid4())[:12]
        async with get_db_session() as db:
            sql = text("""
                INSERT OR REPLACE INTO trading_knowledge_base (
                    id, rule_type, symbol_scope, trigger_condition,
                    action_rule, confidence_score, source_review_date,
                    times_triggered, is_active, created_at, updated_at
                ) VALUES (
                    :id, :rule_type, :symbol_scope, :trigger_condition,
                    :action_rule, :confidence_score, :source_review_date,
                    COALESCE((SELECT times_triggered FROM trading_knowledge_base WHERE id = :id), 0),
                    1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
            """)
            await db.execute(sql, {
                "id": r_id,
                "rule_type": rule_type,
                "symbol_scope": symbol_scope,
                "trigger_condition": trigger_condition,
                "action_rule": action_rule,
                "confidence_score": confidence_score,
                "source_review_date": source_review_date
            })
        logger.info(f"避坑规则已沉淀落库 [{r_id}]: {trigger_condition} -> {action_rule}")
        return r_id

    async def retrieve_relevant_rules(self, symbol: str, context_hint: str = "", limit: int = 2) -> List[str]:
        """
        RAG 检索与盘面/标的最相关的避坑规则 (Negative Few-Shot)
        并在命中时递增 times_triggered
        """
        rules_resp = await self.get_rules(is_active_only=True)
        all_rules = rules_resp.rules
        if not all_rules:
            return []

        matched: List[TradingKnowledgeRule] = []
        hint_lower = context_hint.lower()

        # 优先标的匹配或全品类 '*'
        for r in all_rules:
            if r.symbol_scope == "*" or r.symbol_scope.lower() in symbol.lower():
                matched.append(r)

        selected = matched[:limit]
        out_prompts: List[str] = []

        # 累加命中次数
        async with get_db_session() as db:
            for s in selected:
                out_prompts.append(f"【前车之鉴警示】前置: {s.trigger_condition} => 准则: {s.action_rule}")
                await db.execute(
                    text("UPDATE trading_knowledge_base SET times_triggered = times_triggered + 1, updated_at = CURRENT_TIMESTAMP WHERE id = :id"),
                    {"id": s.id}
                )

        return out_prompts

    async def _seed_initial_rules(self):
        """预置系统初始避坑规则库"""
        seeds = [
            {
                "rule_type": "MISTAKE_AVOIDANCE",
                "symbol_scope": "*",
                "trigger_condition": "资金费率 > 0.05% 且 4H RSI > 75 顶背离",
                "action_rule": "坚决禁止追涨开多，仅允许等待背离确认后轻仓试空或空仓观望"
            },
            {
                "rule_type": "BEST_PRACTICE",
                "symbol_scope": "*",
                "trigger_condition": "持仓浮盈达到第一目标位 (0.5R ~ 1.0R)",
                "action_rule": "强制将止损线移至开仓成本价锁定保本，杜绝让盈利单变成亏损单"
            },
            {
                "rule_type": "MACRO_REGIME",
                "symbol_scope": "*",
                "trigger_condition": "美国 CPI 或 美联储 FOMC 公布前 15 分钟",
                "action_rule": "严禁在重大宏观数据发布前后 15 分钟内盲目猜单开仓，防止假突破插针爆仓"
            }
        ]
        for s in seeds:
            await self.add_or_update_rule(
                rule_type=s["rule_type"],
                trigger_condition=s["trigger_condition"],
                action_rule=s["action_rule"],
                symbol_scope=s["symbol_scope"]
            )