"""
OKX-Dog 后端多智能体博弈协同中枢
模块: okx-dog-backend/services/consensus_service.py
角色: 多智能体交易协同架构师 (agency-multi-agent-systems-architect)
"""

import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from models import MultiAgentConsensusResponse
from risk_controller import risk_controller
from services.knowledge_base_service import KnowledgeBaseService
from services.news_service import NewsService

logger = logging.getLogger("okx_dog.consensus")

# 引入 AI 端 consensus_network
ai_dir = str(Path(__file__).parent.parent.parent / "okx-dog-ai")
if ai_dir not in sys.path:
    sys.path.append(ai_dir)

from agent.consensus_network import ConsensusOrchestrator

class ConsensusService:
    """多智能体博弈与仲裁协同服务"""

    def __init__(self, kb_service: KnowledgeBaseService, news_service: NewsService):
        self.kb_service = kb_service
        self.news_service = news_service
        self.orchestrator = ConsensusOrchestrator()

    async def analyze_market_consensus(
        self,
        snapshot: Dict[str, Any],
        symbol: Optional[str] = None
    ) -> MultiAgentConsensusResponse:
        sym = symbol or snapshot.get("symbol", "BTC-USDT-SWAP")

        # 1. 召回相关的历史避坑教训 (Negative Few-Shot)
        rules = await self.kb_service.retrieve_relevant_rules(symbol=sym, limit=2)

        # 2. 拉取最新新闻情报
        news_resp = await self.news_service.get_latest_news_feed(limit=5)
        news_items = [n.model_dump() for n in news_resp.news] if news_resp else []

        # 3. 获取当前冷却状态
        is_cd, _, _ = risk_controller.is_in_cooldown()

        # 4. 执行多 Agent 对抗辩论与仲裁
        res = self.orchestrator.arbitrate(
            snapshot=snapshot,
            injected_rules=rules,
            news_items=news_items,
            is_in_cooldown=is_cd
        )
        return res