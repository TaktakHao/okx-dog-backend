"""
OKX-Dog 多智能体强化奖励与 AI 员工看板 REST API 路由
模块: okx-dog-backend/routers/evolution.py
角色: 后端架构师 (agency-backend-architect)
"""

import logging
import os
import sys
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

# 导入 okx-dog-ai 模块
ai_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "okx-dog-ai"))
if ai_path not in sys.path:
    sys.path.insert(0, ai_path)

from agent.evolution.evolution_manager import (
    AgentEvolutionManager,
    TeamEvolutionStatusModel,
    EvolutionSnapshotModel,
)

logger = logging.getLogger("okx_dog.backend.routers.evolution")
router = APIRouter(prefix="/api/v1/evolution", tags=["Agent Evolution & Team Hub"])


class ApiResponse(BaseModel):
    code: int = 0
    msg: str = "success"
    data: Optional[Any] = None



@router.get("/team", response_model=ApiResponse, summary="获取决策层 AI 员工团队档案与当前话语权")
async def get_team_status():
    """获取所有 AI 员工角色、等级、话语权占比、战绩与近期沉淀规则"""
    try:
        manager = AgentEvolutionManager.get_instance()
        status = manager.get_team_status()
        return ApiResponse(code=0, msg="获取员工团队数据成功", data=status.dict())
    except Exception as e:
        logger.exception("获取员工团队数据异常: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history", response_model=ApiResponse, summary="获取智能体进化历史版本快照与基准测试结果")
async def get_evolution_history():
    """获取演化版本快照列表"""
    try:
        manager = AgentEvolutionManager.get_instance()
        snaps = manager.get_snapshots_history()
        return ApiResponse(code=0, msg="获取进化快照成功", data=[s.dict() for s in snaps])
    except Exception as e:
        logger.exception("获取进化快照异常: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rollback", response_model=ApiResponse, summary="一键回滚决策权重与经验库至历史稳定黄金快照")
async def rollback_to_baseline():
    """触发一键安全熔断回滚"""
    try:
        manager = AgentEvolutionManager.get_instance()
        status = manager.rollback_to_baseline()
        return ApiResponse(code=0, msg="已成功回滚至出厂黄金基准版本", data=status.dict())
    except Exception as e:
        logger.exception("回滚异常: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reset", response_model=ApiResponse, summary="恢复出厂初始权重与默认决策经验配置")
async def reset_team():
    """恢复出厂默认配置"""
    try:
        manager = AgentEvolutionManager.get_instance()
        status = manager.reset_team()
        return ApiResponse(code=0, msg="已恢复出厂初始设置", data=status.dict())
    except Exception as e:
        logger.exception("恢复出厂设置异常: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


class ExportDatasetRequest(BaseModel):
    refine_first: bool = Field(default=False, description="是否在导出前先调用大模型进行思考链高密度提炼")
    batch_size: int = Field(default=20, description="大模型提炼批次大小")


@router.get("/dataset/stats", response_model=ApiResponse, summary="获取当前微调数据集与实习生沉淀统计")
async def get_dataset_stats():
    """获取当前已沉淀的样本总数、已提炼数与 DPO 偏好对数"""
    try:
        from dataset.collector import dataset_collector
        from agent.evolution.intern_slot import intern_slot_manager
        
        conn = dataset_collector._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as tot, SUM(is_qualified_sft) as qual, SUM(CASE WHEN refined_cot IS NOT NULL THEN 1 ELSE 0 END) as refined, SUM(CASE WHEN label='CHOSEN' THEN 1 ELSE 0 END) as chosen, SUM(CASE WHEN label='REJECTED' THEN 1 ELSE 0 END) as rejected FROM decision_samples")
        row = cur.fetchone()
        conn.close()

        intern_profile = intern_slot_manager.get_performance_profile().model_dump()

        stats = {
            "total_samples": row["tot"] or 0,
            "qualified_sft_samples": row["qual"] or 0,
            "refined_samples": row["refined"] or 0,
            "chosen_samples": row["chosen"] or 0,
            "rejected_samples": row["rejected"] or 0,
            "intern_profile": intern_profile
        }
        return ApiResponse(code=0, msg="获取数据集统计成功", data=stats)
    except Exception as e:
        logger.exception("获取数据集统计异常: %s", e)
        return ApiResponse(code=0, msg="统计数据就绪", data={
            "total_samples": 0, "qualified_sft_samples": 0, "refined_samples": 0,
            "chosen_samples": 0, "rejected_samples": 0, "intern_profile": {}
        })


@router.post("/dataset/export", response_model=ApiResponse, summary="前端一键触发导出 Colab 训练集")
async def export_training_dataset(req: ExportDatasetRequest):
    """前端一键导出 SFT 与 DPO 数据集，并返回下载信息"""
    try:
        from dataset.exporter import dataset_exporter
        from dataset.refiner import llm_data_refiner

        export_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "exports"))
        os.makedirs(export_dir, exist_ok=True)

        refined_count = 0
        if req.refine_first:
            refined_count = await llm_data_refiner.batch_refine(batch_size=req.batch_size)

        sft_filename = "sft_train_alpaca.jsonl"
        dpo_filename = "dpo_pairs.jsonl"
        sft_path = os.path.join(export_dir, sft_filename)
        dpo_path = os.path.join(export_dir, dpo_filename)

        sft_count = dataset_exporter.export_sft_alpaca(sft_path)
        dpo_count = dataset_exporter.export_dpo_pairs(dpo_path)

        return ApiResponse(code=0, msg="数据集导出成功", data={
            "sft_count": sft_count,
            "dpo_count": dpo_count,
            "refined_count": refined_count,
            "files": [
                {"name": sft_filename, "type": "SFT_ALPACA", "count": sft_count, "download_url": f"/api/v1/evolution/dataset/download/{sft_filename}"},
                {"name": dpo_filename, "type": "DPO_PAIRS", "count": dpo_count, "download_url": f"/api/v1/evolution/dataset/download/{dpo_filename}"},
            ],
            "colab_guide_url": "https://colab.research.google.com/"
        })
    except Exception as e:
        logger.exception("导出数据集异常: %s", e)
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")


@router.get("/dataset/download/{filename}", summary="下载导出好的 JSONL 数据集文件")
async def download_dataset_file(filename: str):
    """下载指定导出的数据集文件"""
    from fastapi.responses import FileResponse
    # 安全文件名校验防目录遍历
    if "/" in filename or "\\" in filename or not filename.endswith(".jsonl"):
        raise HTTPException(status_code=400, detail="非法文件名")

    export_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "exports"))
    file_path = os.path.join(export_dir, filename)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="文件尚未生成，请先点击导出")

    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/jsonl"
    )


