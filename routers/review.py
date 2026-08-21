"""
OKX-Dog 每日自动复盘 REST API 路由
模块: okx-dog-backend/routers/review.py
"""

from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from models import ApiResponse, DailyReviewDetail, DailyReviewListResponse
from state import daily_review_service

router = APIRouter(prefix="/api/v1/reviews", tags=["Daily Review"])

@router.get("/daily", response_model=ApiResponse[DailyReviewListResponse], summary="查询历史每日复盘报告列表")
async def get_daily_reviews(limit: int = Query(10, ge=1, le=50)):
    try:
        data = await daily_review_service.get_daily_reviews(limit=limit)
        return ApiResponse(code=0, msg="获取复盘报告成功", data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/generate", response_model=ApiResponse[DailyReviewDetail], summary="手动触发生成指定日期的复盘报告")
async def generate_review(date: Optional[str] = Query(None, description="格式: YYYY-MM-DD")):
    try:
        data = await daily_review_service.generate_daily_review(target_date=date)
        return ApiResponse(code=0, msg="生成复盘报告成功", data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))