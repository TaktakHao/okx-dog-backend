"""
OKX-Dog 全网情报与热点新闻感知 REST API 路由
模块: okx-dog-backend/routers/news.py
"""

from fastapi import APIRouter, HTTPException, Query
from models import ApiResponse, MacroCalendarResponse, NewsFeedResponse
from state import news_service

router = APIRouter(prefix="/api/v1/news", tags=["News & Intelligence"])

@router.get("/feed", response_model=ApiResponse[NewsFeedResponse], summary="获取全网热点资讯流与AI情感打分")
async def get_news_feed(limit: int = Query(20, ge=1, le=100)):
    try:
        data = await news_service.get_latest_news_feed(limit=limit)
        return ApiResponse(code=0, msg="获取资讯流成功", data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/macro-calendar", response_model=ApiResponse[MacroCalendarResponse], summary="获取全球宏观经济日历与高危预警")
async def get_macro_calendar():
    try:
        data = await news_service.get_macro_calendar()
        return ApiResponse(code=0, msg="获取宏观日历成功", data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))