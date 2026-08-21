"""
OKX-Dog 数据湖 REST API 路由
模块: okx-dog-backend/routers/data_lake.py
"""

from fastapi import APIRouter, HTTPException, Depends
from models import ApiResponse, DataLakeStatusResponse, DataLakeSyncRequest
from state import data_lake_service

router = APIRouter(prefix="/api/v1/datalake", tags=["DataLake"])

@router.get("/status", response_model=ApiResponse[DataLakeStatusResponse], summary="查询本地数据湖存储现状")
async def get_datalake_status():
    try:
        res = await data_lake_service.get_data_lake_status()
        return ApiResponse(code=0, msg="查询数据湖状态成功", data=res)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sync", response_model=ApiResponse[dict], summary="触发历史行情多周期同步采集")
async def sync_datalake(req: DataLakeSyncRequest):
    try:
        res = await data_lake_service.sync_symbol_history(
            symbol=req.symbol,
            timeframes=req.timeframes,
            days_back=req.days_back
        )
        return ApiResponse(code=0, msg="数据湖历史同步完成", data=res)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))