"""
OKX-Dog 个人量化智能交易系统 - 资深财务分析师 REST API 路由
模块: okx-dog-backend/routers/finance.py
角色映射: agency-financial-analyst (资深财务分析师)
"""

from fastapi import APIRouter, HTTPException, Query
from models import ApiResponse
from services.finance_service import finance_service, FinancialHealthReport

router = APIRouter(prefix="/api/v1/finance", tags=["Finance & Capital Allocation"])


@router.get("/health", response_model=ApiResponse[FinancialHealthReport], summary="资深财务分析师: 全维度财务健康与资金效率体检")
async def get_financial_health(
    is_live: bool = Query(False, description="是否为实盘环境 (默认 False 模拟盘)")
):
    """
    提供资金利用率、夏普比率、索提诺比率、卡玛比率、费率摩擦(Fee Drag)损益归因及专家评级
    """
    try:
        report = finance_service.calculate_health_report(is_live=is_live)
        return ApiResponse(code=0, msg="财务健康与资金效率分析完成", data=report)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"财务体检计算异常: {str(e)}")
