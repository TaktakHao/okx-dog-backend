"""
OKX-Dog 个人量化智能交易系统 - 全局异常处理器
模块: okx-dog-backend/core/exceptions.py
统一定义并注册安全凭证、提现权限拦截、硬风控阻断与 OKX API 异常处理
"""

import time
import logging
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from vault import FatalWithdrawalPermissionException, SecurityException
from risk_controller import RiskInterceptionException, CircuitBreakerTriggeredException, SafeModeException
from okx_rest import OKXApiException

logger = logging.getLogger("okx_dog.exceptions")


def register_exception_handlers(app: FastAPI) -> None:
    """向 FastAPI 实例注册全局异常处理器"""

    @app.exception_handler(FatalWithdrawalPermissionException)
    async def fatal_security_handler(request: Request, exc: FatalWithdrawalPermissionException):
        """提现权限致命安全拦截 (403 强阻断)"""
        logger.critical(f"【安全红线拦截】{str(exc)}")
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={
                "code": 2004,
                "msg": str(exc),
                "data": None,
                "timestamp": int(time.time() * 1000)
            }
        )

    @app.exception_handler(SecurityException)
    async def security_exception_handler(request: Request, exc: SecurityException):
        """安全凭证与解密异常 (400)"""
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": 2002,
                "msg": str(exc),
                "data": None,
                "timestamp": int(time.time() * 1000)
            }
        )

    @app.exception_handler(RiskInterceptionException)
    async def risk_exception_handler(request: Request, exc: RiskInterceptionException):
        """硬风控前置拦截异常 (400 / 403)"""
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": 3001,
                "msg": str(exc),
                "data": {
                    "rule_name": exc.rule_name,
                    "current_value": exc.current_value,
                    "limit_value": exc.limit_value
                },
                "timestamp": int(time.time() * 1000)
            }
        )

    @app.exception_handler(OKXApiException)
    async def okx_api_exception_handler(request: Request, exc: OKXApiException):
        """OKX 官方 API 业务报错"""
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": 4001,
                "msg": f"OKX API 报错: {exc.msg} (code={exc.code})",
                "data": exc.data,
                "timestamp": int(time.time() * 1000)
            }
        )
