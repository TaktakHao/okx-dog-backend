"""
OKX-Dog 个人量化智能交易系统 - 硬风控中枢与紧急熔断路由
模块: okx-dog-backend/routers/risk.py
处理风控状态查询、风控规则参数热更新、熔断手动解除、Panic Button 紧急全平以及风控审计日志分页查询
"""

import hmac
import json
import time
from typing import Any, Dict, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

import models
from config import settings
from database import get_db
from models import (
    ApiResponse,
    PanicButtonRequest,
    PanicButtonResponse,
    ResetCircuitBreakerRequest,
    RiskAuditLogModel,
    RiskEventType,
    RiskRulesConfig,
    RiskStatusResponse,
    WSPushPayload,
    WSServerEvent,
)
from state import (
    rest_client,
    risk_controller,
    ws_manager,
)

router = APIRouter(prefix="/api/v1/risk", tags=["硬风控中枢与紧急处置"])


@router.get("/status", response_model=ApiResponse[RiskStatusResponse], summary="查询风控中枢状态")
async def get_risk_status():
    """获取当前风控状态、熔断指标及当前风控规则阈值"""
    now_ms = int(time.time() * 1000)
    status_data = await risk_controller.get_status()
    return ApiResponse(code=0, msg="success", data=status_data, timestamp=now_ms)


@router.put("/config", response_model=ApiResponse[RiskRulesConfig], summary="更新硬风控规则参数")
async def update_risk_config(
    new_rules: RiskRulesConfig,
    db: AsyncSession = Depends(get_db)
):
    """热更新硬风控规则阈值并持久化至 SQLite"""
    now_ms = int(time.time() * 1000)
    risk_controller.update_rules(new_rules)

    sql = text("""
        UPDATE risk_limits SET
            max_order_usdt = :max_order,
            max_daily_loss_usdt = :max_loss,
            max_leverage = :max_lev,
            max_slippage_pct = :max_slip,
            max_open_positions = :max_pos,
            max_position_usdt = :max_pos_usdt,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = (SELECT id FROM risk_limits ORDER BY id DESC LIMIT 1)
    """)
    await db.execute(sql, {
        "max_order": new_rules.max_order_usdt,
        "max_loss": new_rules.max_daily_loss_usdt,
        "max_lev": new_rules.max_leverage,
        "max_slip": new_rules.max_slippage_pct,
        "max_pos": new_rules.max_open_positions,
        "max_pos_usdt": new_rules.max_position_usdt
    })
    await db.commit()

    return ApiResponse(code=0, msg="硬风控规则参数已成功更新", data=new_rules, timestamp=now_ms)


@router.post("/reset-circuit-breaker", response_model=ApiResponse[RiskStatusResponse], summary="手动解除熔断锁定")
async def reset_circuit_breaker(
    req: ResetCircuitBreakerRequest,
    db: AsyncSession = Depends(get_db)
):
    """交易员核对行情后输入主密码二次确认，方可手动解除熔断锁定"""
    now_ms = int(time.time() * 1000)
    # Stage 0 (T0.4): 严格校验主密码，杜绝当日误触或无授权随意解除熔断
    if not hmac.compare_digest(str(req.master_password or ""), str(settings.MASTER_PASSWORD)):
        await risk_controller.record_risk_event(
            event_type=RiskEventType.ORDER_REJECTED,
            rule_name="CIRCUIT_BREAKER_RESET_AUTH_FAILED",
            limit_val="VALID_MASTER_PASSWORD",
            current_val="INVALID_PASSWORD",
            is_blocked=True,
            action_taken="DENY_UNLOCK",
            severity=models.AuditSeverity.WARNING,
            details={"reason_attempt": req.reason},
        )
        raise HTTPException(status_code=403, detail="主密码校验失败，拒绝解除硬风控熔断锁定！")

    await risk_controller.reset_circuit_breaker(reason=req.reason)
    status_data = await risk_controller.get_status()
    return ApiResponse(code=0, msg="熔断锁定已解除，系统恢复正常交易状态", data=status_data, timestamp=now_ms)


@router.post("/panic-button", response_model=ApiResponse[PanicButtonResponse], summary="Panic Button 紧急撤单全平")
async def trigger_panic_button(
    req: PanicButtonRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Panic Button 紧急全平 (Cancel-Before-Close):
    1. 立即撤销所有活跃挂单释放保证金
    2. 立即发起市价全平所有持仓
    3. 系统锁死进入 SAFE_MODE 并持久化审计日志
    """
    now_ms = int(time.time() * 1000)
    panic_res = await risk_controller.execute_panic_button(rest_client, req)

    # 广播全网紧急风控事件
    push_msg = WSPushPayload[Dict[str, Any]](
        channel="risk_events",
        event=WSServerEvent.UPDATE,
        data={
            "event": "PANIC_BUTTON_TRIGGERED",
            "system_state": "SAFE_MODE",
            "message": panic_res.message
        },
        timestamp=now_ms
    )
    await ws_manager.broadcast_to_subscribers(push_msg)

    return ApiResponse(
        code=0,
        msg="紧急处置指令已执行完成",
        data=panic_res,
        timestamp=now_ms
    )


@router.get("/audit-logs", response_model=ApiResponse[List[RiskAuditLogModel]], summary="查询风控审计日志")
async def get_risk_audit_logs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """分页查询风控拦截事件与审计记录"""
    now_ms = int(time.time() * 1000)
    offset_val = (page - 1) * page_size

    sql = text("""
        SELECT * FROM risk_audit_logs
        ORDER BY id DESC
        LIMIT :limit OFFSET :offset
    """)
    res = await db.execute(sql, {"limit": page_size, "offset": offset_val})
    rows = res.mappings().all()

    logs: List[RiskAuditLogModel] = []
    for r in rows:
        details_val = json.loads(r["details_json"]) if r.get("details_json") else {}
        logs.append(RiskAuditLogModel(
            id=r["id"],
            event_type=RiskEventType(r["event_type"]),
            severity=models.AuditSeverity(r["severity"]),
            trigger_source=r["trigger_source"],
            rule_name=r["rule_name"],
            rule_limit_value=r["rule_limit_value"],
            current_metric_value=r["current_metric_value"],
            symbol=r["symbol"],
            is_blocked=bool(r["is_blocked"]),
            action_taken=r["action_taken"],
            details=details_val,
            timestamp=now_ms
        ))

    return ApiResponse(code=0, msg="success", data=logs, timestamp=now_ms)
