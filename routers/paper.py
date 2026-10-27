"""
OKX-Dog 个人量化智能交易系统 - 模拟操盘沙盒与系统运行模式切换路由
模块: okx-dog-backend/routers/paper.py
处理模拟操盘资金与胜率战绩查询、模拟账户本金重置以及实盘 (LIVE) / 模拟 (DEMO) 模式全局切换
"""

import time
from typing import Any, Dict, List
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import (
    AccountBalanceModel,
    ApiResponse,
    AutoPilotStatusModel,
    PaperAccountInfo,
    PaperResetRequest,
    PositionModel,
    SwitchModeRequest,
    WSPushPayload,
    WSServerEvent,
)
from state import (
    auto_pilot_manager,
    logger,
    paper_engine,
    rest_client,
    risk_controller,
    vault,
    ws_manager,
)

router = APIRouter(tags=["模拟操盘沙盒与系统运行模式"])


@router.get("/api/v1/paper/account", response_model=ApiResponse[PaperAccountInfo], summary="获取模拟账户详情与胜率统计")
async def get_paper_account():
    """获取模拟操盘账户资金详情、累计盈亏与胜率战绩"""
    now_ms = int(time.time() * 1000)
    info = paper_engine.get_account_info()
    return ApiResponse(code=0, msg="success", data=info, timestamp=now_ms)


@router.post("/api/v1/paper/reset", response_model=ApiResponse[PaperAccountInfo], summary="重置模拟操盘账户与初始资金")
async def reset_paper_account(req: PaperResetRequest):
    """重置模拟账户初始虚拟资金，并清空所有模拟持仓与委托挂单"""
    now_ms = int(time.time() * 1000)
    info = paper_engine.reset_account(req.initial_equity_usdt)

    # 广播持仓与资产清空事件
    await ws_manager.broadcast_to_subscribers(WSPushPayload[List[PositionModel]](
        channel="positions",
        event=WSServerEvent.SNAPSHOT,
        data=[],
        timestamp=now_ms
    ))
    await ws_manager.broadcast_to_subscribers(WSPushPayload[AccountBalanceModel](
        channel="account",
        event=WSServerEvent.UPDATE,
        data=paper_engine.get_account_balance(),
        timestamp=now_ms
    ))

    return ApiResponse(
        code=0,
        msg=f"模拟操盘账户已成功重置！初始本金: {req.initial_equity_usdt:.2f} USDT",
        data=info,
        timestamp=now_ms
    )


@router.post("/api/v1/system/mode", response_model=ApiResponse[Dict[str, Any]], summary="切换实盘 / 模拟操盘模式")
async def switch_system_mode(
    req: SwitchModeRequest,
    db: AsyncSession = Depends(get_db)
):
    """切换系统全局运行模式 (实盘 LIVE vs 模拟沙盒 DEMO)"""
    now_ms = int(time.time() * 1000)

    try:
        # 切换环境时重置风控单日累计盈亏与熔断计数，确保实盘与模拟沙盒风控物理隔离
        risk_controller.reset_for_mode_switch()

        if req.target_live_mode:
            # 切换至实盘：从数据库中查询实盘凭证
            res = await db.execute(
                text("SELECT * FROM credentials_vault WHERE credential_type = 'OKX_V5' AND is_live_mode = 1 AND is_valid = 1 ORDER BY updated_at DESC, id DESC LIMIT 1")
            )
            row = res.mappings().first()
            if not row:
                res_any = await db.execute(
                    text("SELECT * FROM credentials_vault WHERE credential_type = 'OKX_V5' AND is_valid = 1 ORDER BY updated_at DESC, id DESC LIMIT 1")
                )
                row = res_any.mappings().first()

            if not row and not rest_client.api_key:
                raise HTTPException(status_code=400, detail="未找到已配置的 OKX 凭证，请先在右上角【安全与 API 配置】中配置并保存实盘 API Key！")

            if row:
                c_vault = vault if not req.master_password else vault.__class__(master_password=req.master_password)
                try:
                    dec_key, dec_sec, dec_pass = c_vault.decrypt_credentials(
                        row["encrypted_api_key"],
                        row["encrypted_secret_key"],
                        row["encrypted_passphrase"],
                        row["salt"],
                        row["nonce"]
                    )
                except Exception as e:
                    logger.warning(f"解密实盘凭证失败: {e}")
                    raise HTTPException(status_code=400, detail="解密实盘凭证失败，主保护密码不正确！")

                rest_client.update_credentials(dec_key, dec_sec, dec_pass, is_live_mode=True)
                ws_manager.update_credentials(dec_key, dec_sec, dec_pass)
            else:
                rest_client.is_live_mode = True

            ws_manager.set_live_mode(True)
            ws_manager.latest_positions.clear()

            # 尝试拉取实盘真实资产与持仓并广播给前端
            try:
                live_bal = await rest_client.get_account_balance()
                live_pos = await rest_client.get_positions(inst_type=None, include_spot=True)
                for p in live_pos:
                    ws_manager.latest_positions[f"{p.symbol}:{p.pos_side}"] = p
                await ws_manager.broadcast_to_subscribers(WSPushPayload[AccountBalanceModel](
                    channel="account",
                    event=WSServerEvent.UPDATE,
                    data=live_bal,
                    timestamp=now_ms
                ))
                await ws_manager.broadcast_to_subscribers(WSPushPayload[List[PositionModel]](
                    channel="positions",
                    event=WSServerEvent.SNAPSHOT,
                    data=live_pos,
                    timestamp=now_ms
                ))
            except Exception as sync_err:
                logger.warning(f"切换实盘后同步真实资产异常: {sync_err}")
                await ws_manager.broadcast_to_subscribers(WSPushPayload[List[PositionModel]](
                    channel="positions",
                    event=WSServerEvent.SNAPSHOT,
                    data=[],
                    timestamp=now_ms
                ))
        else:
            # 切换至模拟沙盒
            rest_client.is_live_mode = False
            ws_manager.set_live_mode(False)
            ws_manager.latest_positions.clear()
            paper_pos = paper_engine.get_positions()
            for p in paper_pos:
                ws_manager.latest_positions[f"{p.symbol}:{p.pos_side}"] = p
            await ws_manager.broadcast_to_subscribers(WSPushPayload[AccountBalanceModel](
                channel="account",
                event=WSServerEvent.UPDATE,
                data=paper_engine.get_account_balance(),
                timestamp=now_ms
            ))
            await ws_manager.broadcast_to_subscribers(WSPushPayload[List[PositionModel]](
                channel="positions",
                event=WSServerEvent.SNAPSHOT,
                data=paper_pos,
                timestamp=now_ms
            ))

        mode_name = "【真实实盘】" if req.target_live_mode else "【模拟操盘沙盒】"
        logger.info(f"系统运行模式已切换为: {mode_name}")

        # 联动自动操盘中枢切换环境并安全暂停原环境
        current_eq = None
        if req.target_live_mode:
            if 'live_bal' in locals() and live_bal:
                current_eq = live_bal.total_equity_usdt
        else:
            current_eq = paper_engine.get_account_balance().total_equity_usdt

        ap_status = await auto_pilot_manager.switch_environment(
            target_live_mode=req.target_live_mode,
            current_equity=current_eq
        )
        await ws_manager.broadcast_to_subscribers(WSPushPayload[AutoPilotStatusModel](
            channel="auto_pilot",
            event=WSServerEvent.UPDATE,
            data=ap_status,
            timestamp=now_ms
        ))

        return ApiResponse(
            code=0,
            msg=f"已成功切换为 {mode_name} 模式",
            data={"is_live_mode": req.target_live_mode},
            timestamp=now_ms
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"切换运行模式发生未知异常: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"切换模式失败: {str(e)}")

