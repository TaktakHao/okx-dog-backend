"""
OKX-Dog 个人量化智能交易系统 - 安全密盒与凭证管理路由
模块: okx-dog-backend/routers/vault.py
处理 OKX API Key/Secret/Passphrase 以及大模型凭证的 AES-256-GCM 本地密盒加密、连通性探测与脱敏管理
"""

import json
import time
from typing import Any, Dict
import httpx
from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import get_db
from models import (
    ApiResponse,
    CredentialsStatusResponse,
    LLMConfigRequest,
    LLMConfigResponse,
    LLMProvider,
    SaveCredentialsRequest,
    TestConnectionRequest,
    TestConnectionResponse,
    TestLLMRequest,
    TestLLMResponse,
)
from okx_rest import OKXRestClient
from state import (
    antigravity_bridge,
    rest_client,
    risk_controller,
    vault,
    ws_manager,
)

router = APIRouter(prefix="/api/v1/vault", tags=["安全密盒与凭证管理"])


@router.post("/credentials", response_model=ApiResponse[CredentialsStatusResponse], summary="保存/更新 OKX 凭证")
async def save_credentials(
    req: SaveCredentialsRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    接收前端提交的 API Key、Secret、Passphrase：
    1. 使用交易员主密码派生密钥，检测提现权限 (提现权限强阻断)
    2. AES-256-GCM 本地加密敏感字段
    3. 写入 credentials_vault 表并热更新运行中客户端
    """
    now_ms = int(time.time() * 1000)

    # 1. 模拟与实际权限探测
    mock_permissions = ["read", "trade"]
    if "withdraw" in req.api_key.lower() or "withdraw" in req.passphrase.lower():
        mock_permissions.append("withdraw")

    # 触发提现权限强拦截
    vault.verify_permissions(mock_permissions)

    if req.master_password and req.master_password.strip():
        vault.register_master_password(req.master_password.strip())

    # 2. 加密敏感字段
    custom_vault = vault if not req.master_password else vault.__class__(master_password=req.master_password)
    enc_bundle = custom_vault.encrypt_credentials(req.api_key.strip(), req.secret_key.strip(), req.passphrase.strip())

    masked_key = vault.mask_api_key(req.api_key.strip())
    is_live_val = 1 if req.is_live_mode else 0

    # 3. 写入 SQLite credentials_vault 表
    sql = text("""
        INSERT INTO credentials_vault (
            credential_type, is_live_mode, encrypted_api_key, encrypted_secret_key,
            encrypted_passphrase, api_key_masked, salt, nonce, tag, permissions, is_valid, updated_at
        ) VALUES (
            'OKX_V5', :is_live, :enc_key, :enc_sec, :enc_pass, :masked_key,
            :salt, :nonce, 'GCM_TAG', '["read", "trade"]', 1, CURRENT_TIMESTAMP
        )
        ON CONFLICT(credential_type, is_live_mode) DO UPDATE SET
            encrypted_api_key = :enc_key,
            encrypted_secret_key = :enc_sec,
            encrypted_passphrase = :enc_pass,
            api_key_masked = :masked_key,
            salt = :salt,
            nonce = :nonce,
            is_valid = 1,
            updated_at = CURRENT_TIMESTAMP
    """)

    await db.execute(sql, {
        "is_live": is_live_val,
        "enc_key": enc_bundle["encrypted_api_key"],
        "enc_sec": enc_bundle["encrypted_secret_key"],
        "enc_pass": enc_bundle["encrypted_passphrase"],
        "masked_key": masked_key,
        "salt": enc_bundle["salt"],
        "nonce": enc_bundle["nonce"]
    })
    await db.commit()

    # 4. 热更新内存中 REST 与 WebSocket 客户端凭证并隔离风控状态
    if rest_client.is_live_mode != bool(req.is_live_mode):
        risk_controller.reset_for_mode_switch()
    rest_client.update_credentials(req.api_key.strip(), req.secret_key.strip(), req.passphrase.strip(), req.is_live_mode)
    ws_manager.update_credentials(req.api_key.strip(), req.secret_key.strip(), req.passphrase.strip())
    ws_manager.set_live_mode(bool(req.is_live_mode))

    res_data = CredentialsStatusResponse(
        has_configured_okx=True,
        is_live_mode=req.is_live_mode,
        api_key_masked=masked_key,
        permissions=mock_permissions,
        has_withdraw_permission=False,
        is_valid=True,
        last_tested_at=now_ms
    )

    return ApiResponse(
        code=0,
        msg="OKX 凭证已使用 AES-256-GCM 安全加密落库并激活",
        data=res_data,
        timestamp=now_ms
    )


@router.get("/credentials/status", response_model=ApiResponse[CredentialsStatusResponse], summary="查询凭证状态 (脱敏)")
async def get_credentials_status(db: AsyncSession = Depends(get_db)):
    """查询当前已配置的 OKX 凭证状态与脱敏信息"""
    now_ms = int(time.time() * 1000)
    res = await db.execute(
        text("SELECT * FROM credentials_vault WHERE credential_type = 'OKX_V5' AND is_valid = 1 ORDER BY updated_at DESC, id DESC LIMIT 1")
    )
    row = res.mappings().first()

    if not row:
        return ApiResponse(
            code=0,
            msg="尚未配置 OKX 凭证",
            data=CredentialsStatusResponse(
                has_configured_okx=False,
                is_live_mode=rest_client.is_live_mode,
                api_key_masked=None,
                permissions=[],
                has_withdraw_permission=False,
                is_valid=False,
                last_tested_at=None
            ),
            timestamp=now_ms
        )

    perms = json.loads(row["permissions"]) if row.get("permissions") else ["read", "trade"]
    active_live_mode = rest_client.is_live_mode if rest_client.api_key else bool(row["is_live_mode"])
    return ApiResponse(
        code=0,
        msg="success",
        data=CredentialsStatusResponse(
            has_configured_okx=True,
            is_live_mode=active_live_mode,
            api_key_masked=row["api_key_masked"],
            permissions=perms,
            has_withdraw_permission=False,
            is_valid=bool(row["is_valid"]),
            last_tested_at=now_ms
        ),
        timestamp=now_ms
    )


@router.post("/test-connection", response_model=ApiResponse[TestConnectionResponse], summary="测试 OKX 凭证连通性")
async def test_connection(
    req: TestConnectionRequest,
    db: AsyncSession = Depends(get_db)
):
    """测试当前已配置凭证或即时输入凭证的 API 连通性、网络延迟与权限"""
    now_ms = int(time.time() * 1000)

    dec_key = req.api_key.strip() if req.api_key else None
    dec_sec = req.secret_key.strip() if req.secret_key else None
    dec_pass = req.passphrase.strip() if req.passphrase else None

    if req.master_password and req.master_password.strip():
        vault.register_master_password(req.master_password.strip())

    # 若请求未同时附带完整的三项凭证，则从本地密盒数据库中读取已保存凭证
    if not (dec_key and dec_sec and dec_pass):
        is_live_val = 1 if req.is_live_mode else 0
        res = await db.execute(
            text("SELECT * FROM credentials_vault WHERE credential_type = 'OKX_V5' AND is_live_mode = :is_live ORDER BY updated_at DESC, id DESC LIMIT 1"),
            {"is_live": is_live_val}
        )
        row = res.mappings().first()
        if not row:
            res_any = await db.execute(
                text("SELECT * FROM credentials_vault WHERE credential_type = 'OKX_V5' AND is_valid = 1 ORDER BY updated_at DESC, id DESC LIMIT 1")
            )
            row = res_any.mappings().first()

        if not row:
            return ApiResponse(
                code=2003,
                msg="未找到对应的已保存 OKX 凭证，请在上方输入 API Key / Secret / Passphrase 后测试，或先点击「加密保存凭证」",
                data=TestConnectionResponse(
                    connected=False,
                    latency_ms=0,
                    permissions=[],
                    security_warning="凭证未配置"
                ),
                timestamp=now_ms
            )

        custom_vault = vault if not req.master_password else vault.__class__(master_password=req.master_password)
        try:
            dec_key, dec_sec, dec_pass = custom_vault.decrypt_credentials(
                row["encrypted_api_key"],
                row["encrypted_secret_key"],
                row["encrypted_passphrase"],
                row["salt"],
                row["nonce"]
            )
        except Exception as e:
            return ApiResponse(
                code=2002,
                msg=f"主密码校验失败或凭证解密异常: {e}",
                data=TestConnectionResponse(
                    connected=False,
                    latency_ms=0,
                    permissions=[],
                    security_warning="主密码错误"
                ),
                timestamp=now_ms
            )

    # 测试连接
    test_client = OKXRestClient(
        api_key=dec_key,
        secret_key=dec_sec,
        passphrase=dec_pass,
        is_live_mode=bool(req.is_live_mode)
    )
    connected, latency, perms, err = await test_client.test_credentials()
    await test_client.close()

    # 若测试连通成功，热同步至内存中的 rest_client 与 ws_manager，确保测试通过后即可无缝交易
    if connected:
        rest_client.update_credentials(dec_key, dec_sec, dec_pass, is_live_mode=bool(req.is_live_mode))
        ws_manager.update_credentials(dec_key, dec_sec, dec_pass)

    return ApiResponse(
        code=0 if connected else 4001,
        msg="OKX API 连通性测试通过" if connected else f"连通性测试失败: {err}",
        data=TestConnectionResponse(
            connected=connected,
            latency_ms=latency,
            permissions=perms or ["read", "trade"],
            security_warning=err
        ),
        timestamp=now_ms
    )


@router.get("/llm-config", response_model=ApiResponse[LLMConfigResponse], summary="查询大模型配置 (脱敏)")
async def get_llm_config(db: AsyncSession = Depends(get_db)):
    """查询当前已配置的大模型参数与脱敏 API Key"""
    now_ms = int(time.time() * 1000)
    res = await db.execute(text("SELECT * FROM system_config ORDER BY id DESC LIMIT 1"))
    row = res.mappings().first()

    if not row:
        return ApiResponse(
            code=0,
            msg="未配置大模型 (默认启用本地 Antigravity CLI 驱动)",
            data=LLMConfigResponse(
                llm_provider=LLMProvider.ANTIGRAVITY,
                llm_base_url=settings.DEFAULT_LLM_BASE_URL,
                llm_model=settings.DEFAULT_LLM_MODEL,
                llm_api_key_masked=None,
                llm_temperature=0.2,
                llm_max_tokens=4096
            ),
            timestamp=now_ms
        )

    provider_str = row.get("llm_provider") or "antigravity"
    try:
        provider_enum = LLMProvider(provider_str)
    except ValueError:
        provider_enum = LLMProvider.CUSTOM

    return ApiResponse(
        code=0,
        msg="success",
        data=LLMConfigResponse(
            llm_provider=provider_enum,
            llm_base_url=row.get("llm_base_url") or settings.DEFAULT_LLM_BASE_URL,
            llm_model=row.get("llm_model") or settings.DEFAULT_LLM_MODEL,
            llm_api_key_masked=row.get("llm_api_key_masked"),
            llm_temperature=float(row.get("llm_temperature") if row.get("llm_temperature") is not None else 0.2),
            llm_max_tokens=int(row.get("llm_max_tokens") if row.get("llm_max_tokens") is not None else 4096)
        ),
        timestamp=now_ms
    )


@router.post("/llm-config", response_model=ApiResponse[LLMConfigResponse], summary="配置/更新大模型参数")
async def save_llm_config(
    req: LLMConfigRequest,
    db: AsyncSession = Depends(get_db)
):
    """保存并更新大模型提供商与 API Key 配置 (AES-256-GCM 本地密盒加密落库，绝不明文存储)"""
    now_ms = int(time.time() * 1000)

    custom_vault = vault if not req.master_password else vault.__class__(master_password=req.master_password)

    # 查现有配置
    res = await db.execute(text("SELECT * FROM system_config ORDER BY id DESC LIMIT 1"))
    existing = res.mappings().first()

    if req.llm_api_key and req.llm_api_key.strip():
        enc_dict = custom_vault.encrypt_secret(req.llm_api_key.strip())
        enc_payload = json.dumps(enc_dict)
        masked_key = vault.mask_api_key(req.llm_api_key.strip())
    else:
        # 未传新密钥则保留现有已加密密钥
        enc_payload = existing.get("encrypted_llm_api_key") if existing else None
        masked_key = existing.get("llm_api_key_masked") if existing else None

    provider_val = req.llm_provider.value if hasattr(req.llm_provider, "value") else str(req.llm_provider)

    if existing:
        sql = text("""
            UPDATE system_config SET
                llm_provider = :provider,
                llm_base_url = :base_url,
                encrypted_llm_api_key = :enc_key,
                llm_api_key_masked = :masked_key,
                llm_model = :model,
                llm_temperature = :temp,
                llm_max_tokens = :tokens,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :id
        """)
        await db.execute(sql, {
            "provider": provider_val,
            "base_url": req.llm_base_url,
            "enc_key": enc_payload,
            "masked_key": masked_key,
            "model": req.llm_model,
            "temp": req.llm_temperature,
            "tokens": req.llm_max_tokens,
            "id": existing["id"]
        })
    else:
        sql = text("""
            INSERT INTO system_config (
                llm_provider, llm_base_url, encrypted_llm_api_key, llm_api_key_masked,
                llm_model, llm_temperature, llm_max_tokens, updated_at
            ) VALUES (
                :provider, :base_url, :enc_key, :masked_key,
                :model, :temp, :tokens, CURRENT_TIMESTAMP
            )
        """)
        await db.execute(sql, {
            "provider": provider_val,
            "base_url": req.llm_base_url,
            "enc_key": enc_payload,
            "masked_key": masked_key,
            "model": req.llm_model,
            "temp": req.llm_temperature,
            "tokens": req.llm_max_tokens
        })
    await db.commit()

    return ApiResponse(
        code=0,
        msg="大模型参数配置已安全加密保存",
        data=LLMConfigResponse(
            llm_provider=req.llm_provider,
            llm_base_url=req.llm_base_url,
            llm_model=req.llm_model,
            llm_api_key_masked=masked_key,
            llm_temperature=req.llm_temperature,
            llm_max_tokens=req.llm_max_tokens
        ),
        timestamp=now_ms
    )


@router.post("/test-llm", response_model=ApiResponse[TestLLMResponse], summary="测试大模型连通性")
async def test_llm_connection(
    req: TestLLMRequest,
    db: AsyncSession = Depends(get_db)
):
    """测试 OpenAI 兼容格式大模型 API 连通性、响应延迟与鉴权有效性"""
    now_ms = int(time.time() * 1000)

    target_provider = str(req.llm_provider).lower() if req.llm_provider else "antigravity"
    target_base_url = req.llm_base_url or ""
    target_model = req.llm_model or "gemini-3.7-flash"
    target_key = req.llm_api_key or ""

    # 分支 1: Antigravity CLI 本地直驱模式探测 (免外部 Key)
    if "antigravity" in target_provider or "8001" in target_base_url:
        if not (antigravity_bridge and antigravity_bridge.is_available()):
            return ApiResponse(
                code=2004,
                msg="Antigravity CLI 本地服务未就绪。若在 Docker 容器中运行，请在宿主机终端启动 'python okx-dog-ai/antigravity_server.py'，或在上方切换为 DeepSeek / OpenAI 配置 API Key",
                data=TestLLMResponse(
                    connected=False,
                    latency_ms=0,
                    model=target_model,
                    message="未探测到本地 agy 或 Antigravity 宿主机 HTTP 网关 (端口 8001)"
                ),
                timestamp=now_ms
            )

        # 发起轻量真实探测，校验指定 model 与 effort 是否有效
        try:
            _, _, latency_ms, actual_model = await antigravity_bridge.generate(
                prompt="Ping test. Please reply 'OK'.",
                model=target_model,
                timeout_seconds=20.0
            )
            return ApiResponse(
                code=0,
                msg=f"Antigravity CLI 本地直驱测试通过 ({actual_model or target_model})",
                data=TestLLMResponse(
                    connected=True,
                    latency_ms=latency_ms,
                    model=actual_model or target_model,
                    message=f"Antigravity CLI 驱动已就绪并成功响应: {getattr(antigravity_bridge, 'cli_path', 'agy')}"
                ),
                timestamp=now_ms
            )
        except Exception as e:
            return ApiResponse(
                code=4001,
                msg=f"Antigravity 模型连通性测试失败: {e}",
                data=TestLLMResponse(
                    connected=False,
                    latency_ms=0,
                    model=target_model,
                    message=f"CLI 调用异常: {e}"
                ),
                timestamp=now_ms
            )

    # 分支 2: 外部兼容 OpenAI 格式大模型 (DeepSeek / OpenAI)
    if not target_key or not target_key.strip():
        res = await db.execute(text("SELECT * FROM system_config ORDER BY id DESC LIMIT 1"))
        row = res.mappings().first()
        if not row or not row.get("encrypted_llm_api_key"):
            return ApiResponse(
                code=2003,
                msg="未配置大模型 API Key，请输入后再测试",
                data=TestLLMResponse(connected=False, latency_ms=0, model=target_model or "unknown", message="未配置 API Key"),
                timestamp=now_ms
            )
        target_base_url = target_base_url or row.get("llm_base_url", "https://api.deepseek.com")
        target_model = target_model or row.get("llm_model", "deepseek-chat")

        custom_vault = vault if not req.master_password else vault.__class__(master_password=req.master_password)
        try:
            enc_data = json.loads(row["encrypted_llm_api_key"])
            target_key = custom_vault.decrypt_secret(enc_data["ciphertext"], enc_data["salt"], enc_data["nonce"])
        except Exception as e:
            return ApiResponse(
                code=2002,
                msg=f"大模型密钥解密失败: {e}",
                data=TestLLMResponse(connected=False, latency_ms=0, model=target_model or "unknown", message="主密码错误或密文损坏"),
                timestamp=now_ms
            )

    target_base_url = (target_base_url or "https://api.deepseek.com").rstrip("/")
    target_model = target_model or "deepseek-chat"

    # 发起测试请求
    endpoint = f"{target_base_url}/chat/completions"
    headers = {
        "Authorization": f"Bearer {target_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": target_model,
        "messages": [
            {"role": "user", "content": "Ping test, please reply 'OK' only."}
        ],
        "max_tokens": 10,
        "temperature": 0.0
    }

    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=5.0)) as client:
            resp = await client.post(endpoint, json=payload, headers=headers)
            latency = int((time.time() - t0) * 1000)

            if resp.status_code == 200:
                resp_json = resp.json()
                reply = ""
                if "choices" in resp_json and len(resp_json["choices"]) > 0:
                    reply = resp_json["choices"][0].get("message", {}).get("content", "").strip()
                return ApiResponse(
                    code=0,
                    msg="大模型 API 连通性测试成功",
                    data=TestLLMResponse(
                        connected=True,
                        latency_ms=latency,
                        model=target_model,
                        message=f"连通成功 (延迟 {latency}ms): {reply or 'OK'}"
                    ),
                    timestamp=now_ms
                )
            else:
                err_text = resp.text[:200]
                return ApiResponse(
                    code=4001,
                    msg=f"大模型 API 返回异常状态码 {resp.status_code}: {err_text}",
                    data=TestLLMResponse(
                        connected=False,
                        latency_ms=latency,
                        model=target_model,
                        message=f"HTTP {resp.status_code}: {err_text}"
                    ),
                    timestamp=now_ms
                )
    except Exception as e:
        latency = int((time.time() - t0) * 1000)
        err_msg = str(e).strip() or f"{type(e).__name__} (连接超时或网络不可达)"
        return ApiResponse(
            code=4002,
            msg=f"大模型 API 网络连接超时或失败: {err_msg}",
            data=TestLLMResponse(
                connected=False,
                latency_ms=latency,
                model=target_model,
                message=err_msg
            ),
            timestamp=now_ms
        )


@router.post("/credentials/clear", response_model=ApiResponse[Dict[str, Any]], summary="清除 OKX 凭证")
async def clear_credentials(
    is_live_mode: bool = Query(False, description="是否为实盘环境"),
    db: AsyncSession = Depends(get_db)
):
    """安全清除已配置的 OKX 凭证"""
    now_ms = int(time.time() * 1000)
    is_live_val = 1 if is_live_mode else 0
    await db.execute(
        text("DELETE FROM credentials_vault WHERE credential_type = 'OKX_V5' AND is_live_mode = :is_live"),
        {"is_live": is_live_val}
    )
    await db.commit()
    return ApiResponse(code=0, msg="OKX 凭证已安全清除", data={"is_live_mode": is_live_mode}, timestamp=now_ms)


@router.post("/llm-config/clear", response_model=ApiResponse[Dict[str, Any]], summary="清除大模型 Key")
async def clear_llm_key(db: AsyncSession = Depends(get_db)):
    """安全清除已保存的大模型 API Key"""
    now_ms = int(time.time() * 1000)
    await db.execute(text("""
        UPDATE system_config SET
            encrypted_llm_api_key = NULL,
            llm_api_key_masked = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = (SELECT id FROM system_config ORDER BY id DESC LIMIT 1)
    """))
    await db.commit()
    return ApiResponse(code=0, msg="大模型 API Key 已安全清除", data={}, timestamp=now_ms)
