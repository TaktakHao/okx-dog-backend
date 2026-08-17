"""
OKX-Dog 个人量化智能交易系统 - 本地凭证安全密盒模块
模块: okx-dog-backend/vault.py
实现 AES-256-GCM 凭证加密、PBKDF2 密钥派生、API Key 脱敏掩码与提现权限强阻断
"""

import os
import base64
import json
from typing import Tuple, Dict, Any, List, Optional
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

from config import settings


class SecurityException(Exception):
    """通用安全异常基类"""
    pass


class FatalWithdrawalPermissionException(SecurityException):
    """提现权限致命安全异常：当检测到 API Key 具备提现权限时抛出，系统强制拒绝使用"""
    pass


class CredentialsVault:
    """
    金融级本地凭证密盒
    - 采用 AES-256-GCM 进行对称加密，保障机密性与完整性
    - 采用 PBKDF2-HMAC-SHA256 进行 100,000 次密钥派生
    - 提现权限物理隔离与主动检测
    """

    def __init__(self, master_password: Optional[str] = None, iterations: int = 100_000):
        self.master_password = (master_password or settings.MASTER_PASSWORD).encode("utf-8")
        self.iterations = iterations

    def _derive_key(self, salt: bytes) -> bytes:
        """基于主密码与随机盐派生 256 位 (32 字节) 对称加密密钥"""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=self.iterations,
            backend=default_backend()
        )
        return kdf.derive(self.master_password)

    def encrypt_secret(self, plaintext: str) -> Dict[str, str]:
        """
        加密明文敏感字段
        :param plaintext: 待加密字符串
        :return: 包含密文、salt、nonce 的字典 (均为 Base64 编码)
        """
        if not plaintext:
            raise ValueError("待加密内容不能为空")

        salt = os.urandom(16)
        key = self._derive_key(salt)
        aesgcm = AESGCM(key)
        
        # 96 位 (12 字节) 标准 GCM Nonce
        nonce = os.urandom(12)
        
        # 加密并将 Tag 附在密文末尾 (AESGCM 自动处理)
        ciphertext = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)

        return {
            "ciphertext": base64.b64encode(ciphertext).decode("utf-8"),
            "salt": base64.b64encode(salt).decode("utf-8"),
            "nonce": base64.b64encode(nonce).decode("utf-8")
        }

    def decrypt_secret(self, ciphertext_b64: str, salt_b64: str, nonce_b64: str) -> str:
        """
        解密敏感字段
        :param ciphertext_b64: Base64 编码的密文
        :param salt_b64: Base64 编码的派生盐
        :param nonce_b64: Base64 编码的 Nonce
        :return: 解密后的明文字符串
        """
        try:
            salt = base64.b64decode(salt_b64)
            nonce = base64.b64decode(nonce_b64)
            ciphertext = base64.b64decode(ciphertext_b64)

            key = self._derive_key(salt)
            aesgcm = AESGCM(key)

            decrypted_bytes = aesgcm.decrypt(nonce, ciphertext, None)
            return decrypted_bytes.decode("utf-8")
        except Exception as e:
            raise SecurityException(f"密盒凭证解密失败，主密码错误或密文已被篡改: {str(e)}")

    @staticmethod
    def mask_api_key(api_key: str) -> str:
        """
        生成脱敏 API Key 显示字符串
        例如: "6b2a8d3e-9f1c-4b5a-8e91-123456789abc" -> "6b2a****9abc"
        """
        if not api_key or len(api_key) < 8:
            return "****"
        prefix = api_key[:4]
        suffix = api_key[-4:]
        return f"{prefix}****{suffix}"

    @staticmethod
    def verify_permissions(permissions: List[str]) -> Tuple[bool, str]:
        """
        严格校验 API Key 的权限范围
        【红线】：严禁包含 'withdraw' / 'withdrawal' / 'transfer' 等资金划转权限
        :param permissions: OKX API 权限列表
        :return: (是否安全通过, 诊断信息)
        """
        dangerous_permissions = {"withdraw", "withdrawal", "transfer", "internal_transfer"}
        normalized = {p.strip().lower() for p in permissions}

        hit_dangerous = normalized.intersection(dangerous_permissions)
        if hit_dangerous:
            msg = (
                f"【严重安全警告】检测到该 API Key 开启了资金划转/提现权限 ({list(hit_dangerous)})！"
                f"根据金融级安全规范，系统已强制阻断该凭证接入。请立即在 OKX 控制台关闭提现权限并重新生成只读/交易 Key！"
            )
            raise FatalWithdrawalPermissionException(msg)

        return True, "API Key 权限校验合规 (仅限读取与交易权限)"


# 全局密盒单例
vault = CredentialsVault()
