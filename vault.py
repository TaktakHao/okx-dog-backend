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


import re
import logging

logger = logging.getLogger("okx_dog.vault")


class SensitiveDataFilter(logging.Filter):
    """
    敏感数据自动脱敏日志过滤器 (Log Sanitizer)
    自动拦截并脱敏 OKX-API-KEY, OKX-PASSPHRASE, Secret Key, Signature 等
    """
    PATTERNS = [
        (re.compile(r'(OKX-API-KEY["\']?\s*[:=]\s*["\']?)([^"\'\s,]{4})([^"\'\s,]+)([^"\'\s,]{4})', re.I), r'\1\2****\4'),
        (re.compile(r'(OKX-PASSPHRASE["\']?\s*[:=]\s*["\']?)([^"\'\s,]+)', re.I), r'\1****'),
        (re.compile(r'(OKX-SIGN["\']?\s*[:=]\s*["\']?)([^"\'\s,]+)', re.I), r'\1****'),
        (re.compile(r'(secret_key["\']?\s*[:=]\s*["\']?)([^"\'\s,]+)', re.I), r'\1****'),
        (re.compile(r'(passphrase["\']?\s*[:=]\s*["\']?)([^"\'\s,]+)', re.I), r'\1****'),
        (re.compile(r'(api_key["\']?\s*[:=]\s*["\']?)([^"\'\s,]{4})([^"\'\s,]+)([^"\'\s,]{4})', re.I), r'\1\2****\4'),
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            for pattern, repl in self.PATTERNS:
                record.msg = pattern.sub(repl, record.msg)
        if record.args:
            if isinstance(record.args, dict):
                clean_args = {}
                for k, v in record.args.items():
                    if isinstance(v, str):
                        for pattern, repl in self.PATTERNS:
                            v = pattern.sub(repl, v)
                    clean_args[k] = v
                record.args = clean_args
            elif isinstance(record.args, tuple):
                clean_args = []
                for v in record.args:
                    if isinstance(v, str):
                        for pattern, repl in self.PATTERNS:
                            v = pattern.sub(repl, v)
                    clean_args.append(v)
                record.args = tuple(clean_args)
        return True


def install_sensitive_data_filter():
    """将敏感数据脱敏过滤器挂载至全局 Root Logger 与主流 Logger"""
    sanitizer = SensitiveDataFilter()
    logging.getLogger().addFilter(sanitizer)
    for name in ["okx_dog", "uvicorn", "uvicorn.access", "uvicorn.error", "httpx"]:
        logging.getLogger(name).addFilter(sanitizer)


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
    - 严禁硬编码弱默认口令，仅使用用户配置及运行时显式注册的主密码
    """
    # 运行时动态认证通过的活跃密码集合 (不包含任何硬编码弱口令)
    _active_passwords: set = set()

    def __init__(self, master_password: Optional[str] = None, iterations: int = 100_000):
        raw_pwd = master_password or settings.MASTER_PASSWORD
        self.master_password = raw_pwd.encode("utf-8") if raw_pwd else b""
        self.iterations = iterations
        if raw_pwd:
            self._active_passwords.add(raw_pwd.strip())

    @classmethod
    def register_master_password(cls, password: str):
        """记录用户验证通过的主密码供系统后台解密"""
        if password and password.strip():
            cls._active_passwords.add(password.strip())

    def _derive_key_from_pwd(self, pwd_bytes: bytes, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=self.iterations,
            backend=default_backend()
        )
        return kdf.derive(pwd_bytes)

    def _derive_key(self, salt: bytes) -> bytes:
        """基于主密码与随机盐派生 256 位 (32 字节) 对称加密密钥"""
        return self._derive_key_from_pwd(self.master_password, salt)


    def encrypt_credentials(self, api_key: str, secret_key: str, passphrase: str) -> Dict[str, str]:
        """
        原子加密完整 OKX API 凭证三件套 (API Key, Secret Key, Passphrase)
        - 共享 1 个随机派生盐 (16 字节)
        - 采用基于主 Nonce 的确定性子 Nonce 派生，既杜绝 GCM Nonce 重用，又能通过单个 salt/nonce 完整持久化并精准解密
        """
        if not api_key or not secret_key or not passphrase:
            raise ValueError("API Key / Secret Key / Passphrase 均不能为空")

        salt = os.urandom(16)
        key = self._derive_key(salt)
        aesgcm = AESGCM(key)

        import hashlib
        nonce = os.urandom(12)
        nonce_sec = hashlib.sha256(nonce + b":secret_key").digest()[:12]
        nonce_pass = hashlib.sha256(nonce + b":passphrase").digest()[:12]

        enc_key = aesgcm.encrypt(nonce, api_key.encode("utf-8"), None)
        enc_sec = aesgcm.encrypt(nonce_sec, secret_key.encode("utf-8"), None)
        enc_pass = aesgcm.encrypt(nonce_pass, passphrase.encode("utf-8"), None)

        return {
            "encrypted_api_key": base64.b64encode(enc_key).decode("utf-8"),
            "encrypted_secret_key": base64.b64encode(enc_sec).decode("utf-8"),
            "encrypted_passphrase": base64.b64encode(enc_pass).decode("utf-8"),
            "salt": base64.b64encode(salt).decode("utf-8"),
            "nonce": base64.b64encode(nonce).decode("utf-8"),
        }

    def decrypt_credentials(
        self,
        encrypted_api_key: str,
        encrypted_secret_key: str,
        encrypted_passphrase: str,
        salt_b64: str,
        nonce_b64: str
    ) -> Tuple[str, str, str]:
        """
        解密 OKX API 凭证三件套 (支持多候选主密码自适应)
        :return: (api_key, secret_key, passphrase)
        """
        import hashlib
        salt = base64.b64decode(salt_b64)
        nonce = base64.b64decode(nonce_b64)
        nonce_sec = hashlib.sha256(nonce + b":secret_key").digest()[:12]
        nonce_pass = hashlib.sha256(nonce + b":passphrase").digest()[:12]

        raw_k = base64.b64decode(encrypted_api_key)
        raw_s = base64.b64decode(encrypted_secret_key)
        raw_p = base64.b64decode(encrypted_passphrase)

        candidate_pwds = set(self._active_passwords)
        if self.master_password:
            candidate_pwds.add(self.master_password.decode("utf-8", "ignore"))

        for pwd in candidate_pwds:
            if not pwd:
                continue
            try:
                key = self._derive_key_from_pwd(pwd.encode("utf-8"), salt)
                aesgcm = AESGCM(key)

                # 方式 1: 派生 Nonce 解密
                try:
                    dec_k = aesgcm.decrypt(nonce, raw_k, None).decode("utf-8")
                    dec_s = aesgcm.decrypt(nonce_sec, raw_s, None).decode("utf-8")
                    dec_p = aesgcm.decrypt(nonce_pass, raw_p, None).decode("utf-8")
                    self.register_master_password(pwd)
                    return dec_k, dec_s, dec_p
                except Exception:
                    pass

                # 方式 2: 同 Nonce 解密兼容
                try:
                    dec_k = aesgcm.decrypt(nonce, raw_k, None).decode("utf-8")
                    dec_s = aesgcm.decrypt(nonce, raw_s, None).decode("utf-8")
                    dec_p = aesgcm.decrypt(nonce, raw_p, None).decode("utf-8")
                    self.register_master_password(pwd)
                    return dec_k, dec_s, dec_p
                except Exception:
                    pass
            except Exception:
                continue

        raise SecurityException("主保护密码错误或凭证数据已被篡改")

    def encrypt_secret(self, plaintext: str) -> Dict[str, str]:
        """
        加密单个明文敏感字段 (如 LLM API Key)
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
        解密单个敏感字段 (如 LLM API Key，支持多候选主密码自适应)
        :param ciphertext_b64: Base64 编码的密文
        :param salt_b64: Base64 编码的派生盐
        :param nonce_b64: Base64 编码的 Nonce
        :return: 解密后的明文字符串
        """
        salt = base64.b64decode(salt_b64)
        nonce = base64.b64decode(nonce_b64)
        ciphertext = base64.b64decode(ciphertext_b64)

        candidate_pwds = set(self._active_passwords)
        if self.master_password:
            candidate_pwds.add(self.master_password.decode("utf-8", "ignore"))

        for pwd in candidate_pwds:
            if not pwd:
                continue
            try:
                key = self._derive_key_from_pwd(pwd.encode("utf-8"), salt)
                aesgcm = AESGCM(key)
                decrypted_bytes = aesgcm.decrypt(nonce, ciphertext, None)
                self.register_master_password(pwd)
                return decrypted_bytes.decode("utf-8")
            except Exception:
                continue

        raise SecurityException("密盒凭证解密失败，主密码错误或密文已被篡改")



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
