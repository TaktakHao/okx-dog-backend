"""
OKX-Dog Telegram 移动端全功能指挥官与双向协同网关
模块: okx-dog-backend/services/telegram_bot.py
角色: 后端架构师 (agency-backend-architect) & 风控安全审计师 (agency-blockchain-security-auditor)
功能:
1. 毫秒级开平仓、移动保本、风控熔断图文推流
2. 每日复盘 Markdown 战报推送到 TG
3. 双向交互指令支持: /report, /news, /status, /panic, /adopt
4. 凭证脱敏与安全配置存储
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional
import httpx

from models import TelegramConfig, TelegramConfigSaveRequest

logger = logging.getLogger("okx_dog.telegram")


class TelegramBotService:
    """Telegram Bot 移动端协同服务"""

    def __init__(self):
        self.bot_token: Optional[str] = None
        self.chat_id: Optional[str] = None
        self.is_enabled: bool = False
        self.is_panic_enabled: bool = True
        self._polling_task: Optional[asyncio.Task] = None

    def get_config(self) -> TelegramConfig:
        """获取当前配置 (脱敏)"""
        token_masked = None
        if self.bot_token:
            token_masked = f"{self.bot_token[:6]}****{self.bot_token[-4:]}" if len(self.bot_token) > 10 else "****"

        chat_masked = None
        if self.chat_id:
            chat_masked = f"{self.chat_id[:3]}****{self.chat_id[-2:]}" if len(self.chat_id) > 5 else "****"

        return TelegramConfig(
            bot_token_masked=token_masked,
            chat_id_masked=chat_masked,
            is_enabled=self.is_enabled,
            is_panic_enabled=self.is_panic_enabled
        )

    def save_config(self, req: TelegramConfigSaveRequest) -> TelegramConfig:
        """更新配置"""
        self.bot_token = req.bot_token.strip()
        self.chat_id = req.chat_id.strip()
        self.is_enabled = req.is_enabled
        self.is_panic_enabled = req.is_panic_enabled
        logger.info("Telegram Bot 移动端配置已更新")
        return self.get_config()

    async def send_message(self, text_msg: str, parse_mode: str = "Markdown") -> bool:
        """向 Telegram 发送消息"""
        if not self.is_enabled or not self.bot_token or not self.chat_id:
            logger.debug(f"[TG 未启用或未配置] 忽略推送: {text_msg[:50]}...")
            return False

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text_msg,
            "parse_mode": parse_mode
        }

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    logger.info("Telegram 消息推送成功")
                    return True
                else:
                    logger.warning(f"Telegram 推送失败 (HTTP {resp.status_code}): {resp.text}")
                    return False
        except Exception as e:
            logger.error(f"Telegram 网络连接异常: {e}")
            return False

    async def send_trade_notification(self, action: str, symbol: str, price: float, pnl: Optional[float] = None):
        """推送交易开平仓通知"""
        icon = "🚀" if "BUY" in action.upper() or "LONG" in action.upper() else "📉"
        pnl_str = f"\n💰 **实现盈亏**: `{pnl:+.2f} USDT`" if pnl is not None else ""
        msg = (
            f"{icon} **【OKX-Dog 交易执行】**\n"
            f"- **标的**: `{symbol}`\n"
            f"- **动作**: `{action}`\n"
            f"- **价格**: `{price}`{pnl_str}\n"
            f"⏱️ *{time.strftime('%Y-%m-%d %H:%M:%S')}*"
        )
        await self.send_message(msg)

    async def send_daily_review_report(self, date_str: str, pnl: float, win_rate: float, lessons: List[str]):
        """推送每日复盘简报"""
        lessons_text = "\n".join([f"• {l}" for l in lessons[:3]]) if lessons else "• 保持纪律"
        msg = (
            f"📊 **【OKX-Dog 每日盘后复盘简报 ({date_str})】**\n\n"
            f"• **当日已实现盈亏**: `{pnl:+.2f} USDT`\n"
            f"• **当日胜率**: `{win_rate*100:.1f}%`\n\n"
            f"💡 **核心避坑教训**:\n{lessons_text}\n\n"
            f"👉 详情请登录桌面暗黑终端查看完整 Markdown 战报！"
        )
        await self.send_message(msg)