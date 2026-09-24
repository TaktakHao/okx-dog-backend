"""
OKX-Dog 个人量化智能交易系统 - 后台任务守护协程
模块: okx-dog-backend/core/background_tasks.py
管理自动操盘收益采样时序后台监听协程与生命周期
"""

import asyncio
import logging
from state import rest_client, paper_engine, auto_pilot_manager

logger = logging.getLogger("okx_dog.tasks")


async def auto_pilot_snapshot_loop():
    """
    自动操盘收益采样后台任务：
    每 5 秒定期提取当前权益、浮动盈亏与已实现盈亏，注入 auto_pilot_manager 生成时序收益曲线点
    """
    logger.info("【自动操盘中枢】收益时序采样后台监听协程已启动")
    while True:
        try:
            await asyncio.sleep(5)
            is_live = rest_client.is_live_mode and bool(rest_client.api_key)
            if is_live:
                try:
                    bal = await rest_client.get_account_balance()
                    cur_eq = bal.total_equity_usdt
                    unrealized_pnl = bal.unrealized_pnl_usdt
                    realized_pnl = bal.today_realized_pnl_usdt
                except Exception:
                    continue
            else:
                bal = paper_engine.get_account_balance()
                cur_eq = bal.total_equity_usdt
                unrealized_pnl = bal.unrealized_pnl_usdt
                realized_pnl = bal.today_realized_pnl_usdt

            await auto_pilot_manager.record_snapshot(
                current_equity=cur_eq,
                realized_pnl=realized_pnl,
                unrealized_pnl=unrealized_pnl,
            )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"收益采样微循环轻度异常: {e}")


async def auto_pilot_worker_loop():
    """
    7x24h 脱机全自动量化操盘守护协程：
    每 15 秒自主触发多标的池巡检、持仓移动保本、超时挂单清理与自动量化决策
    完全独立于前端运行，即使前端页面关闭依然保持全天候自动化
    """
    logger.info("【自动操盘中枢】7x24h 脱机自主巡检守护协程已启动")
    while True:
        try:
            await asyncio.sleep(15)
            if auto_pilot_manager.is_running:
                await auto_pilot_manager.execute_patrol_step()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"【自动操盘中枢】常驻巡检异常: {e}")


async def daily_review_and_macro_watch_loop():
    """
    每日盘后自动复盘与宏观事件风险感知守护协程：
    1. 持续监控全球宏观经济事件，提前 15 分钟向 TG / 前端发出高危插针预警
    2. 每日跨日 (UTC 00:05) 全自动触发每日复盘报告生成，并沉淀避坑经验
    """
    from state import daily_review_service, news_service, telegram_bot_service
    import datetime

    logger.info("【量化大脑】每日盘后复盘与宏观日历事件守护协程已启动")
    last_reviewed_date = None

    while True:
        try:
            await asyncio.sleep(30)
            now_utc = datetime.datetime.utcnow()
            today_str = now_utc.strftime("%Y-%m-%d")

            # 1. 每日 UTC 00:05 自动触发复盘
            if now_utc.hour == 0 and now_utc.minute >= 5 and last_reviewed_date != today_str:
                logger.info(f"【自动复盘触发】正在生成 [{today_str}] 每日盘后复盘深度报告...")
                try:
                    rev = await daily_review_service.generate_daily_review(today_str)
                    last_reviewed_date = today_str
                    # 推送 TG 战报
                    await telegram_bot_service.send_daily_review_report(
                        date_str=rev.review_date,
                        pnl=rev.realized_pnl,
                        win_rate=rev.win_rate,
                        lessons=rev.lessons_learned
                    )
                except Exception as rev_err:
                    logger.error(f"每日自动复盘执行异常: {rev_err}")

            # 2. 检查宏观事件高危预警
            try:
                macro_resp = await news_service.get_macro_calendar()
                for ev in macro_resp.events:
                    if ev.is_imminent_risk:
                        logger.warning(f"【宏观事件高危预警】{ev.title} 即将在 15 分钟内公布，请注意防范插针！")
            except Exception:
                pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"宏观与复盘监控微循环轻度异常: {e}")


async def reconciliation_worker_loop():
    """
    60秒常驻自动化对账中枢守护协程：
    确立交易所为 Remote SSOT 唯一事实源，自动捕获持仓/挂单/资金漂移并闭环自愈
    """
    from state import reconciliation_service
    await reconciliation_service.run_forever()


