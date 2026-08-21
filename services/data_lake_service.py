"""
OKX-Dog 个人量化智能交易系统 - 历史时序数据湖与行情归档引擎
模块: okx-dog-backend/services/data_lake_service.py
角色: 后端架构师 (agency-backend-architect)
"""

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from config import BASE_DIR
from database import get_db_session
from models import (
    DataLakeStatusItem,
    DataLakeStatusResponse,
    DataLakeSyncRequest,
    HistoricalKlineRecord,
)
from okx_rest import OKXRestClient

logger = logging.getLogger("okx_dog.data_lake")


class DataLakeService:
    """本地多维时序数据湖服务 (高性能零依赖实现)"""

    def __init__(self, rest_client: OKXRestClient):
        self.rest_client = rest_client
        self.history_dir = BASE_DIR / "data" / "history"
        self.history_dir.mkdir(parents=True, exist_ok=True)
        self._syncing = False

    async def get_data_lake_status(self) -> DataLakeStatusResponse:
        """查询数据湖当前存储状态与覆盖范围"""
        async with get_db_session() as db:
            sql = text("""
                SELECT 
                    symbol, 
                    timeframe, 
                    COUNT(*) as count, 
                    MIN(timestamp) as start_time, 
                    MAX(timestamp) as end_time
                FROM historical_klines
                GROUP BY symbol, timeframe
                ORDER BY symbol ASC, timeframe ASC
            """)
            res = await db.execute(sql)
            rows = res.mappings().all()

            items: List[DataLakeStatusItem] = []
            total_klines = 0
            for r in rows:
                cnt = int(r["count"])
                total_klines += cnt
                items.append(DataLakeStatusItem(
                    symbol=r["symbol"],
                    timeframe=r["timeframe"],
                    count=cnt,
                    start_time=r["start_time"],
                    end_time=r["end_time"]
                ))

            return DataLakeStatusResponse(
                total_klines=total_klines,
                items=items
            )

    async def sync_symbol_history(
        self,
        symbol: str,
        timeframes: List[str],
        days_back: int = 30
    ) -> Dict[str, Any]:
        """全自动拉取指定标的的历史 K 线入库"""
        now_ms = int(time.time() * 1000)
        start_target_ms = now_ms - (days_back * 24 * 60 * 60 * 1000)
        total_inserted = 0
        details: Dict[str, int] = {}

        logger.info(f"开始同步标的 {symbol} 历史数据 (回溯 {days_back} 天, 周期: {timeframes})...")

        for tf in timeframes:
            inserted_for_tf = 0
            bar = tf
            if tf == "1h":
                bar = "1H"
            elif tf == "4h":
                bar = "4H"
            elif tf == "1d":
                bar = "1Dutc"

            cursor_after = None
            reached_target = False

            while not reached_target:
                try:
                    candles = await self.rest_client.get_kline_history(
                        symbol=symbol,
                        bar=bar,
                        limit=100,
                        after=cursor_after
                    )
                    if not candles:
                        break

                    async with get_db_session() as db:
                        for c in candles:
                            ts = int(c.timestamp)
                            if ts < start_target_ms:
                                reached_target = True

                            sql = text("""
                                INSERT OR REPLACE INTO historical_klines (
                                    symbol, timeframe, timestamp, open_price, 
                                    high_price, low_price, close_price, volume, 
                                    funding_rate, open_interest, created_at
                                ) VALUES (
                                    :symbol, :timeframe, :timestamp, :open,
                                    :high, :low, :close, :vol,
                                    :funding_rate, :open_interest, CURRENT_TIMESTAMP
                                )
                            """)
                            await db.execute(sql, {
                                "symbol": symbol,
                                "timeframe": tf,
                                "timestamp": ts,
                                "open": float(c.open_price),
                                "high": float(c.high_price),
                                "low": float(c.low_price),
                                "close": float(c.close_price),
                                "vol": float(c.volume),
                                "funding_rate": 0.0001,
                                "open_interest": 0.0
                            })
                            inserted_for_tf += 1

                    total_inserted += len(candles)
                    cursor_after = str(candles[-1].timestamp)

                    if len(candles) < 100 or reached_target:
                        break

                    await asyncio.sleep(0.05)

                except Exception as e:
                    logger.error(f"拉取 {symbol} [{tf}] 历史 K 线分批异常: {e}")
                    break

            details[tf] = inserted_for_tf
            logger.info(f"标的 {symbol} [{tf}] 历史 K 线同步完成，共写入 {inserted_for_tf} 根")

        return {
            "symbol": symbol,
            "total_inserted": total_inserted,
            "details": details
        }

    async def load_klines(
        self,
        symbol: str,
        timeframe: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        limit: int = 10000
    ) -> List[Dict[str, Any]]:
        """从数据湖高效加载 K 线记录列表"""
        async with get_db_session() as db:
            query = """
                SELECT 
                    timestamp, open_price as open, high_price as high, 
                    low_price as low, close_price as close, volume, 
                    funding_rate, open_interest
                FROM historical_klines
                WHERE symbol = :symbol AND timeframe = :timeframe
            """
            params: Dict[str, Any] = {"symbol": symbol, "timeframe": timeframe}

            if start_ts:
                query += " AND timestamp >= :start_ts"
                params["start_ts"] = start_ts
            if end_ts:
                query += " AND timestamp <= :end_ts"
                params["end_ts"] = end_ts

            query += " ORDER BY timestamp ASC LIMIT :limit"
            params["limit"] = limit

            res = await db.execute(text(query), params)
            rows = res.mappings().all()
            return [dict(r) for r in rows]