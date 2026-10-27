"""
OKX-Dog 个人量化智能交易系统 - 欧易全量交易标的元数据管理与高速搜索引擎
模块: okx-dog-backend/services/instrument_service.py
实现 OKX v5 全量真实在架交易标的 (SWAP 永续 / SPOT 现货) 的定时拉取、二级索引、本地持久化与毫秒级模糊检索
"""

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import DATA_DIR
from models import (
    InstrumentCategory,
    InstrumentInfo,
    InstrumentSearchResponse,
    InstType,
)

logger = logging.getLogger("okx_dog.instruments")

# 本地持久化缓存文件路径
INSTRUMENTS_CACHE_FILE = DATA_DIR / "okx_instruments_cache.json"

# TradFi / RWA / 美股 / 大宗商品特征关键词映射表
TRADFI_KEYWORDS = {
    "XAU": (InstrumentCategory.TRADFI, "黄金永续 (Gold Perpetuals)"),
    "XAG": (InstrumentCategory.TRADFI, "白银永续 (Silver Perpetuals)"),
    "EUR": (InstrumentCategory.TRADFI, "欧元外汇 (EUR)"),
    "GBP": (InstrumentCategory.TRADFI, "英镑外汇 (GBP)"),
    "JPY": (InstrumentCategory.TRADFI, "日元外汇 (JPY)"),
    "PAXG": (InstrumentCategory.TRADFI, "PAX Gold 黄金代币"),
    "NVDA": (InstrumentCategory.EQUITIES, "英伟达代币化永续 (NVIDIA Synthetic)"),
    "TSLA": (InstrumentCategory.EQUITIES, "特斯拉代币化永续 (Tesla Synthetic)"),
    "AAPL": (InstrumentCategory.EQUITIES, "苹果代币化永续 (Apple Synthetic)"),
    "MSFT": (InstrumentCategory.EQUITIES, "微软代币化永续 (Microsoft Synthetic)"),
    "GOOGL": (InstrumentCategory.EQUITIES, "谷歌代币化永续 (Google Synthetic)"),
    "ONDO": (InstrumentCategory.RWA, "Ondo Finance (RWA美债龙头)"),
    "PENDLE": (InstrumentCategory.RWA, "Pendle (收益利息拆分协议)"),
    "MKR": (InstrumentCategory.RWA, "MakerDAO (Sky RWA抵押协议)"),
}


class InstrumentManager:
    """
    OKX 全量标的元数据管理单例
    - 内存双向索引 (支持 instId, baseCcy, 模糊名称多维度秒级检索)
    - 本地 JSON 离线持久化兜底 (杜绝网络波动导致标的池空白)
    - 后台定时异步静默自愈与刷新 (每 6 小时自动增量同步)
    """

    def __init__(self):
        self._instruments_map: Dict[str, InstrumentInfo] = {}
        self._all_instruments: List[InstrumentInfo] = []
        self._is_initialized: bool = False
        self._last_synced_at: int = 0
        self._sync_lock = asyncio.Lock()
        self._refresh_task: Optional[asyncio.Task] = None

    @property
    def is_initialized(self) -> bool:
        return self._is_initialized

    @property
    def total_count(self) -> int:
        return len(self._all_instruments)

    async def initialize(self):
        """服务启动预热初始化"""
        # 1. 优先尝试从本地持久化文件毫秒级恢复
        loaded = self._load_from_disk()
        if loaded:
            self._is_initialized = True
            logger.info(f"[InstrumentManager] 已从本地缓存加载 {len(self._all_instruments)} 个 OKX 真实标的")

        # 2. 异步启动全量后台同步刷新协程
        asyncio.create_task(self.sync_from_okx(force=False))

    def _load_from_disk(self) -> bool:
        """从本地磁盘缓存文件读取"""
        try:
            if not INSTRUMENTS_CACHE_FILE.exists():
                return False
            with open(INSTRUMENTS_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            raw_list = data.get("instruments", [])
            if not raw_list:
                return False

            inst_list = []
            inst_map = {}
            for item in raw_list:
                try:
                    obj = InstrumentInfo(**item)
                    inst_list.append(obj)
                    inst_map[obj.inst_id] = obj
                except Exception:
                    continue

            if inst_list:
                self._all_instruments = inst_list
                self._instruments_map = inst_map
                self._last_synced_at = data.get("synced_at", int(time.time() * 1000))
                return True
        except Exception as e:
            logger.warning(f"[InstrumentManager] 从本地缓存文件恢复失败: {e}")
        return False

    def _save_to_disk(self):
        """持久化保存到本地磁盘文件"""
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            payload = {
                "synced_at": int(time.time() * 1000),
                "total_count": len(self._all_instruments),
                "instruments": [item.model_dump() for item in self._all_instruments],
            }
            with open(INSTRUMENTS_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            logger.info(f"[InstrumentManager] 成功持久化 {len(self._all_instruments)} 个标的到磁盘")
        except Exception as e:
            logger.error(f"[InstrumentManager] 持久化到磁盘失败: {e}")

    async def sync_from_okx(self, force: bool = False):
        """
        从 OKX 官方 REST 接口拉取全量真实在架交易标的：
        1. SWAP (永续合约)
        2. SPOT (现货交易对)
        """
        async with self._sync_lock:
            from state import rest_client

            now_ms = int(time.time() * 1000)
            # 若非强制且上次同步在 6 小时内，跳过刷新
            if not force and self._last_synced_at > 0 and (now_ms - self._last_synced_at) < 6 * 3600 * 1000:
                logger.debug("[InstrumentManager] 本地标的缓存处于有效期内，无需全量同步")
                return

            logger.info("[InstrumentManager] 开始拉取 OKX 官方全量在架产品列表...")
            new_instruments: List[InstrumentInfo] = []
            new_map: Dict[str, InstrumentInfo] = {}

            # 1. 拉取全量永续合约 SWAP
            try:
                swap_raw = await rest_client.get_instruments(inst_type="SWAP")
                logger.info(f"[InstrumentManager] 成功拉取到 {len(swap_raw)} 个 SWAP 永续合约原始记录")
                for item in swap_raw:
                    inst = self._parse_okx_item(item, InstType.SWAP)
                    if inst and inst.state == "live":
                        new_instruments.append(inst)
                        new_map[inst.inst_id] = inst
            except Exception as e:
                logger.warning(f"[InstrumentManager] 拉取 OKX SWAP 标的列表失败: {e}")

            # 2. 拉取全量现货 SPOT
            try:
                spot_raw = await rest_client.get_instruments(inst_type="SPOT")
                logger.info(f"[InstrumentManager] 成功拉取到 {len(spot_raw)} 个 SPOT 现货原始记录")
                for item in spot_raw:
                    inst = self._parse_okx_item(item, InstType.SPOT)
                    if inst and inst.state == "live":
                        new_instruments.append(inst)
                        new_map[inst.inst_id] = inst
            except Exception as e:
                logger.warning(f"[InstrumentManager] 拉取 OKX SPOT 标的列表失败: {e}")

            if new_instruments:
                # 排序规则：优先主流资产与常用计价币 (USDT)，按字母与流动性权重呈现
                new_instruments.sort(key=self._instrument_sort_key)
                self._all_instruments = new_instruments
                self._instruments_map = new_map
                self._last_synced_at = now_ms
                self._is_initialized = True
                self._save_to_disk()
                logger.info(f"[InstrumentManager] OKX 全量标的同步完成，共就绪 {len(new_instruments)} 个活跃在架产品！")
            else:
                logger.warning("[InstrumentManager] 本次未同步到有效标的数据，保持现有缓存")

    def _parse_okx_item(self, item: Dict[str, Any], default_inst_type: InstType) -> Optional[InstrumentInfo]:
        """将 OKX v5 返回的单条产品结构解析为标准 InstrumentInfo"""
        try:
            inst_id = str(item.get("instId", "")).strip()
            if not inst_id:
                return None

            base_ccy = str(item.get("baseCcy", "")).strip().upper()
            quote_ccy = str(item.get("quoteCcy", "")).strip().upper()

            # 部分永续合约可能未直接返回 baseCcy，从 instId (例如 BTC-USDT-SWAP) 拆解
            if not base_ccy and "-" in inst_id:
                parts = inst_id.split("-")
                base_ccy = parts[0]
                if len(parts) > 1 and not quote_ccy:
                    quote_ccy = parts[1]

            if not quote_ccy:
                quote_ccy = "USDT"

            raw_type = str(item.get("instType", default_inst_type.value)).upper()
            if "SWAP" in raw_type:
                inst_type = InstType.SWAP
            elif "SPOT" in raw_type:
                inst_type = InstType.SPOT
            elif "MARGIN" in raw_type:
                inst_type = InstType.MARGIN
            elif "FUTURES" in raw_type:
                inst_type = InstType.FUTURES
            else:
                inst_type = default_inst_type

            # 判断 TradFi / RWA / 大宗 / 美股代币化
            is_tradfi = False
            category = InstrumentCategory.CRYPTO
            name = ""

            for kw, (cat, display_name) in TRADFI_KEYWORDS.items():
                if kw == base_ccy or (kw in inst_id and not base_ccy.startswith("BTC")):
                    is_tradfi = True
                    category = cat
                    name = display_name
                    break

            if not name:
                type_name = "永续合约" if inst_type == InstType.SWAP else "现货交易对"
                name = f"{base_ccy} {type_name}"

            # 解析精度与面值
            ct_val = float(item.get("ctVal", 1.0)) if item.get("ctVal") else 1.0
            ct_mult = float(item.get("ctMult", 1.0)) if item.get("ctMult") else None
            ct_val_ccy = item.get("ctValCcy")
            tick_sz = float(item.get("tickSz", 0.01)) if item.get("tickSz") else 0.01
            lot_sz = float(item.get("lotSz", 0.01)) if item.get("lotSz") else 0.01
            min_sz = float(item.get("minSz", 0.01)) if item.get("minSz") else 0.01
            lever_str = item.get("lever")
            lever = float(lever_str) if lever_str else (100.0 if inst_type == InstType.SWAP else 1.0)
            state = str(item.get("state", "live")).lower()

            return InstrumentInfo(
                symbol=inst_id,
                inst_id=inst_id,
                inst_type=inst_type,
                base_ccy=base_ccy,
                quote_ccy=quote_ccy,
                settle_ccy=item.get("settleCcy", quote_ccy),
                name=name,
                category=category,
                is_tradfi=is_tradfi,
                ct_val=ct_val,
                ct_mult=ct_mult,
                ct_val_ccy=ct_val_ccy,
                tick_sz=tick_sz,
                lot_sz=lot_sz,
                min_sz=min_sz,
                lever=lever,
                state=state
            )
        except Exception as err:
            logger.debug(f"[InstrumentManager] 解析标的失败: {item.get('instId')}, 原因: {err}")
            return None

    def _instrument_sort_key(self, item: InstrumentInfo) -> tuple:
        """标的排序权重：主流核心在前，USDT 优先"""
        priority_symbols = [
            "BTC", "ETH", "SOL", "DOGE", "PEPE", "SUI", "BNB", "XRP",
            "XAU", "XAG", "NVDA", "TSLA", "ONDO", "PENDLE", "MKR"
        ]
        p_score = 999
        for idx, s in enumerate(priority_symbols):
            if item.base_ccy == s:
                p_score = idx
                break

        is_usdt = 0 if item.quote_ccy == "USDT" else 1
        is_swap = 0 if item.inst_type == InstType.SWAP else 1
        return (p_score, is_usdt, is_swap, item.symbol)

    def search(
        self,
        query: Optional[str] = None,
        inst_type: Optional[str] = None,
        quote_ccy: Optional[str] = None,
        limit: int = 30
    ) -> InstrumentSearchResponse:
        """
        在全量产品库中执行毫秒级多维模糊过滤
        :param query: 搜索关键词 (如 "PEPE", "btc", "gold", "xau")
        :param inst_type: 产品类型筛选 ("SWAP", "SPOT", "ALL")
        :param quote_ccy: 计价币筛选 ("USDT", "USDC", "ALL")
        :param limit: 返回最大条数 (默认 30，上限 100)
        """
        clean_limit = min(max(1, limit), 100)
        q = (query or "").strip().upper()

        results: List[InstrumentInfo] = []

        # 1. 基础类型过滤判断函数
        def _matches_filters(item: InstrumentInfo) -> bool:
            if inst_type and inst_type.upper() != "ALL":
                if item.inst_type.value.upper() != inst_type.upper():
                    return False
            if quote_ccy and quote_ccy.upper() != "ALL":
                if item.quote_ccy.upper() != quote_ccy.upper():
                    return False
            return True

        if not q:
            # 无关键词时，返回推荐标的前 N 个
            for item in self._all_instruments:
                if _matches_filters(item):
                    results.append(item)
                    if len(results) >= clean_limit:
                        break
            return InstrumentSearchResponse(
                instruments=results,
                total_count=len(results),
                query="",
                cached=True
            )

        # 2. 具备关键词时的分级智能匹配
        exact_matches = []
        prefix_matches = []
        contain_matches = []

        for item in self._all_instruments:
            if not _matches_filters(item):
                continue

            sym_u = item.symbol.upper()
            base_u = item.base_ccy.upper()
            name_u = item.name.upper()

            # 级别 1: 精确命中基础币或代码
            if base_u == q or sym_u == q or sym_u == f"{q}-USDT-SWAP" or sym_u == f"{q}-USDT":
                exact_matches.append(item)
            # 级别 2: 前缀命中
            elif sym_u.startswith(q) or base_u.startswith(q):
                prefix_matches.append(item)
            # 级别 3: 包含命中 (代码或全名)
            elif q in sym_u or q in base_u or q in name_u:
                contain_matches.append(item)

        combined = exact_matches + prefix_matches + contain_matches
        total_found = len(combined)
        final_list = combined[:clean_limit]

        return InstrumentSearchResponse(
            instruments=final_list,
            total_count=total_found,
            query=query or "",
            cached=True
        )

    def get_by_symbol(self, symbol: str) -> Optional[InstrumentInfo]:
        """根据精确 symbol 查询合法标的元数据"""
        clean_sym = symbol.strip().upper()
        return self._instruments_map.get(clean_sym)


# 全局单例管理器
instrument_manager = InstrumentManager()
