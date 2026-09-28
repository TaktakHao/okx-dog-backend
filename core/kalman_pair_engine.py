"""
OKX-Dog 量化数理核心 - 两状态自适应卡尔曼滤波协整配对与对冲套利引擎
模块: okx-dog-backend/core/kalman_pair_engine.py
融合自: oke_auto_trade (run_pair_backest.py & run_pair.py)

核心职责:
1. 实时跟踪两相关资产 (如 BTC vs ETH, SOL vs AVAX) 对数价格的动态对冲斜率 Beta 与动态截距 Alpha。
2. 计算实时无偏残差价差 (Spread) 与动态滚动 Z-Score 偏离度。
3. 动态协方差发散熔断保护 (Covariance Trace Guard):
   - 当 Tr(P) 发散超阈值或新息持续异常，判定资产间统计协整脱钩，强制发出对冲平仓或熔断警告。
4. 生成市场中性 (Market-Neutral) 套利动作建议:
   - Z-Score > +2.0: 空从属资产、多主资产 (SHORT_SUB_LONG_MAIN)
   - Z-Score < -2.0: 多从属资产、空主资产 (LONG_SUB_SHORT_MAIN)
   - |Z-Score| <= 0.5: 价差回归，获利平仓 (CLOSE)
"""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional, Tuple, Union
import numpy as np

try:
    from models import KalmanPairMetrics
except ImportError:
    from pydantic import BaseModel, Field

    class KalmanPairMetrics(BaseModel):
        main_symbol: str
        sub_symbol: str
        dynamic_beta: float
        dynamic_alpha: float
        spread_z_score: float
        covariance_trace: float
        is_divergent: bool
        suggested_arbitrage_action: str


logger = logging.getLogger("KalmanPairEngine")


class KalmanPairEngine:
    """两状态动态卡尔曼滤波配对套利引擎"""

    def __init__(
        self,
        main_symbol: str = "BTC-USDT-SWAP",
        sub_symbol: str = "ETH-USDT-SWAP",
        delta: float = 1e-4,       # 状态转移过程方差系数
        ve: float = 1e-3,          # 观测噪声方差 Ve
        z_lookback: int = 60,      # 计算 Z-Score 的滚动窗口长度
        z_entry: float = 2.0,      # 入场阈值 (如 2.0 倍标准差)
        z_exit: float = 0.5,       # 离场止盈阈值 (如 0.5 倍标准差)
        trace_limit_mult: float = 100.0, # 协方差发散熔断乘数
    ):
        self.main_symbol = main_symbol
        self.sub_symbol = sub_symbol
        self.delta = delta
        self.ve = ve
        self.z_lookback = z_lookback
        self.z_entry = z_entry
        self.z_exit = z_exit
        self.trace_limit_mult = trace_limit_mult

        # 初始化状态向量 theta = [beta, alpha]^T
        self.theta = np.zeros(2, dtype=np.float64)  # [0]: beta, [1]: alpha
        # 初始后验协方差矩阵 P
        self.P = np.eye(2, dtype=np.float64)
        self.init_trace = float(np.trace(self.P))

        # 过程噪声协方差矩阵 Q
        self.Q = (self.delta / (1.0 - self.delta)) * np.eye(2, dtype=np.float64)

        # 历史价差缓存 (用于计算均值与标准差)
        self.spread_history: List[float] = []

    def reset_state(self, initial_beta: float = 0.0, initial_alpha: float = 0.0):
        """重置滤波器状态"""
        self.theta = np.array([initial_beta, initial_alpha], dtype=np.float64)
        self.P = np.eye(2, dtype=np.float64)
        self.spread_history.clear()

    def update_tick(
        self,
        main_price: float,
        sub_price: float,
    ) -> KalmanPairMetrics:
        """
        单步实时增量更新卡尔曼状态并计算套利指标。

        :param main_price: 主资产最新价格 (如 BTC 价格)
        :param sub_price: 从属资产最新价格 (如 ETH 价格)
        :return: KalmanPairMetrics
        """
        if main_price <= 0 or sub_price <= 0:
            raise ValueError(f"资产价格必须大于 0: main={main_price}, sub={sub_price}")

        log_main = math.log(main_price)
        log_sub = math.log(sub_price)

        # 1. 先验预测步
        # theta_{t|t-1} = theta_{t-1}
        # P_{t|t-1} = P_{t-1} + Q
        P_prior = self.P + self.Q

        # 2. 观测步: y_t = log_sub, H_t = [log_main, 1.0]
        H = np.array([log_main, 1.0], dtype=np.float64)
        y_pred = float(np.dot(H, self.theta))
        innovation = log_sub - y_pred  # 新息残差

        # 3. 新息方差 F_t = H P_prior H^T + Ve
        F = float(np.dot(H, np.dot(P_prior, H))) + self.ve
        if F <= 0:
            F = 1e-9

        # 4. 计算卡尔曼增益 K_t = P_prior H^T / F_t
        K = np.dot(P_prior, H) / F

        # 5. 后验更新
        self.theta = self.theta + K * innovation
        self.P = P_prior - np.outer(K, np.dot(H, P_prior))

        current_beta = float(self.theta[0])
        current_alpha = float(self.theta[1])
        current_trace = float(np.trace(self.P))

        # 6. 监测协方差发散 (Covariance Trace divergence)
        is_divergent = current_trace > (self.init_trace * self.trace_limit_mult)

        # 7. 计算残差价差并存入窗口
        spread = log_sub - (current_beta * log_main + current_alpha)
        self.spread_history.append(spread)
        if len(self.spread_history) > self.z_lookback * 3:
            self.spread_history = self.spread_history[-self.z_lookback * 2:]

        # 8. 滚动计算 Z-Score
        if len(self.spread_history) >= 5:
            window_slice = self.spread_history[-self.z_lookback:]
            mean_s = float(np.mean(window_slice))
            std_s = float(np.std(window_slice))
            if std_s > 1e-8:
                z_score = (spread - mean_s) / std_s
            else:
                z_score = 0.0
        else:
            z_score = 0.0

        # 9. 信号决策
        action = "NONE"
        if is_divergent:
            # 协整关系破裂，若有仓位强制离场
            action = "CLOSE"
        else:
            if z_score >= self.z_entry:
                # 价差显著偏高：从属资产被高估，做空从属资产，做多主资产对冲
                action = "SHORT_SUB_LONG_MAIN"
            elif z_score <= -self.z_entry:
                # 价差显著偏低：从属资产被低估，做多从属资产，做空主资产对冲
                action = "LONG_SUB_SHORT_MAIN"
            elif abs(z_score) <= self.z_exit:
                # 均值回归至止盈带
                action = "CLOSE"

        return KalmanPairMetrics(
            main_symbol=self.main_symbol,
            sub_symbol=self.sub_symbol,
            dynamic_beta=round(current_beta, 4),
            dynamic_alpha=round(current_alpha, 4),
            spread_z_score=round(z_score, 4),
            covariance_trace=round(current_trace, 4),
            is_divergent=is_divergent,
            suggested_arbitrage_action=action,
        )

    def batch_train_sequence(
        self,
        main_prices: List[float],
        sub_prices: List[float],
    ) -> List[KalmanPairMetrics]:
        """批量快速热身预热卡尔曼滤波器"""
        if len(main_prices) != len(sub_prices):
            raise ValueError("主资产与从属资产序列长度必须一致")
        results = []
        for mp, sp in zip(main_prices, sub_prices):
            results.append(self.update_tick(mp, sp))
        return results
