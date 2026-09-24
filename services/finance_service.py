"""
OKX-Dog 个人量化智能交易系统 - 资深财务分析师与资金配置服务
模块: okx-dog-backend/services/finance_service.py
角色映射: agency-financial-analyst (资深财务分析师)
核心功能:
- 资本利用率 (Capital Utilization) 与现金流健康度监控
- 真实费率摩擦消耗 (Fee Drag) 与毛利/净利损益归因 (PnL Attribution)
- 机构级风险调整后收益指标: 夏普比率 (Sharpe Ratio)、索提诺比率 (Sortino Ratio)、卡玛比率 (Calmar Ratio)
- 交易数学期望 (Expectancy) 与胜率/盈亏比深度剖析
- 资深财务分析师专业诊断与决策支持
"""

import logging
import math
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from paper_trading import paper_engine
from auto_pilot import auto_pilot_manager
from state import rest_client

logger = logging.getLogger("okx_dog.finance")


class FinancialHealthReport(BaseModel):
    """资深财务分析师全维度财务体检与资金效率报告"""
    # 账户权益与现金流
    total_equity: float = Field(..., description="账户总权益 (USDT)")
    available_margin: float = Field(..., description="可用保证金 (USDT)")
    used_margin: float = Field(..., description="已占用保证金 (USDT)")
    capital_utilization_pct: float = Field(..., description="资本利用率 (%)")

    # 损益与费率磨损归因 (PnL Attribution)
    gross_profit: float = Field(0.0, description="累计毛盈利 (USDT，不含手续费)")
    gross_loss: float = Field(0.0, description="累计毛亏损绝对值 (USDT，不含手续费)")
    total_fees_paid: float = Field(0.0, description="累计产生的手续费摩擦 (USDT)")
    net_pnl: float = Field(0.0, description="真实净盈亏 (USDT = GrossProfit - GrossLoss - Fees)")
    fee_drag_pct: float = Field(0.0, description="费率摩擦拖累比 (%，手续费占毛盈利的比率)")

    # 交易质量指标
    total_trades: int = Field(0, description="已完成结算交易总笔数")
    winning_trades: int = Field(0, description="盈利交易笔数")
    losing_trades: int = Field(0, description="亏损交易笔数")
    win_rate_pct: float = Field(0.0, description="胜率 (%)")
    profit_factor: float = Field(0.0, description="盈亏比 (Gross Profit / Gross Loss)")
    expectancy_usdt: float = Field(0.0, description="单笔数学期望净收益 (USDT/笔)")

    # 风险调整后收益比率 (Risk-Adjusted Return)
    sharpe_ratio: float = Field(0.0, description="年化夏普比率 (Sharpe Ratio)")
    sortino_ratio: float = Field(0.0, description="年化索提诺比率 (Sortino Ratio)")
    calmar_ratio: float = Field(0.0, description="卡玛比率 (Calmar Ratio)")
    max_drawdown_pct: float = Field(0.0, description="历史最大回撤率 (%)")
    current_drawdown_pct: float = Field(0.0, description="当前自高水位线回撤率 (%)")

    # 财务分析师评价
    financial_rating: str = Field("HEALTHY", description="财务健康度评级: EXCELLENT / HEALTHY / WARNING / CRITICAL")
    analyst_commentary: List[str] = Field(default_factory=list, description="资深财务分析师专业意见与风险提示")


class FinanceService:
    """资深财务分析师核心服务"""

    def __init__(self):
        pass

    def calculate_health_report(self, is_live: bool = False) -> FinancialHealthReport:
        """
        生成综合财务健康度与资金效率诊断报告
        :param is_live: True 为实盘，False 为模拟盘
        """
        # 1. 获取账户资金与保证金
        if is_live:
            try:
                bal = rest_client.get_account_balance_sync() if hasattr(rest_client, "get_account_balance_sync") else None
                total_equity = getattr(bal, "total_equity_usdt", 10000.0) if bal else 10000.0
                avail_margin = getattr(bal, "available_margin_usdt", total_equity) if bal else total_equity
                used_margin = max(0.0, total_equity - avail_margin)
            except Exception as e:
                logger.warning(f"获取实盘资产失败，使用基准值计算: {e}")
                total_equity = 10000.0
                avail_margin = 10000.0
                used_margin = 0.0
            trades_source = auto_pilot_manager.get_state(is_live_mode=True).trades
            snapshots = auto_pilot_manager.get_state(is_live_mode=True).snapshots
        else:
            acc = paper_engine.get_account_info()
            total_equity = acc.total_equity_usdt
            avail_margin = acc.available_margin_usdt
            used_margin = acc.used_margin_usdt
            trades_source = paper_engine.closed_trades
            snapshots = auto_pilot_manager.get_state(is_live_mode=False).snapshots

        # 资本利用率
        cap_util = (used_margin / total_equity * 100.0) if total_equity > 0 else 0.0

        # 2. 统合已平仓交易，计算损益与费率拖累归因
        gross_profit = 0.0
        gross_loss = 0.0
        total_fees = 0.0
        net_pnl = 0.0
        win_count = 0
        loss_count = 0
        returns_list: List[float] = []

        total_trades = len(trades_source)

        for t in trades_source:
            if isinstance(t, dict):
                pnl = float(t.get("realized_pnl", 0.0))
                fee = float(t.get("fee", 0.0))
                roi = float(t.get("roi_pct", 0.0))
            else:
                pnl = float(getattr(t, "realized_pnl", 0.0))
                fee = float(getattr(t, "fee", 0.0))
                roi = float(getattr(t, "roi_pct", 0.0))

            net_pnl += pnl
            total_fees += fee
            # 毛盈亏 = 净盈亏 + 扣减的手续费
            gross_trade_pnl = pnl + fee
            if gross_trade_pnl > 0:
                gross_profit += gross_trade_pnl
                win_count += 1
            else:
                gross_loss += abs(gross_trade_pnl)
                loss_count += 1

            returns_list.append(roi / 100.0)

        # 费率摩擦拖累比 (Fee Drag)
        fee_drag_pct = (total_fees / gross_profit * 100.0) if gross_profit > 0 else (100.0 if total_fees > 0 else 0.0)
        win_rate = (win_count / total_trades * 100.0) if total_trades > 0 else 0.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0)
        expectancy = (net_pnl / total_trades) if total_trades > 0 else 0.0

        # 3. 风险调整后收益指标计算 (夏普 / 索提诺 / 卡玛 / 回撤)
        sharpe_ratio, sortino_ratio, mdd_pct, cur_dd_pct, calmar_ratio = self._calculate_risk_adjusted_metrics(
            snapshots, returns_list, total_equity
        )

        # 4. 资深财务分析师综合诊断
        rating, commentary = self._generate_analyst_commentary(
            cap_util, fee_drag_pct, win_rate, profit_factor, cur_dd_pct, mdd_pct, sharpe_ratio
        )

        return FinancialHealthReport(
            total_equity=round(total_equity, 2),
            available_margin=round(avail_margin, 2),
            used_margin=round(used_margin, 2),
            capital_utilization_pct=round(cap_util, 2),
            gross_profit=round(gross_profit, 2),
            gross_loss=round(gross_loss, 2),
            total_fees_paid=round(total_fees, 4),
            net_pnl=round(net_pnl, 2),
            fee_drag_pct=round(fee_drag_pct, 2),
            total_trades=total_trades,
            winning_trades=win_count,
            losing_trades=loss_count,
            win_rate_pct=round(win_rate, 2),
            profit_factor=round(profit_factor, 2),
            expectancy_usdt=round(expectancy, 2),
            sharpe_ratio=round(sharpe_ratio, 2),
            sortino_ratio=round(sortino_ratio, 2),
            calmar_ratio=round(calmar_ratio, 2),
            max_drawdown_pct=round(mdd_pct, 2),
            current_drawdown_pct=round(cur_dd_pct, 2),
            financial_rating=rating,
            analyst_commentary=commentary
        )

    def _calculate_risk_adjusted_metrics(
        self,
        snapshots: List[Any],
        returns_list: List[float],
        current_equity: float
    ) -> tuple:
        """计算夏普、索提诺、卡玛与回撤率"""
        equities: List[float] = []
        for s in snapshots:
            eq = getattr(s, "equity", None) if hasattr(s, "equity") else (s.get("equity") if isinstance(s, dict) else None)
            if eq and eq > 0:
                equities.append(float(eq))

        if not equities or equities[-1] != current_equity:
            equities.append(current_equity)

        hwm = equities[0]
        max_dd = 0.0
        for eq in equities:
            if eq > hwm:
                hwm = eq
            dd = (hwm - eq) / hwm if hwm > 0 else 0.0
            if dd > max_dd:
                max_dd = dd

        cur_hwm = max(equities) if equities else current_equity
        cur_dd = (cur_hwm - current_equity) / cur_hwm if cur_hwm > 0 else 0.0

        # 夏普与索提诺比率 (年化换算：假设日频/交易频 252)
        annual_factor = math.sqrt(252)
        rf = 0.03 / 252  # 假定无风险日收益率 ~3% 年化

        if len(returns_list) >= 2:
            n = len(returns_list)
            mean_r = sum(returns_list) / n
            variance = sum((r - mean_r) ** 2 for r in returns_list) / (n - 1)
            std_r = math.sqrt(variance) if variance > 0 else 0.0

            sharpe = float((mean_r - rf) / std_r * annual_factor) if std_r > 1e-6 else 0.0

            neg_returns = [r for r in returns_list if r < rf]
            if len(neg_returns) >= 1:
                if len(neg_returns) > 1:
                    neg_mean = sum(neg_returns) / len(neg_returns)
                    neg_var = sum((r - neg_mean) ** 2 for r in neg_returns) / (len(neg_returns) - 1)
                    downside_std = math.sqrt(neg_var) if neg_var > 0 else 0.0
                else:
                    downside_std = abs(neg_returns[0])
                sortino = float((mean_r - rf) / downside_std * annual_factor) if downside_std > 1e-6 else 0.0
            else:
                sortino = sharpe * 1.5 if sharpe > 0 else 0.0

            annualized_ret = float(mean_r * 252 * 100)
            calmar = (annualized_ret / (max_dd * 100)) if max_dd > 0.001 else 99.0
        else:
            sharpe = 0.0
            sortino = 0.0
            calmar = 0.0

        return sharpe, sortino, max_dd * 100.0, cur_dd * 100.0, calmar

    def _generate_analyst_commentary(
        self,
        cap_util: float,
        fee_drag_pct: float,
        win_rate: float,
        profit_factor: float,
        cur_dd_pct: float,
        mdd_pct: float,
        sharpe_ratio: float
    ) -> tuple:
        """生成资深财务分析师评级与专业建议"""
        comments: List[str] = []
        rating = "HEALTHY"

        # 1. 资金利用率评估
        if cap_util > 50.0:
            comments.append(f"【财务预警】当前保证金占用高达 {cap_util:.1f}%，资本利用率偏高，存在多空双向逼仓时的穿仓风险，建议控制同时持仓标的上限。")
            rating = "WARNING"
        elif cap_util < 10.0:
            comments.append(f"【资金配置建议】当前资本利用率仅为 {cap_util:.1f}%，超 90% 资金处于闲置状态，在市场趋势明确时可适度提高活跃标的分配权重。")
        else:
            comments.append(f"【资金配置合理】资本利用率处于 {cap_util:.1f}% 的健康区间，具备充沛的安全垫抵御极端插针。")

        # 2. 手续费摩擦损耗评估
        if fee_drag_pct > 35.0:
            comments.append(f"【致命摩擦警报】手续费侵蚀比率高达 {fee_drag_pct:.1f}%，超过三分之一的毛利被交易费吞噬，必须全面实施 Post-Only 挂单与扩大单笔目标止盈点位！")
            rating = "CRITICAL" if rating != "CRITICAL" else rating
        elif fee_drag_pct > 18.0:
            comments.append(f"【摩擦关注】手续费磨损率为 {fee_drag_pct:.1f}%，处于中度消耗区间，建议减少高频微利平仓。")
        else:
            comments.append(f"【费率极度友好】手续费磨损率低至 {fee_drag_pct:.1f}%，Maker 挂单优化效果显著，收益绝大部分转化为实得净值。")

        # 3. 回撤与风险回报评估
        if cur_dd_pct >= 8.0:
            comments.append(f"【回撤防御触发】当前账户回撤已达 {cur_dd_pct:.1f}%，FP&A 滚动预算已介入压缩单笔风险，建议暂停非主线标的激进开仓。")
            rating = "CRITICAL"
        elif cur_dd_pct >= 4.0:
            comments.append(f"【轻度回撤】当前自峰值回撤 {cur_dd_pct:.1f}%，整体可控，保持防守纪律。")

        if sharpe_ratio >= 1.8:
            comments.append(f"【卓越 Alpha】年化夏普比率达到 {sharpe_ratio:.2f}，策略具备显著的机构级超额收益与极佳的风险补偿。")
        elif sharpe_ratio < 0.8 and sharpe_ratio != 0.0:
            comments.append(f"【收益波动偏大】夏普比率仅为 {sharpe_ratio:.2f}，表明每承担一单位风险所获得的补偿偏低，需强化顺势进场过滤。")

        return rating, comments


# 单例财务分析服务
finance_service = FinanceService()
