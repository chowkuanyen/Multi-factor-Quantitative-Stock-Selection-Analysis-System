import akshare as ak
import pandas as pd
import time
import os
import ParallelUtils as utils  # 确保你的工程目录下有这个工具类
from typing import List, Dict, Optional


class ChipDistributionAnalyzer:
    """
    筹码分布分析类：直接调用东方财富(EM)接口获取筹码数据
    """

    def __init__(self, config):
        self.config = config
        self.today_str = time.strftime("%Y%m%d")

    def _interpret_cyq_status(self, profit_ratio: float, concentration: float) -> str:
        """
        基于接口返回字段的逻辑判定逻辑
        """
        if pd.isna(profit_ratio) or pd.isna(concentration):
            return "数据缺失"

        # 1. 高度锁仓：获利盘极高且筹码高度集中
        if profit_ratio >= 90 and concentration <= 10:
            return "高度锁仓"
        # 2. 筹码密集：无论获利比例如何，90%筹码都在很窄的价格区间
        elif concentration <= 12:
            return "筹码密集"
        # 3. 低位深套：绝大多数筹码处于亏损状态
        elif profit_ratio <= 5:
            return "深套区间"
        # 4. 筹码分散：筹码分布杂乱，缺乏合力
        elif concentration >= 20:
            return "筹码分散"

        return "筹码常规"

    def fetch_chip_data_parallel(self, codes: List[str]) -> pd.DataFrame:
        """
        并发获取全量股票的筹码分布数据
        """
        print(f"\n>>> 正在调用 AkShare 接口获取 {len(codes)} 只股票的筹码分布信息...")

        def fetch_worker(code):
            # 接口尝试
            for _ in range(self.config.DATA_FETCH_RETRIES):
                try:
                    # 调用 stock_cyq_em 接口
                    cyq_df = ak.stock_cyq_em(symbol=code, adjust="hfq")
                    if cyq_df is not None and not cyq_df.empty:
                        last_row = cyq_df.iloc[-1]
                        p_ratio = last_row['获利比例']
                        concentrate = last_row['90集中度']
                        avg_cost = last_row['平均成本']

                        return {
                            '股票代码': code,
                            '获利比例': f"{p_ratio:.2f}%",
                            '90集中度': f"{concentrate:.2f}%",
                            '平均成本': round(avg_cost, 3),
                            '筹码状态': self._interpret_cyq_status(p_ratio, concentrate)
                        }
                except Exception:
                    time.sleep(1)
            return None

        # 使用 ParallelUtils 中的通用并发工具
        results = utils.run_with_thread_pool(
            items=codes,
            worker_func=fetch_worker,
            max_workers=self.config.MAX_WORKERS,
            desc="获取筹码分布"
        )

        final_df = pd.DataFrame([r for r in results if r])
        if final_df.empty:
            return pd.DataFrame(columns=['股票代码', '获利比例', '90集中度', '平均成本', '筹码状态'])

        return final_df
