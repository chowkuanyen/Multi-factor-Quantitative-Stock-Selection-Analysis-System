import os
from datetime import datetime

import pandas as pd
import akshare as ak
import time
import numpy as np

class IndustryFlowAnalyzer:

    def __init__(self, config):
        self.config = config
        self.today_str = datetime.now().strftime('%Y%m%d')
        # 定义缓存文件名
        self.cache_filename = f"行业权重趋势_{self.today_str}.txt"
        self.cache_path = os.path.join(self.config.TEMP_DATA_DIRECTORY, self.cache_filename)

    def _normalize_amount(self, val):
        """清洗金额单位"""
        if pd.isna(val): return 0.0
        s = str(val).strip()
        try:
            if '亿' in s:
                return float(s.replace('亿', ''))
            elif '万' in s:
                return float(s.replace('万', '')) / 10000.0
            else:
                return float(s)
        except:
            return 0.0

    def _clean_pct_string(self, val):
        """处理百分比字符串"""
        if pd.isna(val): return 0.0
        if isinstance(val, (int, float)): return float(val)
        try:
            return float(str(val).replace('%', '').strip())
        except:
            return 0.0

    def _fetch_and_clean(self, period_name):
        """抓取并清洗"""
        try:
            df = ak.stock_fund_flow_industry(symbol=period_name)
            if df is None or df.empty: return pd.DataFrame()
            df = df.rename(columns={'行业': '行业名称'})
            # 清洗百分比列
            pct_cols = ['行业-涨跌幅', '阶段涨跌幅', '领涨股-涨跌幅']
            for col in pct_cols:
                if col in df.columns:
                    df[col] = df[col].apply(self._clean_pct_string)
            # 清洗资金列
            money_cols = ['流入资金', '流出资金', '净额']
            for col in money_cols:
                if col in df.columns:
                    # 针对行业接口，若原本是float则直接用，若是object则清洗单位
                    if df[col].dtype == 'object':
                        df[col] = df[col].apply(self._normalize_amount)
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            return df
        except Exception as e:
            print(f"[WARN] 周期 {period_name} 数据抓取失败: {e}")
            return pd.DataFrame()

    def run_analysis(self) -> pd.DataFrame:
        # --- 1. 检查本地缓存 ---
        if os.path.exists(self.cache_path):
            print(f">>> 发现本地缓存：{self.cache_filename}，正在加载...")
            try:
                # 使用 sep='\t' 保持与主程序 TXT 保存格式一致
                cached_df = pd.read_csv(self.cache_path, sep='\t', encoding='utf-8')
                return cached_df
            except Exception as e:
                print(f"[WARN] 加载本地缓存失败: {e}，将重新计算...")

        # --- 2. 若无缓存，则执行计算逻辑 ---
        print(f"\n>>> 未发现本地数据，开始从获取行业趋势 (多周期融合)...")
        period_map = {"即时": "now", "3日排行": "3d", "5日排行": "5d", "10日排行": "10d", "20日排行": "20d"}
        dfs = {}
        for p_name, p_key in period_map.items():
            df = self._fetch_and_clean(p_name)
            if not df.empty:
                dfs[p_key] = df
            time.sleep(1.2)  # 保护性延迟

        if "now" not in dfs:
            return pd.DataFrame()

        # 数据融合计算 (与之前公式一致)
        main = dfs['now'][['行业名称', '行业指数', '行业-涨跌幅', '净额', '流入资金', '领涨股', '领涨股-涨跌幅']].copy()
        main.rename(columns={'净额': '净额_now', '行业-涨跌幅': '涨幅_now'}, inplace=True)

        for p in ['3d', '5d', '10d', '20d']:
            if p in dfs:
                tmp = dfs[p][['行业名称', '净额', '阶段涨跌幅']]
                tmp.columns = ['行业名称', f'净额_{p}', f'涨幅_{p}']
                main = pd.merge(main, tmp, on='行业名称', how='left')

            main[f'净额_{p}'] = pd.to_numeric(main.get(f'净额_{p}', 0.0), errors='coerce').fillna(0.0)
            main[f'涨幅_{p}'] = pd.to_numeric(main.get(f'涨幅_{p}', 0.0), errors='coerce').fillna(0.0)

        # 核心权重公式
        main['资金分'] = (main['净额_3d'] * 0.4 + main['净额_5d'] * 0.3 + main['净额_10d'] * 0.2 + main[
            '净额_20d'] * 0.1).rank(pct=True) * 100
        main['价格分'] = (main['涨幅_3d'] * 0.4 + main['涨幅_5d'] * 0.3 + main['涨幅_10d'] * 0.2 + main[
            '涨幅_20d'] * 0.1).rank(pct=True) * 100
        main['趋势得分'] = (main['资金分'] * 0.5 + main['价格分'] * 0.5).round(2)

        # 信号定义
        conds = [(main['趋势得分'] > 85), (main['趋势得分'] < 25)]
        main['行业信号'] = np.select(conds, ['资金主攻', '退潮预警'], default='观望')

        result = main.sort_values('趋势得分', ascending=False)

        # --- 3. 计算后保存到本地 ---
        try:
            if not os.path.exists(self.config.TEMP_DATA_DIRECTORY):
                os.makedirs(self.config.TEMP_DATA_DIRECTORY)
            # 保存为 TXT 格式，方便主程序统一管理
            result.to_csv(self.cache_path, sep='\t', index=False, encoding='utf-8')
            print(f">>> 行业分析结果已存至本地: {self.cache_filename}")
        except Exception as e:
            print(f"[WARN] 保存本地数据失败: {e}")

        return result
