import numpy as np
import pandas as pd

class MACDAnalyzer:

 def _custom_macd(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    [自定义实现] 同时计算 MACD 标准周期 (12, 26, 9) 和加速周期 (6, 13, 5) 的快慢线金叉信号。
    **改进：区分零轴上金叉和零轴下金叉。**
    """
    if 'close' not in df.columns:
        return df

    close = df['close']

    # 定义需要计算的MACD周期组合
    macd_periods = {
        '12269': (12, 26, 9),  # 标准中长线周期
        '6135': (6, 13, 5)  # 短线/加速周期
    }

    for name, (fast, slow, signal) in macd_periods.items():
        # 1. 计算快线和慢线
        ema_fast_col = f'EMA_{fast}_{name}'
        ema_slow_col = f'EMA_{slow}_{name}'

        # adjust=False 确保权重符合技术分析的传统公式
        df[ema_fast_col] = close.ewm(span=fast, adjust=False).mean()
        df[ema_slow_col] = close.ewm(span=slow, adjust=False).mean()

        # 2. 计算 DIF
        dif_col = f'DIF_{name}'
        df[dif_col] = df[ema_fast_col] - df[ema_slow_col]

        # 3. 计算 DEA
        dea_col = f'DEA_{name}'
        df[dea_col] = df[dif_col].ewm(span=signal, adjust=False).mean()

        # 4. 判断 MACD 金叉信号 (DIF 上穿 DEA) 和零轴位置
        cross_col = f'MACD_{name}_CROSS'
        signal_col = f'MACD_{name}_SIGNAL_DETAIL'  # <== 新增的列

        # 判定金叉的布尔序列
        is_cross = (df[dif_col] > df[dea_col]) & \
                   (df[dif_col].shift(1).fillna(0) <= df[dea_col].shift(1).fillna(0))

        # **核心修改：区分零轴上/下金叉**
        df[signal_col] = np.where(
            is_cross,
            np.where(
                # 零轴上金叉条件: 金叉发生，且 DIF 和 DEA 均大于 0
                (df[dif_col] > 0) & (df[dea_col] > 0),
                '零轴上金叉',
                # 零轴下金叉条件: 金叉发生，且 DIF 或 DEA 小于等于 0
                '零轴下金叉'
            ),
            ''  # 非金叉，返回空字符串
        )

        # 保持原有的 CROSS 标志位 (非必须，但保持兼容)
        df[cross_col] = np.where(is_cross, 1, 0)

        # 5. 清理中间计算列 (保留 DIF 和 DEA)
        df.drop(columns=[ema_fast_col, ema_slow_col],
                inplace=True, errors='ignore')

    return df


 def _calculate_macd_momentum(self, df: pd.DataFrame, dif_col: str, dea_col: str) -> str:
    """
    计算 MACD 动能状态: 加速上涨/减速上涨/加速下跌/减速下跌
    """
    if len(df) < 2:
        return "N/A (数据不足)"

    # 获取最新的 DIF, DEA 值和前一天的 DIF 值
    latest_dif = df[dif_col].iloc[-1]
    latest_dea = df[dea_col].iloc[-1]
    prev_dif = df[dif_col].iloc[-2]

    # DIF 线的变化 (MACD 柱的变化方向)
    dif_change = latest_dif - prev_dif

    momentum_state = ""

    if latest_dif >= latest_dea:
        # DIF 在 DEA 之上 (多头区域/红柱)
        if dif_change > 0:
            momentum_state = "加速上涨 (红柱加长)"
        elif dif_change <= 0:
            momentum_state = "减速上涨 (红柱缩短)"
    else:
        # DIF 在 DEA 之下 (空头区域/绿柱)
        if dif_change < 0:
            momentum_state = "加速下跌 (绿柱加长)"
        elif dif_change >= 0:
            momentum_state = "减速下跌 (绿柱缩短)"

    return momentum_state
