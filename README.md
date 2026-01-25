<p align="center">
  <img src="https://github.com/paiyuyen/Multi-factor-Quantitative-Stock-Selection-Analysis-System/raw/main/Images/logo.png" alt="LOGO" width="50%">
  <br>
  <b>BAISYS 多因子A股量化分析报告</b>
</p>
<p align="center">
    <img src="https://img.shields.io/badge/Python-3.8+-blue?logo=python&logoColor=white" />
    <img src="https://img.shields.io/badge/Data-AkShare-red?logo=databricks&logoColor=white" />
    <img src="https://img.shields.io/badge/Analysis-Pandas_TA-green?logo=pandas&logoColor=white" />
    <img src="https://img.shields.io/badge/Performance-15_Thread_Parallel-brightgreen?logo=speedtest" />
    <br />
    <img src="https://img.shields.io/badge/MACD-Dual_Cycle_&_Momentum-ff4500?style=flat-square" />
    <img src="https://img.shields.io/badge/KDJ-Divergence_Detection-8a2be2?style=flat-square" />
    <img src="https://img.shields.io/badge/Output-Auto_Excel_Report-success?logo=microsoftexcel&style=social" />
    <img src="https://img.shields.io/badge/CCI-Professional_Tiering-7cfc00?style=flat-square" />
    <img src="https://img.shields.io/badge/Trend-MA_Bullish_Alignment-00ced1?style=flat-square" />
    
</p>

## 🚀2025年投资复盘与2026年展望

**2025年复盘**

2025年，我个人认为这是在经历疫情及中美博弈后，市场触底反弹迎来的第一个牛市。作为一个刚入门的初学者，能获得可观的收益，首先要归功于市场的整体回暖（Beta收益）。当然，牛市与熊市始终是风险与机遇并存的。这一年，我主要在腾讯理财通操作基金，在招商证券进行股票投机（以短线为主，少量长线配置）。这一系列操作验证了我所使用的辅助工具，结果显示该工具基本具有参考价值，但也急需打磨以提高收益率。

收益统计：

腾讯理财通（基金）：收益率 51.99%

招商证券（股票）：复利收益率 42.53%（按单利计算为 33.86%）

<br>

**2026年计划**

指标优化：引入更多参考指标，利用股市标准参数进行校验，并结合统计学方法进行多重验证，提升信号准确度。

技术升级：计划引入人工智能技术，开发股票回测与预测系统，实现从“经验判断”到“量化分析”的跨越。

<p align="center">
<img src="https://github.com/paiyuyen/Multi-factor-Quantitative-Stock-Selection-Analysis-System/raw/main/Images/2025.jpg" alt="LOGO" width="50%">
</p>

<br>

## 📖 项目简介

这是一个针对 A 股市场的综合性量化分析系统。它结合了基本面数据（机构研报、资金流向）与技术面数据（多周期 MACD、KDJ 背离、CCI 分层、均线多头），通过多线程并发获取数据，自动清洗、计算并生成一份包含投资建议的 Excel 深度分析报告。

核心设计理念是：“机构选股 + 技术择时”。系统优先筛选机构看好的标的，再通过量化指标寻找最佳买点。

<br>

## 🚀 核心功能与策略


**多维数据源整合**

实时行情：全市场股票的最新价、涨跌幅、成交量。

机构研报：主力研报盈利预测，筛选机构评级为“买入”的股票。

资金流向：5日、10日、20日的主力资金净流入数据。

特色榜单：强势股池、连续上涨、量价齐升、持续放量榜单。

行业板块：涨幅前10的行业板块及其成分股。

<br>

**深度技术分析**

Dual-Cycle MACD："双周期共振：同时计算标准周期 (12,26,9) 和加速周期 (6,13,5)。

动能状态：计算 DIF 动能，识别“加速上涨”、“减速上涨”、“加速下跌”。

零轴区分：明确区分“零轴上金叉”（强势）与“零轴下金叉”（反弹）。"

Enhanced-KDJ：识别价格创新低但 K 值未创新低的“底背离”信号。

极值反转：捕捉 J 线从负值极速反转的超跌机会。趋势确认：结合 5 日均线过滤虚假金叉。

Pro-CCI分层：将 CCI 数值量化为：极度超买(>200)、强势超买(100-200)、弱势超卖(-200~-100)、极度超卖(<-200)。

量比过滤：计算 5 日均量，剔除无量上涨 (量比 < 0.7) 的虚高个股。自动标记缩量、温和放量、巨量。

Trend-MA多头：筛选 10日 > 30日 > 60日 均线的稳健标的。突破信号：捕捉向上突破关键均线的异动。

BOLL ：识别低波段宽缩口，变盘前夕。

RSI：低位超卖区的底背离检测。

<br>

**行业权重分析**

通过多周期加权算法评估行业热度，并将其作为过滤器嵌入个股交易决策流中，实现“顺势而为”的量化策略，降低过滤出假突破的难度。

资金热度因子计算 3、5、10、20 日资金净额的加权总和，反映资金介入的持续性。

money_factor = ( 净额_3d * 0.4 + 净额_5d* 0.3 + 净额_10d * 0.2 + 净额_20d' * 0.1)

价格强度因子计算对应周期的行业指数涨跌幅，反映价格对资金的反馈强度。

price_factor = (涨幅_3d* 0.4 + 涨幅_5d * 0.3 涨幅_10d* 0.2 + 涨幅_20d * 0.1)

将上述两个银子做进一步计算，得出趋势得分 TrendScore = (price_factor * 0.5 + price_factor * 0.5).round(2)

信号定义TrendScore > 85 资金主攻 ; < 25 退潮预警; default 观望


量化交易中，最忌讳的是使用“移动平均（SMA）”，因为它对旧数据和新数据一视同仁，导致信号发出的时刻往往股价已经涨完了。

近期高权重（0.4）：

如果近 3 日资金突然暴增，这个权重能让指标迅速调头向上，产生“敏感度”。将最大权重控制在 0.4，并辅助 0.3 和 0.2，本质上是做了一次低通滤波。它滤掉了单日的随机噪点，只保留了“有预谋的持续性流入”。

远期低权重（0.1）：

20 日前的资金流虽然对现在影响微弱，但它决定了行业是在“长期阴跌”还是“长期走牛”。

<br>


**高性能与鲁棒性**

并发加速：使用 ThreadPoolExecutor (默认 15 线程) 并发下载历史行情与行业数据，大幅缩短运行时间。

智能缓存：内置本地缓存机制，避免重复请求，支持断点续跑。

自动重试：针对网络波动设计的装饰器级重试机制，确保数据获取的高可用性。

脏数据清洗：自动处理不同数据源的列名差异（如“最新价”、“现价”、“收盘”统一化）。

<br>

## 🛠️ 安装与配置

Python 13+

pip install akshare pandas pandas_ta numpy xlsxwriter

git clone [https://github.com/paiyuyen/Multi-factor-Quantitative-Stock-Selection-Analysis-System.git](https://github.com/paiyuyen/Multi-factor-Quantitative-Stock-Selection-Analysis-System.git)

python ShareAnalysis.py

<br>

**运行流程详解**

初始化：检查目录，配置线程池。

获取原始数据：下载研报、资金流、榜单数据。

初步筛选：过滤掉机构“买入”评级次数不足的股票。

历史行情下载：并发下载入选股票过去 90 天的日线数据 (前复权)。

量化计算 MACD/KDJ/RSI/CCI/BOLL。执行量比过滤 (Vol Ratio > 0.7)。提取技术信号（金叉、背离、突破）。

数据清洗与合并：将基本面数据与技术面信号对齐,输出 Excel 文件。

<br>

## 📊 报告输出解读

生成的 Excel 报告 (股票筛选报告_YYYYMMDD.xlsx) 包含多个 Sheet，其中 "数据汇总" 是核心表。

<br>

**主要字段说明**

研报买入次数：近六个月机构推荐买入的次数（热度指标）。

强势股/量价齐升：是否入选当日相关强势榜单。

MACD_12269/6135：显示具体的金叉类型（如“零轴上金叉”）。

MACD动能：如“加速上涨 (红柱加长)”。

KDJ_Signal：如“底背离金叉”、“极值J线反转”。

完全多头排列：MA10 > MA30 > MA60。

资金流入：5日/10日/20日的主力资金净额。

排序逻辑：默认按 研报买入次数 (降序) -> 连涨天数 (降序) -> 放量天数 (降序) 进行排序，优先展示机构看好且处于上升趋势的个股。

<br>

## ⚠️ 免责声明

本项目仅供学习和研究使用，不构成任何投资建议。

量化指标基于历史数据计算，无法预测未来市场黑天鹅事件。

股市有风险，入市需谨慎。
