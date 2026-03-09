import os
import akshare as ak
import pandas as pd
import datetime
import time
from sqlalchemy import create_engine, text
import tushare as ts
from concurrent.futures import ThreadPoolExecutor, as_completed  # 导入多线程相关模块


class StockSyncEngine:
    DB_URL = "postgresql+psycopg2://postgres:025874yan@127.0.0.1:5432/Corenews"

    def __init__(self, db_url=DB_URL):
        self.token = "f4422b90a91c02d7dc68dd24f066988064d7307790f200243822cac3"  # 更新为你的 Tushare Token
        self.db = create_engine(db_url)
        self.global_start = "20230101"
        self.today = datetime.datetime.now().strftime("%Y%m%d")
        self._cached_calendar = None
        calendar_filename = f"tradeCalendar_{self.today}.txt"
        self.calendar_file_path = os.path.join(os.path.expanduser('~'), calendar_filename)

    def get_trade_calendar_from_akshare(self):
        # 1. 检查内存缓存
        if self._cached_calendar is not None:
            return self._cached_calendar

        # 2. 检查本地文件缓存
        if os.path.exists(self.calendar_file_path):
            try:
                # 尝试从本地文件加载，并确保日期格式正确
                df = pd.read_csv(self.calendar_file_path, parse_dates=['date'])
                # 重新筛选，确保文件中的数据范围符合当前的 global_start 和 today
                start_dt = pd.to_datetime(self.global_start)
                end_dt = pd.to_datetime(self.today)
                df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
                df = df.sort_values('date').reset_index(drop=True)
                self._cached_calendar = df
                print(f"[INFO] 从本地文件加载交易日历: {self.calendar_file_path}")
                return df
            except Exception as e:
                print(f"[WARNING] 从本地文件加载交易日历失败，将尝试从 Akshare 获取: {e}")
                # 如果文件损坏或格式不对，则继续从 Akshare 获取

        # 3. 从 Akshare 获取数据
        try:
            print(f"[INFO] 从 Akshare 获取交易日历...")
            df = ak.tool_trade_date_hist_sina()
            df = df.rename(columns={'trade_date': 'date'})
            df['date'] = pd.to_datetime(df['date'])
            start_dt = pd.to_datetime(self.global_start)
            end_dt = pd.to_datetime(self.today)
            df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
            df = df.sort_values('date').reset_index(drop=True)

            self._cached_calendar = df  # 缓存到内存

            # 写入本地文件
            try:
                df.to_csv(self.calendar_file_path, index=False, encoding='utf-8-sig')
                print(f"[INFO] 交易日历已成功写入本地文件: {self.calendar_file_path}")
            except Exception as e:
                print(f"[ERROR] 写入交易日历到本地文件失败: {e}")

            return df
        except Exception as e:
            print(f"[ERROR] 从 akshare 获取交易日历失败: {e}")
            return pd.DataFrame(columns=['date'])

    def get_main_board_pool(self):
        try:
            from Ts_GetStockBasicinfo import TushareStockManager
        except ImportError:
            print("[ERROR] 无法导入 Ts_GetStockBasicinfo 模块，请检查文件路径和类名。")
            return None

        date_suffix = self.today
        filename = f"StockIndes_{date_suffix}.txt"
        dict_file_path = os.path.join(os.path.expanduser('~'), filename)

        if not os.path.exists(dict_file_path):
            try:
                manager = TushareStockManager(self.token)
                manager.get_basic_data(list_status='L', market='主板', save_path=dict_file_path)
                print(f"股票字典保存至：{dict_file_path}")
            except Exception as e:
                print(f"[ERROR] Tushare接口获取主板股票数据失败: {e}")
                return None

        try:
            dict_df = pd.read_csv(
                dict_file_path,
                encoding='utf-8-sig',
                sep='|'
            )
            mask = dict_df.astype(str).apply(lambda row: row.str.contains(r'ST', case=False, na=False).any(), axis=1)
            dict_df = dict_df[~mask]

            # ############ 优化点开始 ############
            # 修正：将 Tushare 的 '000001.SZ' 格式直接转换为 Akshare 的 'sz000001' 格式
            def format_tushare_code_to_akshare_symbol(ts_code_str):
                if pd.isna(ts_code_str):
                    return None
                parts = ts_code_str.split('.')
                if len(parts) == 2:
                    code = parts[0]
                    market_suffix = parts[1].lower()  # 'SZ' -> 'sz', 'SH' -> 'sh'
                    # 确保是6位数字代码
                    code = str(code).zfill(6)
                    return f"{market_suffix}{code}"
                return ts_code_str  # 返回原始字符串，以便在日志中识别问题

            dict_df['ts_code'] = dict_df['ts_code'].apply(format_tushare_code_to_akshare_symbol)

            # 过滤掉无法正确转换或格式不符合 Akshare 规范的股票代码
            # (例如，原始就是不规则的，或者转换后依然是纯数字而不是 sh/sz 开头的)
            dict_df = dict_df[dict_df['ts_code'].str.match(r'^(sh|sz)\d{6}$', na=False)]
            # ############ 优化点结束 ############

            dict_df = dict_df[['ts_code']].drop_duplicates(subset=['ts_code'])
            print(f"加载 {len(dict_df)} 条股票代码（Akshare格式）：")
            return dict_df
        except Exception as e:
            print(f"[ERROR] 读取本地字典文件失败: {e}")
            # 确保返回 DataFrame 的列名与期望一致，方便后续处理
            return pd.DataFrame(columns=['ts_code'])

            # ############ 移除 add_stock_prefix 方法 ############

    # 该方法在 get_main_board_pool 优化后已不再需要，因为 ts_code 列直接是 Akshare 格式
    # def add_stock_prefix(self, stock_code):
    #     """
    #     根据A股股票代码（如'000001'）添加市场前缀（'sh'或'sz'）。
    #     此方法预期接收6位纯数字股票代码。
    #     """
    #     if not isinstance(stock_code, str) or not stock_code.isdigit() or len(stock_code) != 6:
    #         return stock_code
    #     first_digit = stock_code[0]
    #     if first_digit == '6':
    #         return f"sh{stock_code}"
    #     elif first_digit in ['0', '3']:
    #         return f"sz{stock_code}"
    #     else:
    #         return stock_code
    # ############ 移除 add_stock_prefix 方法结束 ############

    def fetch_combined_data(self, symbol, start, end):
        """封装腾讯接口，合并前复权与不复权数据"""
        try:
            df_qfq = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start, end_date=end, adjust="qfq")
            # 引入一个小的延迟，避免对Akshare接口造成过大压力
            time.sleep(0.05)  # 每个QFQ数据获取后延迟50毫秒

            if df_qfq.empty:
                # print(f"[WARNING] {symbol} 无数据返回（QFQ）") # 频繁打印可能影响日志可读性
                return None

            expected_api_cols = ['date', 'open', 'close', 'high', 'low']
            missing_api_cols = [col for col in expected_api_cols if col not in df_qfq.columns]
            if missing_api_cols:
                print(
                    f"[ERROR] {symbol} QFQ 数据结构异常，Akshare腾讯接口缺少预期列：{missing_api_cols} (当前列: {df_qfq.columns.tolist()})")
                return None

            df_norm = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start, end_date=end, adjust="")
            # 引入一个小的延迟
            time.sleep(0.05)  # 每个Normal数据获取后延迟50毫秒

            if df_norm.empty:
                # print(f"[WARNING] {symbol} 无数据返回（Normal）") # 频繁打印可能影响日志可读性
                return None

            expected_cols_norm = ['date', 'close']
            missing_norm_cols = [col for col in expected_cols_norm if col not in df_norm.columns]
            if missing_norm_cols:
                print(
                    f"[ERROR] {symbol} Normal 数据结构异常，缺少必要列：{missing_norm_cols} (当前列: {df_norm.columns.tolist()})")
                return None

            df_norm = df_norm[['date', 'close']].rename(columns={'close': 'close_normal'})
            df = pd.merge(df_qfq, df_norm, on='date', how='inner')

            if df.empty:
                # print(f"[WARNING] {symbol} 合并后无数据") # 频繁打印可能影响日志可读性
                return None

            try:
                df['close'] = pd.to_numeric(df['close'], errors='raise')
                df['close_normal'] = pd.to_numeric(df['close_normal'], errors='raise')
                # 避免除以零导致的问题，替换0为NaN后计算
                df['adj_ratio'] = df['close'] / df['close_normal'].replace(0, pd.NA)
                df.dropna(subset=['adj_ratio'], inplace=True)
            except Exception as e:
                print(f"[ERROR] {symbol} 计算 adj_ratio 失败：{e}")
                return None

            df['symbol'] = symbol
            df['date'] = pd.to_datetime(df['date'])
            df.rename(columns={'date': 'trade_date'}, inplace=True)

            final_columns_for_db = [
                'trade_date', 'symbol', 'open', 'close', 'high', 'low',
                'close_normal', 'adj_ratio'
            ]
            df = df[final_columns_for_db]

            return df

        except Exception as e:
            print(f"[CRITICAL] Error fetching {symbol}: {e}")
            return None

    def check_and_sync(self, symbol):
        query = text(
            "SELECT trade_date, adj_ratio FROM stock_daily_kline WHERE symbol = :s ORDER BY trade_date DESC LIMIT 1")
        with self.db.connect() as conn:
            local_last = conn.execute(query, {"s": symbol}).fetchone()

        if not local_last:
            # print(f"[INFO] 首次同步: {symbol}") # 减少日志输出
            data = self.fetch_combined_data(symbol, self.global_start, self.today)
            if data is not None and not data.empty:
                self.save_to_db(data)
            else:
                print(f"[WARNING] 首次同步 {symbol} 未获取到有效数据，跳过保存。")
            return

        last_date_str = local_last[0].strftime("%Y%m%d")
        remote_sample = self.fetch_combined_data(symbol, last_date_str, last_date_str)

        if remote_sample is not None and not remote_sample.empty:
            if 'adj_ratio' in remote_sample.columns and not remote_sample['adj_ratio'].empty:
                if abs(remote_sample['adj_ratio'].iloc[0] - float(local_last[1])) > 1e-8:
                    print(f"[INFO] 检测到除权: {symbol}, 触发全量重写...")
                    data = self.fetch_combined_data(symbol, self.global_start, self.today)
                    if data is not None and not data.empty:
                        self.save_to_db(data, method='replace')
                    else:
                        print(f"[WARNING] 检测到除权 {symbol} 但未能获取有效数据，跳过全量重写。")
                else:
                    # print(f"[INFO] 比率一致: {symbol}, 执行增量/修补...") # 减少日志输出
                    self.repair_gaps(symbol)
            else:
                print(f"[WARNING] 无法获取 {symbol} 在 {last_date_str} 的远程样本 adj_ratio 数据，无法进行比率校验。")
        else:
            print(f"[WARNING] 无法获取 {symbol} 在 {last_date_str} 的远程样本数据，无法进行比率校验或增量修补。")

    def repair_gaps(self, symbol):
        try:
            with self.db.connect() as conn_local:
                local_dates = pd.read_sql(text(f"SELECT trade_date FROM stock_daily_kline WHERE symbol='{symbol}'"),
                                          conn_local)
            local_dates['trade_date'] = pd.to_datetime(local_dates['trade_date'])
        except Exception as e:
            print(f"[ERROR] 查询本地数据或转换日期失败: {symbol}, {e}")
            local_dates = pd.DataFrame(columns=['trade_date'])

        std_cal = self.get_trade_calendar_from_akshare()
        if std_cal.empty:
            print(f"[WARNING] 无法获取交易日历，跳过 {symbol} 的缺口修补")
            return

        missing = sorted(list(set(std_cal['date']) - set(local_dates['trade_date'])))
        if not missing:
            # print(f"[INFO] {symbol} 没有发现缺口，无需修补。") # 减少日志输出
            return

        intervals = []
        if missing:
            start_p = missing[0]
            for i in range(1, len(missing)):
                if (missing[i] - missing[i - 1]).days > 1:
                    intervals.append((start_p.strftime("%Y%m%d"), missing[i - 1].strftime("%Y%m%d")))
                    start_p = missing[i]
            intervals.append((start_p.strftime("%Y%m%d"), missing[-1].strftime("%Y%m%d")))

        print(f"[INFO] 正在修补 {symbol} 的 {len(intervals)} 个数据缺口...")
        for s, e in intervals:
            patch_data = self.fetch_combined_data(symbol, s, e)
            if patch_data is not None and not patch_data.empty:
                self.save_to_db(patch_data, method='upsert')
                print(f"[INFO] 已修补 {symbol} 从 {s} 到 {e} 的数据。")
            else:
                print(f"[WARNING] 无法获取 {symbol} 在 {s} ~ {e} 的数据，跳过修补。")

    def save_to_db(self, df, method='upsert'):
        if df is None or df.empty:
            return

        table_name = 'stock_daily_kline'
        symbol = df['symbol'].iloc[0]

        with self.db.begin() as conn:
            if method == 'replace':
                conn.execute(text(f"DELETE FROM {table_name} WHERE symbol='{symbol}'"))
                # print(f"[INFO] 已删除 {symbol} 的旧数据，准备全量插入。") # 减少日志输出
                df.to_sql(table_name, conn, if_exists='append', index=False)
                # print(f"[INFO] 已全量插入 {len(df)} 条 {symbol} 的数据。") # 减少日志输出
            elif method == 'upsert':
                for _, row in df.iterrows():
                    row_dict = row.to_dict()
                    sql_upsert = text(f"""
                        INSERT INTO {table_name} (trade_date, symbol, open, close, high, low, close_normal, adj_ratio)
                        VALUES (:trade_date, :symbol, :open, :close, :high, :low, :close_normal, :adj_ratio)
                        ON CONFLICT (symbol, trade_date) DO UPDATE SET
                            open = EXCLUDED.open,
                            close = EXCLUDED.close,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close_normal = EXCLUDED.close_normal,
                            adj_ratio = EXCLUDED.adj_ratio
                    """)
                    try:
                        conn.execute(sql_upsert, row_dict)
                    except Exception as e:
                        print(f"[ERROR] {symbol} 在 {row_dict.get('trade_date')} UPSERT失败: {e}")
                # print(f"[INFO] 已对 {symbol} 执行 UPSERT 操作，尝试插入/更新 {len(df)} 条数据。") # 减少日志输出
            else:
                print(f"[ERROR] 未知的保存方法: {method}")

    def run_engine(self):
        # 在程序开始时，主动加载交易日历，确保它被缓存起来
        _ = self.get_trade_calendar_from_akshare()
        if self._cached_calendar.empty:
            print("[CRITICAL] 无法获取有效的交易日历，程序退出。")
            return

        pool_df = self.get_main_board_pool()
        if pool_df is None or pool_df.empty:
            print("[CRITICAL] 无法获取股票池，程序退出。")
            return

        print(f"开始同步主板共 {len(pool_df)} 只股票，使用多线程（最大15个）。..")

        MAX_WORKERS = 15
        futures = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for _, row in pool_df.iterrows():

                akshare_symbol = row['ts_code']

                futures.append(executor.submit(self.check_and_sync, akshare_symbol))


            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f'[CRITICAL] 股票数据同步任务在线程中失败: {exc}')

        print("所有股票同步任务已提交并完成处理。")
        # 不需要返回 pool_df，因为 Corenews_Main 会直接从 DB 获取已同步的股票列表

    def get_synced_stock_codes_from_db(self) -> pd.DataFrame:
        """
        从数据库获取当天已同步的股票代码列表 (带前缀，如 'sz000001')。
        这个列表将作为 Corenews_Main 的基础股票池。
        """
        # 注意：这里只查询当天有数据的股票。
        # get_main_board_pool 已经过滤了ST股和非主板。
        query = text(f"""
            SELECT DISTINCT symbol
            FROM stock_daily_kline
            WHERE trade_date = '{self.today}'
        """)
        try:
            with self.db.connect() as conn:
                df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"[ERROR] 从数据库获取已同步股票代码失败: {e}")
            return pd.DataFrame(columns=['symbol'])