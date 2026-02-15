
-- app_stock_strategy_report definition

DROP TABLE app_stock_strategy_report;
CREATE TABLE app_stock_strategy_report ( archive_date date NOT NULL, stock_code varchar(20) NOT NULL, stock_name varchar(50) NULL, industry varchar(50) NULL, close_price numeric(12, 2) NULL, is_strong_stock varchar(10) NULL, is_vol_price_rise varchar(10) NULL, consecutive_up_days int4 DEFAULT 0 NULL, high_vol_days int4 DEFAULT 0 NULL, is_top10_industry varchar(10) NULL, is_full_bullish varchar(10) NULL, macd_12269_signal varchar(50) NULL, macd_12269_momentum varchar(50) NULL, macd_12269_dif numeric(12, 4) NULL, macd_6135_signal varchar(50) NULL, macd_6135_momentum varchar(50) NULL, macd_6135_dif numeric(12, 4) NULL, kdj_signal text NULL, cci_signal varchar(100) NULL, rsi_signal varchar(100) NULL, boll_signal varchar(50) NULL, report_buy_count int4 DEFAULT 0 NULL, fund_flow_trend numeric(18, 2) NULL, fund_inflow_5d numeric(18, 2) NULL, fund_inflow_10d numeric(18, 2) NULL, fund_inflow_20d numeric(18, 2) NULL, stock_link text NULL, created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL, CONSTRAINT app_stock_strategy_report_pkey PRIMARY KEY (archive_date, stock_code));
CREATE INDEX idx_strategy_report_code ON app_stock_strategy_report USING btree (stock_code);
CREATE INDEX idx_strategy_report_date ON app_stock_strategy_report USING btree (archive_date);

-- ods_ak_industry_analysis definition

DROP TABLE ods_ak_industry_analysis;
CREATE TABLE ods_ak_industry_analysis ( id serial4 NOT NULL, archive_date date NOT NULL, industry_name varchar(100) NULL, industry_index numeric(12, 2) NULL, change_pct_now numeric(10, 4) NULL, net_inflow_now numeric(20, 2) NULL, total_inflow_money numeric(20, 2) NULL, leading_stock varchar(100) NULL, leading_stock_pct numeric(10, 4) NULL, net_inflow_3d numeric(20, 2) NULL, change_pct_3d numeric(10, 4) NULL, net_inflow_5d numeric(20, 2) NULL, change_pct_5d numeric(10, 4) NULL, net_inflow_10d numeric(20, 2) NULL, change_pct_10d numeric(10, 4) NULL, net_inflow_20d numeric(20, 2) NULL, change_pct_20d numeric(10, 4) NULL, turnover_rate numeric(10, 4) NULL, big_order_confirm varchar(50) NULL, score_fund numeric(10, 4) NULL, score_price numeric(10, 4) NULL, score_turnover numeric(10, 4) NULL, score_trend numeric(10, 2) NULL, industry_signal varchar(50) NULL, created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL, CONSTRAINT ods_ak_industry_analysis_pkey PRIMARY KEY (id));
CREATE INDEX idx_ind_date ON ods_ak_industry_analysis USING btree (archive_date);

-- ods_ak_ranking_stocks definition

DROP TABLE ods_ak_ranking_stocks;
CREATE TABLE ods_ak_ranking_stocks ( id int4 DEFAULT nextval('ods_ak_strategy_stocks_id_seq'::regclass) NOT NULL, archive_date date NOT NULL, strategy_type varchar(50) NOT NULL, stock_code varchar(20) NOT NULL, stock_name varchar(50) NULL, feature_value numeric(10, 2) NULL, description text NULL, created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL, CONSTRAINT ods_ak_strategy_stocks_combined_pkey PRIMARY KEY (archive_date, strategy_type, stock_code));
CREATE INDEX idx_stock_code_lookup ON ods_ak_ranking_stocks USING btree (stock_code);
