# -*- coding:utf-8 -*-
# author = mb
# date = 2026/2/8
"""
通达信选股公式转Python3.11实现
分为两个模块：
1. data_fetcher.py - 获取历史K线数据并保存为Excel
2. stock_selector.py - 读取数据并执行选股逻辑
"""

# ==================== 模块1：数据获取模块 (data_fetcher.py) ====================

import akshare as ak
import pandas as pd
import os
from datetime import datetime, timedelta
import time


def get_stock_list():
    """
    获取A股所有股票代码列表
    """
    try:
        # 获取东方财富A股列表
        stock_df = ak.stock_zh_a_spot()
        # 过滤掉北交所、ST股票等（可选）
        # # 1. 过滤ST股（名称包含ST）
        # stock_df = stock_df[~stock_df['名称'].str.contains('ST', na=False)]
        #
        # # 2. 过滤北交所（代码以8或4开头）
        # stock_df = stock_df[~stock_df['代码'].str.startswith(('8', '4'))]
        #
        # # 3. 过滤B股（代码以2或9开头）
        # stock_df = stock_df[~stock_df['代码'].str.startswith(('2', '9'))]
        stock_df = stock_df[stock_df['代码'].str.contains('sh', na=False)]
        stock_df = stock_df[stock_df['代码'].str.contains('sz', na=False)]
        stock_list = stock_df['代码'].tolist()
        print(f"过滤后剩余: {len(stock_list)} 只")

        return stock_list
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        return []


def fetch_single_stock_data(stock_code, start_date, end_date, data_dir="data"):
    """
    获取单只股票的历史日K线数据并保存为Excel

    参数:
        stock_code: 股票代码，如 "000001"
        start_date: 开始日期，格式 "YYYYMMDD"
        end_date: 结束日期，格式 "YYYYMMDD"
        data_dir: 数据保存目录
    """
    try:
        # 确保数据目录存在
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            print(f"创建目录: {data_dir}")

        # 检查文件是否已存在（避免重复下载）
        file_path = os.path.join(data_dir, f"{stock_code}.xlsx")
        if os.path.exists(file_path):
            print(f"股票 {stock_code} 数据已存在，跳过下载")
            return True

        # 使用akshare获取历史数据（前复权）
        # 注意：akshare的stock_zh_a_hist接口不需要sh/sz前缀
        df = ak.stock_zh_a_daily(
            symbol=stock_code,
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"  # 前复权
        )

        if df.empty:
            print(f"股票 {stock_code} 无数据")
            return False

        # 重命名列名为英文（便于后续处理）
        column_mapping = {
            '日期': 'date',
            '开盘': 'open',
            '最高': 'high',
            '最低': 'low',
            '收盘': 'close',
            '成交量': 'volume',
            '成交额': 'amount',
            '换手率': 'turnover_rate'
        }
        df.rename(columns=column_mapping, inplace=True)

        # 确保日期格式正确
        df['date'] = pd.to_datetime(df['date'])

        # 按日期排序（升序，旧数据在前）
        df = df.sort_values('date').reset_index(drop=True)

        # 保存为Excel
        df.to_excel(file_path, index=False, engine='openpyxl')
        print(f"股票 {stock_code} 数据已保存: {file_path} (共 {len(df)} 条记录)")

        # 添加延迟，避免请求过快
        time.sleep(0.5)
        return True

    except Exception as e:
        print(f"获取股票 {stock_code} 数据失败: {e}")
        return False


def batch_fetch_data(start_date=None, end_date=None, data_dir="data", max_stocks=None):
    """
    批量获取所有A股历史数据

    参数:
        start_date: 开始日期，默认一年前
        end_date: 结束日期，默认今天
        data_dir: 数据保存目录
        max_stocks: 最大获取股票数量（用于测试），None表示全部
    """
    # 设置默认日期范围（最近2年，确保有足够的历史数据计算指标）
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=730)).strftime("%Y%m%d")

    print(f"数据获取范围: {start_date} 至 {end_date}")
    print(f"数据保存目录: {os.path.abspath(data_dir)}")

    # 获取股票列表
    stock_list = get_stock_list()
    if max_stocks:
        stock_list = stock_list[:max_stocks]
        print(f"限制获取前 {max_stocks} 只股票（测试模式）")

    # 批量下载
    success_count = 0
    fail_count = 0

    for i, stock_code in enumerate(stock_list, 1):
        print(f"\n[{i}/{len(stock_list)}] 正在处理: {stock_code}")
        if fetch_single_stock_data(stock_code, start_date, end_date, data_dir):
            success_count += 1
        else:
            fail_count += 1

    print(f"\n{'=' * 50}")
    print(f"数据获取完成！成功: {success_count}, 失败: {fail_count}")
    print(f"数据保存在: {os.path.abspath(data_dir)}")
    print(f"{'=' * 50}")





