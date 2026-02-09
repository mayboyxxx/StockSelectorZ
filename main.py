# ==================== 主程序入口 ====================
# -*- coding:utf-8 -*-
# author = mb
# date = 2026/2/8
"""
通达信选股公式转Python3.11实现
分为两个模块：
1. data_fetcher.py - 获取历史K线数据并保存为Excel
2. stock_selector.py - 读取数据并执行选股逻辑
"""
from data_fetcher import batch_fetch_data
from stock_selector import run_stock_selection
if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("通达信选股系统 - Python 3.11")
    print("=" * 60)
    print("功能选择:")
    print("1. 获取历史K线数据 (data_fetcher)")
    print("2. 执行选股分析 (stock_selector)")
    print("3. 获取数据并立即选股 (完整流程)")
    print("=" * 60)

    choice = input("请输入选项 (1/2/3): ").strip()

    if choice == "1":
        # 仅获取数据
        days = input("获取最近多少天的数据？(默认730天/2年): ").strip()
        max_s = input("最多获取多少只股票？(直接回车表示全部，建议测试时输入100): ").strip()

        start = None
        end = None
        max_stocks = int(max_s) if max_s.isdigit() else None

        batch_fetch_data(start_date=start, end_date=end, max_stocks=max_stocks)

    elif choice == "2":
        # 仅执行选股
        run_stock_selection()

    elif choice == "3":
        # 完整流程
        print("\n步骤1: 获取历史数据...")
        batch_fetch_data(max_stocks=None)  # 获取全部

        print("\n步骤2: 执行选股分析...")
        run_stock_selection()

    else:
        print("无效选项，程序退出")