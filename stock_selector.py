# -*- coding:utf-8 -*-
# author = mb
# date = 2026/2/8
"""
通达信选股公式转Python3.11实现
分为两个模块：
1. data_fetcher.py - 获取历史K线数据并保存为Excel
2. stock_selector.py - 读取数据并执行选股逻辑
"""
# ==================== 模块2：选股模块 (stock_selector.py) ====================

import numpy as np
import pandas as pd
import os
import glob
from datetime import datetime


def sma(series, n, m):
    """
    计算SMA（平滑移动平均），通达信公式中的SMA

    公式: SMA(X,N,M) = M/N*X + (N-M)/N*REF(SMA,1)
    相当于EMA的变体，但权重不同

    参数:
        series: 输入序列
        n: 周期
        m: 权重
    """
    result = pd.Series(index=series.index, dtype=float)
    result.iloc[0] = series.iloc[0]  # 初始值

    for i in range(1, len(series)):
        if pd.isna(result.iloc[i - 1]):
            result.iloc[i] = series.iloc[i]
        else:
            result.iloc[i] = (m * series.iloc[i] + (n - m) * result.iloc[i - 1]) / n

    return result


def calculate_kdj(df):
    """
    计算KDJ指标
    """
    # RSV: (收盘价-9日最低)/(9日最高-9日最低)*100
    low_9 = df['low'].rolling(window=9).min()
    high_9 = df['high'].rolling(window=9).max()
    den = high_9 - low_9

    # 处理DEN=0的情况，设为50
    rsv = pd.Series(50, index=df.index)
    mask = den != 0
    rsv[mask] = (df['close'][mask] - low_9[mask]) / den[mask] * 100

    # K: RSV的3日SMA，权重1
    k = sma(rsv, 3, 1)
    # D: K的3日SMA，权重1
    d = sma(k, 3, 1)
    # J: 3K-2D
    j = 3 * k - 2 * d

    return k, d, j


def calculate_macd(df, fast=12, slow=26, signal=9):
    """
    计算MACD指标
    返回: dif, dea, macd
    """
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd = (dif - dea) * 2
    return dif, dea, macd


def analyze_stock(df):
    """
    对单只股票进行通达信公式选股分析

    参数:
        df: DataFrame包含股票历史数据

    返回:
        dict: 包含选股结果和各指标状态
    """
    if len(df) < 60:  # 需要至少60天的数据计算40日指标
        return None

    # 确保数据按日期排序（升序）
    df = df.sort_values('date').reset_index(drop=True)

    # 计算必要指标
    # H, L, C, O, VOL 对应 high, low, close, open, volume
    df['H'] = df['high']
    df['L'] = df['low']
    df['C'] = df['close']
    df['O'] = df['open']
    df['VOL'] = df['volume']
    df['AMOUNT'] = df['amount'] if 'amount' in df.columns else df['volume'] * df['close']

    # 计算REF（前一日数据）
    df['REF_C_1'] = df['C'].shift(1)

    # ==================== KDJ计算 ====================
    k, d, j = calculate_kdj(df)
    df['K'] = k
    df['D'] = d
    df['J'] = j
    j_ok = df['J'] <= 13  # J_OK: J<=13

    # ==================== 真假阴阳线 ====================
    # REAL_YANG: C>O AND NOT(C<REF(C,1))  - 真阳线（收涨且不低于昨日收盘）
    real_yang = (df['C'] > df['O']) & ~(df['C'] < df['REF_C_1'])
    # REAL_YIN: C<O AND NOT(C>REF(C,1))   - 真阴线（收跌且不高于昨日收盘）
    real_yin = (df['C'] < df['O']) & ~(df['C'] > df['REF_C_1'])
    # FAKE_YANG: NOT(C<REF(C,1))          - 假阳线（不低于昨日收盘，包含平收和上涨）
    fake_yang = ~(df['C'] < df['REF_C_1'])
    # FAKE_YIN: NOT(C>REF(C,1))           - 假阴线（不高于昨日收盘，包含平收和下跌）
    fake_yin = ~(df['C'] > df['REF_C_1'])

    df['REAL_YANG'] = real_yang
    df['REAL_YIN'] = real_yin
    df['FAKE_YANG'] = fake_yang
    df['FAKE_YIN'] = fake_yin

    # ==================== 成交量统计 ====================
    # VOL*REAL_YANG 等，然后SUM
    df['VOL_YANG21'] = (df['VOL'] * df['REAL_YANG']).rolling(window=21).sum()
    df['VOL_YIN21'] = (df['VOL'] * df['REAL_YIN']).rolling(window=21).sum()
    df['VOL_YANG14'] = (df['VOL'] * df['REAL_YANG']).rolling(window=14).sum()
    df['VOL_YIN14'] = (df['VOL'] * df['REAL_YIN']).rolling(window=14).sum()

    df['VOL_FAKEYANG21'] = (df['VOL'] * df['FAKE_YANG']).rolling(window=21).sum()
    df['VOL_FAKEYIN21'] = (df['VOL'] * df['FAKE_YIN']).rolling(window=21).sum()
    df['VOL_FAKEYANG14'] = (df['VOL'] * df['FAKE_YANG']).rolling(window=14).sum()
    df['VOL_FAKEYIN14'] = (df['VOL'] * df['FAKE_YIN']).rolling(window=14).sum()
    df['VOL_FAKEYANG10'] = (df['VOL'] * df['FAKE_YANG']).rolling(window=10).sum()
    df['VOL_FAKEYIN10'] = (df['VOL'] * df['FAKE_YIN']).rolling(window=10).sum()

    # YANGYIN_OK: 阳线量能显著大于阴线量能
    yangyin_ok = (
            (df['VOL_YANG21'] > 1.5 * df['VOL_YIN21']) |
            (df['VOL_YANG14'] > 1.5 * df['VOL_YIN14']) |
            (df['VOL_FAKEYANG21'] > 1.5 * df['VOL_FAKEYIN21']) |
            (df['VOL_FAKEYANG14'] > 1.5 * df['VOL_FAKEYIN14']) |
            (df['VOL_FAKEYANG10'] > 1.8 * df['VOL_FAKEYIN10'])
    )
    df['YANGYIN_OK'] = yangyin_ok

    # ==================== 流动性与市值 ====================
    # A28:=MA(AMOUNT,28)/100000000;  28日均成交额（亿元）
    df['A28'] = df['AMOUNT'].rolling(window=28).mean() / 100000000
    lq = df['A28'] >= 0.005  # 大于5千万

    # MV:=C*CAPITAL*100/100000000;  流通市值（亿元）
    # 注意：CAPITAL在通达信中是流通股本（股），这里用最新数据估算
    # 由于akshare不直接提供流通股本，我们用volume/turnover_rate估算或跳过精确计算
    # 简化处理：假设流通股本使得市值大于30亿（这里用平均价格*成交量/换手率估算不准确，改为用价格*固定系数）
    # 实际上应该用stock_zh_a_spot_em获取实时流通市值，这里简化处理
    # 暂时用价格>5元且成交量较大来粗略代替，或跳过此条件
    # 更好的方法：从spot数据获取流通市值
    mvok = pd.Series(True, index=df.index)  # 暂时默认为True，后面从spot数据验证

    # ==================== 价格形态条件 ====================
    # O85:=LLV(O,28)+0.925*(HHV(O,28)-LLV(O,28));  开盘价28日区间的92.5%位置
    llv_o_28 = df['O'].rolling(window=28).min()
    hhv_o_28 = df['O'].rolling(window=28).max()
    df['O85'] = llv_o_28 + 0.925 * (hhv_o_28 - llv_o_28)
    top15o = df['O'] >= df['O85']  # 开盘价在高位

    # FD15:=C<REF(C,1) AND C<=O AND VOL>=1.15*REF(VOL,1);  下跌且放量
    fd15 = (df['C'] < df['REF_C_1']) & (df['C'] <= df['O']) & (df['VOL'] >= 1.15 * df['VOL'].shift(1))

    # GOOD28:=COUNT(TOP15O AND FD15,28)=0;  28日内无跳空高开且放量下跌
    good28 = ((top15o & fd15).rolling(window=28).sum() == 0)

    # MAXVOL28:=HHV(VOL,28);
    maxvol28 = df['VOL'].rolling(window=28).max()
    # MAX28_OK:=COUNT(VOL=MAXVOL28 AND REAL_YIN,28)=0;  28日无天量阴线
    max28_ok = (((df['VOL'] == maxvol28) & df['REAL_YIN']).rolling(window=28).sum() == 0)

    # ==================== 倍量柱条件 ====================
    # AVG40:=MA(VOL,40);
    df['AVG40'] = df['VOL'].rolling(window=40).mean()
    # PLRY:=VOL>1.8*REF(VOL,1) AND C>O AND VOL>AVG40;  倍量柱
    plry = (df['VOL'] > 1.8 * df['VOL'].shift(1)) & (df['C'] > df['O']) & (df['VOL'] > df['AVG40'])
    # PLRY_CNT:=COUNT(PLRY,28)>=1;  28日内有倍量柱
    plry_cnt = plry.rolling(window=28).sum() >= 1

    # ==================== 关键K条件 ====================
    # V40P:=SUM(REF(VOL,1),40)/40;  昨日开始的40日均量
    df['V40P'] = df['VOL'].shift(1).rolling(window=40).mean()
    # BD:=C>REF(C,1) AND C>=O;  上涨且收阳或平
    bd = (df['C'] > df['REF_C_1']) & (df['C'] >= df['O'])
    # BIGV:=VOL>1.75*V40P;  成交量大于40日均量75%
    bigv = df['VOL'] > 1.75 * df['V40P']
    # R55:=LLV(C,40)+0.55*(HHV(C,40)-LLV(C,40));  价格40日区间的55%位置
    llv_c_40 = df['C'].rolling(window=40).min()
    hhv_c_40 = df['C'].rolling(window=40).max()
    df['R55'] = llv_c_40 + 0.55 * (hhv_c_40 - llv_c_40)
    posok = df['C'] > df['R55']  # 价格在相对高位

    # TRIGGER:= PLRY_CNT OR (BD AND BIGV AND POSOK);  触发条件
    trigger = plry_cnt | (bd & bigv & posok)

    # ==================== MACD条件 ====================
    dif, dea, macd = calculate_macd(df)
    df['DIF'] = dif
    df['DEA'] = dea
    macd_ok = dif > 0  # MACD.DIF>0

    # ==================== 最终选股条件 ====================
    # XG:= J_OK AND TRIGGER AND LQ AND MVOK AND MAX28_OK AND YANGYIN_OK AND MACD.DIF>0;
    xg = j_ok & trigger & lq & mvok & max28_ok & yangyin_ok & macd_ok

    df['XG'] = xg

    # 获取最新一天的数据
    latest = df.iloc[-1]
    latest_date = latest['date']

    return {
        'date': latest_date,
        'stock_code': None,  # 由外部传入
        'selected': latest['XG'],
        'indicators': {
            'J': latest['J'],
            'J_OK': latest['XG'] if not pd.isna(latest['J']) else False,
            'TRIGGER': trigger.iloc[-1],
            'LQ': lq.iloc[-1],
            'MAX28_OK': max28_ok.iloc[-1],
            'YANGYIN_OK': yangyin_ok.iloc[-1],
            'MACD_DIF': latest['DIF'],
            'DIF>0': macd_ok.iloc[-1] if not pd.isna(latest['DIF']) else False
        },
        'raw_data': latest
    }


def get_circulating_market_cap(stock_code):
    """
    从实时行情获取流通市值（亿元）
    """
    try:
        # 使用akshare获取实时行情
        spot_df = ak.stock_zh_a_spot_em()
        stock_info = spot_df[spot_df['代码'] == stock_code]
        if not stock_info.empty:
            # 流通市值字段，单位亿元
            return float(stock_info['流通市值'].values[0]) / 100000000  # 转换为亿元
        return None
    except:
        return None


def run_stock_selection(data_dir="data", output_file="selected_stocks.xlsx"):
    """
    执行选股操作

    参数:
        data_dir: 历史数据存放目录
        output_file: 选股结果输出文件
    """
    # 检查数据目录是否存在
    if not os.path.exists(data_dir):
        print(f"错误：数据目录 {data_dir} 不存在！请先运行数据获取模块。")
        return

    # 获取所有Excel文件
    excel_files = glob.glob(os.path.join(data_dir, "*.xlsx"))
    if not excel_files:
        print(f"错误：在 {data_dir} 中没有找到Excel文件！请先运行数据获取模块。")
        return

    print(f"找到 {len(excel_files)} 只股票数据文件")
    print("开始选股分析...")

    selected_stocks = []
    failed_stocks = []

    # 获取实时流通市值数据（用于MVOK条件）
    print("正在获取实时流通市值数据...")
    try:
        spot_df = ak.stock_zh_a_spot_em()
        spot_df['流通市值亿'] = spot_df['流通市值'] / 100000000
        spot_dict = spot_df.set_index('代码')['流通市值亿'].to_dict()
    except:
        print("获取实时市值数据失败，将跳过市值筛选")
        spot_dict = {}

    for i, file_path in enumerate(excel_files, 1):
        stock_code = os.path.basename(file_path).replace('.xlsx', '')
        print(f"\n[{i}/{len(excel_files)}] 分析股票: {stock_code}")

        try:
            # 读取Excel数据
            df = pd.read_excel(file_path, engine='openpyxl')

            # 检查必要列是否存在
            required_cols = ['date', 'open', 'close', 'high', 'low', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                print(f"  跳过：缺少列 {missing_cols}")
                continue

            # 分析股票
            result = analyze_stock(df)
            if result is None:
                print(f"  跳过：数据不足")
                continue

            result['stock_code'] = stock_code

            # 检查流通市值条件（30亿以上）
            if stock_code in spot_dict:
                mv = spot_dict[stock_code]
                result['indicators']['MV'] = mv
                result['indicators']['MVOK'] = mv >= 30
                # 如果市值不足30亿，不选中
                if mv < 30:
                    result['selected'] = False
            else:
                result['indicators']['MV'] = None
                result['indicators']['MVOK'] = True  # 无法获取时默认通过

            if result['selected']:
                print(f"  *** 选中！ ***")
                print(f"      J值: {result['indicators']['J']:.2f}")
                print(f"      MACD DIF: {result['indicators']['MACD_DIF']:.4f}")
                selected_stocks.append(result)
            else:
                print(f"  未选中")
                # 打印未选中的原因
                ind = result['indicators']
                reasons = []
                if not ind.get('J_OK'): reasons.append(f"J>{13}")
                if not ind.get('TRIGGER'): reasons.append("无触发信号")
                if not ind.get('LQ'): reasons.append("流动性不足")
                if not ind.get('MAX28_OK'): reasons.append("28日有天量阴线")
                if not ind.get('YANGYIN_OK'): reasons.append("阴阳量不平衡")
                if not ind.get('DIF>0'): reasons.append("MACD.DIF<=0")
                if not ind.get('MVOK', True): reasons.append(f"市值{ind.get('MV', 0):.0f}亿<30亿")
                if reasons:
                    print(f"      原因: {', '.join(reasons)}")

        except Exception as e:
            print(f"  错误: {e}")
            failed_stocks.append(stock_code)

    # 输出结果
    print(f"\n{'=' * 60}")
    print(f"选股完成！")
    print(f"分析股票总数: {len(excel_files)}")
    print(f"选中股票数量: {len(selected_stocks)}")
    print(f"失败股票数量: {len(failed_stocks)}")

    if selected_stocks:
        # 整理结果DataFrame
        results_data = []
        for r in selected_stocks:
            row = {
                '股票代码': r['stock_code'],
                '日期': r['date'],
                'J值': r['indicators']['J'],
                'MACD_DIF': r['indicators']['MACD_DIF'],
                '流通市值(亿)': r['indicators'].get('MV'),
                '收盘价': r['raw_data']['close'],
                '成交量': r['raw_data']['volume']
            }
            results_data.append(row)

        result_df = pd.DataFrame(results_data)
        result_df.to_excel(output_file, index=False, engine='openpyxl')
        print(f"\n选股结果已保存至: {os.path.abspath(output_file)}")
        print("\n选中股票列表:")
        print(result_df.to_string())
    else:
        print("\n没有符合条件的股票")

    print(f"{'=' * 60}")