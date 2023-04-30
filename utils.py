import sys
import re
import time

import numpy as np
import pandas as pd

import akshare as ak
import baostock as bs

"""
命名：驼峰转下划线
"""
def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


"""
获取指数的成份股
    -- by akshare & baostock
"""
def get_index_components(index_code='399102'):
    stock_list = ak.index_stock_cons(index_code)["品种代码"].apply(ak.stock_a_code_to_symbol).apply(lambda s:s[:2]+'.'+s[2:]).values
    return np.unique(stock_list)


"""
获取股票列表的历史财务指标
    -- by baostock
"""
def df_multi_merge(df_list, on_cols, how='left'):
    for i, df in enumerate(df_list):
        if i > 0:
            df_output = pd.merge(df_output, df, on=on_cols, how=how)
        else:
            df_output = df.copy()
    return df_output


def get_rs_target(stock_list, year, quarter, target='profit'):
    if target == 'profit':
        api_query_target = bs.query_profit_data
    elif target == 'operation':
        api_query_target = bs.query_operation_data
    elif target == 'growth':
        api_query_target = bs.query_growth_data
    elif target == 'balance':
        api_query_target = bs.query_balance_data
    elif target == 'cashflow':
        api_query_target = bs.query_cash_flow_data
    elif target == 'dupont':
        api_query_target = bs.query_dupont_data
    else:
        raise Exception('this target is not implemented, pls change.')
    output_list = []
    # len_ = len(stock_list)
    for i, stk in enumerate(stock_list):
        # print("\r", end="")
        # print(f"   {target}:{100 * (1 + i) / len_:>0.2f}%", "▋" * (((i + 1) * 100) // len_), end="")
        # sys.stdout.flush()
        # 查询季频估值指标盈利能力
        target_list = []
        rs_target = api_query_target(code=stk, year=year, quarter=quarter)
        while (rs_target.error_code == '0') & rs_target.next():
            target_list.append(rs_target.get_row_data())
        output_list.append(pd.DataFrame(target_list, columns=rs_target.fields))
    return pd.concat(output_list)


def get_finance_indicator(stock_list, stt_date, end_date):
    bs.login()
    all_output_list = []
    yq_list = [(dd.year, dd.quarter) for dd in pd.date_range(stt_date, end_date, freq='Q')]
    for (y, q) in yq_list:
        t0 = time.time()
        print(f'-- 正在获取{y}Q{q}...')
        yq_output_list = []
        for target in ['profit', 'operation', 'growth', 'balance', 'cashflow', 'dupont']:
            yq_output_list.append(get_rs_target(stock_list, y, q, target))
            # print('')
        all_output_list.append(df_multi_merge(yq_output_list, on_cols=['code', 'pubDate', 'statDate']))
        print(f"--     耗时：{(time.time()-t0)/60:>0.2f} mins")
    df_output = pd.concat(all_output_list)
    # 格式转换
    df_output['pubDate'] = df_output['pubDate'].astype('datetime64[ns]')
    df_output['statDate'] = df_output['statDate'].astype('datetime64[ns]')
    for col in np.setdiff1d(df_output.columns, ['code', 'pubDate', 'statDate']):
        df_output[col] = pd.to_numeric(df_output[col])
    # 字段名转换
    df_output.columns = df_output.columns.map(camel_to_snake)
    bs.logout()
    return df_output



