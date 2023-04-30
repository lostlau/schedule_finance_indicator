from sqlalchemy import create_engine
from datetime import date,timedelta
import pandas as pd
from utils import get_index_components, get_finance_indicator

def job():
    # 初始化数据库连接，使用pymysql模块
    user = "lostlau"
    pwd = "Liuwn_0717"
    ip = "110.40.223.131"
    port = "3306"
    db_name = "db_quant"

    today = date.today().strftime('%Y-%m-%d')
    date_startfrom = (date.today() - timedelta(days=183)).strftime('%Y-%m-%d')

    print(f"")
    print(f"{today}")
    stock_list = get_index_components(index_code='399102')

    # 读取数据库中的code和stat_date作为联合uid
    engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{ip}:{port}/{db_name}")
    df_uid = pd.read_sql("select code, stat_date from stock_finance_indicator", engine)
    uid_list = (df_uid['code'] + "-" + df_uid['stat_date'].dt.strftime('%Y-%m')).values
    # 获取数据
    df_output = get_finance_indicator(stock_list, stt_date=date_startfrom, end_date=today)
    # 通过uid去除重复
    df_output_final = df_output[~(df_output['code'] + "-" + df_output['stat_date'].dt.strftime('%Y-%m')).isin(uid_list)]

    engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{ip}:{port}/{db_name}")
    df_output_final.to_sql('stock_finance_indicator', engine, if_exists='append',index=False,chunksize=1000)
    print(f"最终写入行数：{df_output_final.shape[0]}")
    print("写入成功！")

