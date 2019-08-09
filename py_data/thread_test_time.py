#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/30 14:34
# @Author  : GaoJian
# @File    : aps_info_all_insert.py
import pandas as pd
import cx_Oracle
import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from queue import Queue



def db_cnn():
    # # 连pro 旧库
    [userName, password, hostIP, dbName] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
    fund_db_pra = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
    [userName, password, hostIP, dbName] = ['wind', 'wind', '172.16.125.222', 'pra']
    fund_db_wind = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
    cu = fund_db_wind.cursor()
    cu1 = fund_db_pra.cursor()
    return cu, cu1


def update_since_all(row):
    """"""
    try:
        update_time = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')

        cu, cu1 = db_cnn()

        for i in range(10):
            exists_sql = """select *  from  MANAGER_INFO_RANK_bak t 
                            where t.cycle_value ='0' and t.update_time = '20190807'"""
            cu1.execute(exists_sql).fetchall()

            exists_sql = """select f14_1101， f13_1101 as 截止日期, f21_1101 as 复权单位净值 
                        from wind.tb_object_1101
                        left join wind.tb_object_1090
                        
                        on f2_1090 = f14_1101
                        
                        where f16_1101='501303'
                        and
                        (F13_1101 >= '20170921'
                        or
                        f13_1101 <= '20171229')
                        """
            cu.execute(exists_sql).fetchall()

    except Exception as e:
        print(e)

    finally:
        cu.close()
        cu1.close()



q = Queue()


def worker():
    row = q.get()

    # 查询是否计算过
    update_since_all(row)

def simple_worker(df):
    for index, row in df.iterrows():
        update_since_all(row)

if __name__ == '__main__':

    print("----数据准备中----")

    queue = Queue()

    df = find_all_code()
    # fundcode_list = df.fundcode.values
    fundcode_list = ["502049", "511220", "501301"]

    manage_df = pd.DataFrame()
    for i in fundcode_list:
        m_df = find_manager(i)
        manage_df = pd.concat([manage_df, m_df])

    df = pd.merge(df, manage_df, how="outer", on='fundcode')

    # 删除没有基金经理的基金

    df.dropna(subset=['managername', 'managerid'], inplace=True)
    t1 = datetime.datetime.now()
    simple_worker(df)
    t2 = datetime.datetime.now()
    print("simple times:{}".format(t2 - t1))


    for index, row in df.iterrows():
        q.put(row)

    t1 = datetime.datetime.now()
    with ThreadPoolExecutor(3) as t:
        for i in range(3):
            t.submit(worker)
    t2 = datetime.datetime.now()
    print("times:{}".format(t2 - t1))
