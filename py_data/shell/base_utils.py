#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/8/1 10:53
# @Author  : GaoJian
# @File    : base_utils.py

import pandas as pd
import cx_Oracle
import numpy as np
import math

# # 连pro 旧库
# [userName, password, hostIP, dbName] = ['pra_info', 'pra_info', '172.16.126.23:1521', 'pra']
# fund_db_pra = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
#

# 连pro 演练
[userName, password, hostIP, dbName] = ['pra_info', 'pra_info', '172.16.125.222', 'pra']
fund_db_pra = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)


# wande 测试
# [userName, password, hostIP, dbName, tablePrefix] = ['cjcs_wl', 'cjcs_wl', '172.16.121.198:1521', 'dfcfCS', 'wind']
[userName, password, hostIP, dbName] = ['wind', 'wind', '172.16.125.222', 'pra']
fund_db_wind = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)


# # 连接数据库 wande 生产
# [userName, password, hostIP, dbName, tablePrefix] = ['reader', 'reader', '172.16.50.232:1521', 'dfcf', 'wind']
# fund_db_wind = cx_Oracle.connect(user=userName, password=password, dsn=hostIP + '/' + dbName)
#

cu = fund_db_wind.cursor()
cu1 = fund_db_pra.cursor()

def get_tradedate(zrr):
    cu1.execute('select jyr from pra_info.txtjyr t where t.zrr=:rq', rq=zrr)
    try:
        rs = cu1.fetchall()[0][0]
    except:
        rs = 0
    return rs


def deal_time(st_date=None, ed_date=None):
    """
    返回一个可迭代对象
    :param st_date:
    :param ed_date:
    :return:
    """
    rpts = {}

    for j in range(int(st_date[0:4]), int(ed_date[0:4]) + 1, 1):
        if j == int(st_date[0:4]):
            rpts[str(j)] = [st_date, str(j) + '1231']
        elif j == int(ed_date[0:4]):
            rpts[str(j)] = [str(j) + '0101', ed_date]
        else:
            rpts[str(j)] = [str(j) + '0101', str(j) + '1231']

    return rpts.items()


def find_rehab_value(fundcode, start, end):
    """
    查询复权净值
    :param fundcode:
    :param start:
    :param end:
    :return:
    """
    sql = '''
            select f13_1101 as 截止日期, f21_1101 as 复权单位净值 
            from wind.tb_object_1101
            left join wind.tb_object_1090
            on f2_1090 = f14_1101
            where F16_1090= '%(code)s'
            and F13_1101 >= '%(start_date)s' 
            and f13_1101 <= '%(end_date)s' order by F13_1101 desc
            ''' % {'end_date': end, 'code': fundcode, 'start_date': start}

    df = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    return df


def find_all_code():
    sql = """select cpdm, jjjc, yjfl, ejfl, clr from fund_classify"""
    df = pd.DataFrame(cu1.execute(sql).fetchall(),
                      columns=['fundcode', 'fundname', 'yjfl', 'ejfl', 'clr'])
    return df


def find_manager(fundcode):
    query_sql = '''SELECT t1.f16_1090, t.f2_1272, nvl(t.f3_1272,0), nvl(t.f4_1272,0), t.F11_1272
                from wind.TB_OBJECT_1272 t, wind.tb_object_1090 t1
                where t.f1_1272 = t1.f2_1090  and t1.f4_1090 = 'J'
                   and t1.f16_1090 = '%(fund_code)s'    order by t.f3_1272
                ''' % {'fund_code': fundcode}
    df = pd.DataFrame(cu.execute(query_sql).fetchall(),
                      columns=['fundcode', 'managername', 'start', 'end', 'managerid'])
    return df


def max_down_fund(code='163807', start_date='20150528', end_date='20190225'):
    sql = '''
    select
    f13_1101 as 截止日期, f21_1101 as 复权单位净值 
    from
    wind.tb_object_1101
    left join wind.tb_object_1090
    on f2_1090 = f14_1101
    where 
    F16_1090= '%(code)s'
    and
    F13_1101 >= '%(start_date)s'
    and
    f13_1101 <= '%(end_date)s'
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        print("复权净值为空")
        return 0
    fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)
    price_list = fund_price2['复权单位净值'].tolist()
    i = np.argmax((np.maximum.accumulate(price_list) - price_list) / np.maximum.accumulate(price_list))  # 结束位置
    if i == 0:
        max_down_value = 0
    else:
        j = np.argmax(price_list[:i])  # 开始位置
        max_down_value = (price_list[j] - price_list[i]) / (price_list[j])
    return -max_down_value


def zhoubodong(code='163807', start_date='20190101', end_date='20190225'):
    sql = '''
    select
    f13_1101 as 截止日期, f21_1101 as 复权单位净值 
    from
    wind.tb_object_1101
    left join wind.tb_object_1090
    on f2_1090 = f14_1101
    where 
    F16_1090= '%(code)s'
    and
    F13_1101 >= '%(start_date)s'
    and
    f13_1101 <= '%(end_date)s'
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        print("复权净值为空", code, start_date, end_date)
        return 0

    fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)

    fund_price2['fund_return'] = fund_price2.复权单位净值.diff() / fund_price2.复权单位净值.shift(1)
    fund_price2.dropna(axis=0, inplace=True)
    fund_price2.reset_index(drop=True, inplace=True)
    zhou_bodong = fund_price2.fund_return.std() * (math.sqrt(250))

    if str(zhou_bodong) == 'nan':
        return 0

    return zhou_bodong


def fund_performance(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
    select
    f13_1101 as 截止日期, f21_1101 as 复权单位净值 
    from
    wind.tb_object_1101
    left join wind.tb_object_1090
    on f2_1090 = f14_1101
    where 
    F16_1090= '%(code)s'
    and
   (F13_1101 >= '%(start_date)s'
    and
    f13_1101 <= '%(end_date)s') order by F13_1101 desc
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        print("复权净值为空")
        return 0

    price_list = fund_price['复权单位净值'].tolist()

    performance = (price_list[0] - price_list[-1]) / price_list[-1]

    return performance


class RankInfo(object):
    def __init__(self, fundcode, ejfl, start, end):
        self.fundcode = fundcode
        self.ejfl = ejfl
        self.start = start
        self.end = end

        self.hc = max_down_fund(fundcode, start, end)
        self.hb = fund_performance(fundcode, start, end)
        self.bd = zhoubodong(fundcode, start, end)

        self.hc_rank = 1
        self.hb_rank = 1
        self.bd_rank = 1

        self.find_rank()

    def find_status(self, fundcode):
        judgesql = '''
                 select
                 f13_1101 as 截止日期, f21_1101 as 复权单位净值 
                 from
                 wind.tb_object_1101
                 left join wind.tb_object_1090
                 on f2_1090 = f14_1101
                 where 
                 F16_1090= '%(code)s'
                 and
                 (F13_1101 = '%(start_date)s'
                 or
                 f13_1101 = '%(end_date)s')
                 ''' % {'end_date': self.end, 'code': fundcode, 'start_date': self.start}

        judgeresult = pd.DataFrame(cu.execute(judgesql).fetchall(), columns=['截止日期', '复权单位净值'])

        # 出自己之外 其他的基金净值小于2 的为空
        if (len(judgeresult) < 2) and fundcode != self.fundcode:
            return np.nan
        return True

    def same_ejfl(self):

        sql = """select cpdm, jjjc, yjfl, ejfl, clr 
                 from fund_classify t
                 where t.ejfl='{}'""".format(self.ejfl)

        df = pd.DataFrame(cu1.execute(sql).fetchall(),
                          columns=['fundcode', 'fundname', 'yjfl', 'ejfl', 'clr'])

        return df

    def find_rank(self):

        df = self.same_ejfl()

        if df.empty:
            return

        status_list = []
        for i in df.fundcode.values:
            status_list.append(self.find_status(i))

        df["status"] = status_list

        # 去除不是同一时间段的同类
        df.dropna(axis=0, inplace=True)

        if len(df) == 1:
            return

        del df["status"]

        max_dow_list = []
        zhoubodong_list = []
        fund_performance_list = []

        for i in df.fundcode.values:
            max_dow_list.append(max_down_fund(i, self.start, self.end))
            zhoubodong_list.append(zhoubodong(i, self.start, self.end))
            fund_performance_list.append(fund_performance(i, self.start, self.end))



        # 去除空值  排序
        df.dropna(axis=0, inplace=True)

        if df.empty:
            return

        if len(df) == 1:
            self.hc_rank = 0.01
            self.hb_rank = 0.01
            self.bd_rank = 0.01
            return

        df.reset_index(drop=True, inplace=True)

        df["max_down"] = max_dow_list
        df["zhoubodong"] = zhoubodong_list
        df["fund_performance"] = fund_performance_list

        df['max_down_rank'] = df['max_down'].rank(ascending=False, method='max') / len(df['max_down'])
        df['zhoubodong_rank'] = df['zhoubodong'].rank(ascending=True, method='min') / len(df['zhoubodong'])
        df['fund_performance_rank'] = df['fund_performance'].rank(ascending=False, method='min') / len(
            df['fund_performance'])

        df = df.loc[df['fundcode'] == self.fundcode]

        self.hc_rank = df.values[0][-3]
        self.bd_rank = df.values[0][-2]
        self.hb_rank = df.values[0][-1]
