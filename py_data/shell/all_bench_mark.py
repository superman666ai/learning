#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/22 15:20
# @Author  : GaoJian
# @File    : benchmark_years.py
# 计算基金经理任期 的基准
import math
import os
import pandas as pd
import numpy as np
import datetime
import logging
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from base_utils import cu, cu1, fund_db_pra, fund_db_wind


filename = os.path.split(__file__)[-1].split(".")[0]
logging.basicConfig(level=logging.DEBUG,  # 控制台打印的日志级别
                    filename=filename + '.log',
                    filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                    # a是追加模式，默认如果不写的话，就是追加模式
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    # 日志格式
                    )


def get_tradedate(zrr):
    cu1.execute('select jyr from pra_info.txtjyr t where t.zrr=:rq', rq=zrr)
    rs = cu1.fetchall()[0][0]
    return rs


def zhoubodong_qz(code='163807', start_date='20190101', end_date='20190225'):
    sql = '''
        select
        f3_1288 as 截止日期, f2_1288 as 复权单位净值 
        from
        wind.TB_OBJECT_1288
        where 
        f1_1288 ='S3600328'
        and
        f3_1288 >= '%(start_date)s'
        and
        f3_1288 <= '%(end_date)s' order by f3_1288
        ''' % {'end_date': end_date, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)

    fund_price2['fund_return'] = fund_price2.复权单位净值.diff() / fund_price2.复权单位净值.shift(1)
    fund_price2.dropna(axis=0, inplace=True)
    fund_price2.reset_index(drop=True, inplace=True)


    zhou_bodong = fund_price2.fund_return.std() * (math.sqrt(250))

    return zhou_bodong


def fund_performance_qz(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
     select
        f3_1288 as 截止日期, f2_1288 as 复权单位净值 
        from
        wind.TB_OBJECT_1288
        where 
        f1_1288 ='S3600328'
        and
        f3_1288 >= '%(start_date)s'
        and
        f3_1288 <= '%(end_date)s' order by f3_1288 desc
        ''' % {'end_date': end_date, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        return 0
    price_list = fund_price['复权单位净值'].tolist()

    performance = (price_list[0] - price_list[-1]) / price_list[-1]

    return performance


def max_down_fund_qz(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
    select
        f3_1288 as 截止日期, f2_1288 as 复权单位净值 
        from
        wind.TB_OBJECT_1288
        where 
        f1_1288 ='S3600328'
        and
        f3_1288 >= '%(start_date)s'
        and
        f3_1288 <= '%(end_date)s'  order by f3_1288
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
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


def zhoubodong_zh(code='163807', start_date='20190101', end_date='20190225'):
    sql = '''
     select t1.f2_1120 as 截止日期, t1.f8_1120 as 复权单位净值
        from tb_object_1120 t1, TB_OBJECT_1090 t2
         where t1.f1_1120 = t2.F2_1090
           and t2.f16_1090 = '%(code)s'
           AND f4_1090 = 'S'
           and t1.f2_1120 >= '%(start_date)s'
           and t1.f2_1120 <= '%(end_date)s'
        ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)

    fund_price2['fund_return'] = fund_price2.复权单位净值.diff() / fund_price2.复权单位净值.shift(1)
    fund_price2.dropna(axis=0, inplace=True)
    fund_price2.reset_index(drop=True, inplace=True)

    zhou_bodong = fund_price2.fund_return.std() * (math.sqrt(250))

    return zhou_bodong


def fund_performance_zh(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
     select t1.f2_1120 as 截止日期, t1.f8_1120 as 复权单位净值
        from tb_object_1120 t1, TB_OBJECT_1090 t2
         where t1.f1_1120 = t2.F2_1090
           and t2.f16_1090 = '%(code)s'
           AND f4_1090 = 'S'
           and t1.f2_1120 >= '%(start_date)s'
           and t1.f2_1120 <= '%(end_date)s'
         ORDER BY t1.f2_1120 DESC
''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        return 0
    price_list = fund_price['复权单位净值'].tolist()

    performance = (price_list[0] - price_list[-1]) / price_list[-1]

    return performance


def max_down_fund_zh(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
     select t1.f2_1120 as 截止日期, t1.f8_1120 as 复权单位净值
        from tb_object_1120 t1, TB_OBJECT_1090 t2
         where t1.f1_1120 = t2.F2_1090
           and t2.f16_1090 = '%(code)s'
           AND f4_1090 = 'S'
           and t1.f2_1120 >= '%(start_date)s'
           and t1.f2_1120 <= '%(end_date)s'
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
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


def max_down_fund(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
    select
    trade_dt as 截止日期, s_dq_close as 复权单位净值 
    from
    wind.chinamutualfundbenchmarkeod
    where 
    s_info_windcode= '%(code)s'
    and
    trade_dt >= '%(start_date)s'
    and
    trade_dt <= '%(end_date)s'
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
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


def date_gen(days=None, months=None, years=None, end=None):
    """生成指定日期距离最近的交易日期
        功能
        --------
        生成指定日期之前几年/月/日距离最近的交易日期

        参数
        --------
        days:需要往前多少个日历日，格式为int，可以为具体数字，也可为变量
        months:需要往前多少个月份，格式为int，可以为具体数字，也可为变量
        years:需要往前多少个年份，格式为int，可以为具体数字，也可为变量
        end:截止日期，格式为'20171220'，以该截止日期往前距离XX日、XX月、XX年后最近的交易日期
        参数需要写全，如days=3。days、months、years必须输入一个，end也是必要参数

        返回值
        --------
        返回一个具体的日期，返回格式为字符格式，如'20181011'。

        参看
        --------
        if_trade(start_date)：关联函数。

        示例
        --------
        >>>a = date_gen(days = 3,end = '20181220')
        >>>a
        '20181217'

        """

    def none(par):
        if not par:  # 意思是如果par为None时
            par = 0
        return par

    [days, months, years] = [none(days), none(months), none(years)]
    end = pd.to_datetime(end)
    start_date = (end - relativedelta(days=days, months=months, years=years)).strftime('%Y%m%d')
    return start_date


def zhoubodong(code='163807', start_date='20190101', end_date='20190225'):
    sql = '''
        select
        trade_dt as 截止日期, s_dq_close as 复权单位净值 
        from
        wind.chinamutualfundbenchmarkeod
        where 
        s_info_windcode= '%(code)s'
        and
        trade_dt >= '%(start_date)s'
        and
        trade_dt <= '%(end_date)s'
        ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        return 0
    fund_price2 = fund_price.sort_values(by=['截止日期']).reset_index(drop=True)

    fund_price2['fund_return'] = fund_price2.复权单位净值.diff() / fund_price2.复权单位净值.shift(1)
    fund_price2.dropna(axis=0, inplace=True)
    fund_price2.reset_index(drop=True, inplace=True)

    # zhou_fund_price = pd.DataFrame(fund_price2.iloc[0,:]).T
    # for i in fund_price2.index:
    #     if i>0 and i%1==0:
    #         zhou_fund_price = zhou_fund_price.append(fund_price2.iloc[i,:])
    #     else:
    #         pass
    zhou_bodong = fund_price2.fund_return.std() * (math.sqrt(250))

    return zhou_bodong


def fund_performance(code='163807', start_date='20150528', end_date='20190225'):
    # 输出单只基金的最大回撤，返回一个float数值
    # 提取复权净值
    sql = '''
     select
    trade_dt as 截止日期, s_dq_close as 复权单位净值 
    from
    wind.chinamutualfundbenchmarkeod
    where 
    s_info_windcode= '%(code)s'
    and
    trade_dt >= '%(start_date)s'
    and
    trade_dt <= '%(end_date)s'  order by trade_dt desc
    ''' % {'end_date': end_date, 'code': code, 'start_date': start_date}
    fund_price = pd.DataFrame(cu.execute(sql).fetchall(), columns=['截止日期', '复权单位净值'])
    if (fund_price.empty):
        return 0
    price_list = fund_price['复权单位净值'].tolist()

    performance = (price_list[0] - price_list[-1]) / price_list[-1]

    return performance

def copy_after_data():
    """
    复制表
    :return:
    """

    update_time = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')

    sql = "select max(t.update_time) from FUND_BENCHMARK_PERFORMANCE_bak t where t.trade_date='0'"
    try:
        after_time = cu1.execute(sql).fetchall()[0][0]
    except Exception as e:
        print(e)
        print("no max (update_time) copy fail")
        return

    # 查询是否有记录 没有就copy
    exists_sql = '''
                    select * from FUND_BENCHMARK_PERFORMANCE_bak t
                    where t.trade_date = '0'
                    and t.update_time = '{}'
                  '''.format(update_time)

    if (len(cu1.execute(exists_sql).fetchall()) != 0):
        print("don't copy, {} already have data".format(update_time))
        return

    sql = """
            select 
            fundcode,
            fundname,
            fundtype, 
            fundmanager,
            rrin_hc,
            rrin_nhbd,
            rrin_hb,
            manager_startdate,
            manager_enddate,
            index_code,
            fundmanager_id,
            trade_date,
            update_time
            from FUND_BENCHMARK_PERFORMANCE_bak t
            where t.trade_date = '0'
            and t.update_time = '{}'
          """.format(after_time)

    df = pd.DataFrame(cu1.execute(sql).fetchall(), columns=['fundcode',
                                                            'fundname',
                                                            'fundtype',
                                                            'fundmanager',
                                                            'rrin_hc',
                                                            'rrin_nhbd',
                                                            'rrin_hb',
                                                            'manager_startdate',
                                                            'manager_enddate',
                                                            'index_code',
                                                            'fundmanager_id',
                                                            'trade_date',
                                                            'update_time'
                                                            ])

    # df.loc[df['end'] == '0', 'end'] = '20181231'
    if df.empty:
        print("after_time:{}  no value".format(after_time))
        return


    df['update_time'] = update_time
    insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                 "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date,update_time) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)"



    param = [tuple(x) for x in df.values]


    cu1.executemany(insert_sql, param)
    fund_db_pra.commit()
    print("copy OK")


fund_result = pd.DataFrame(
    columns=['基金代码', '基金简称', '二级分类', '基金经理', '任职以来最大回撤', '任职以来波动率', '任职以来回报'])


sql = """select cpdm, jjjc, yjfl, ejfl, clr from fund_classify"""

data = pd.DataFrame(cu1.execute(sql).fetchall(),
                    columns=['基金代码', '基金简称', '一级分类', '二级分类', '成立日'])


data_kind = data[['基金代码', '基金简称', '一级分类', '二级分类', '成立日']]


def main(data_kind):

    update_time = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    num = len(data_kind.index)
    for i in data_kind.index:

        try:

            print('第 {} 个----------总数{}'.format(i, num))

            fund_code = data_kind.基金代码[i]
            #
            # if fund_code != '510070':
            #     continue

            query_sql = '''
                        SELECT t1.f16_1090, t.f2_1272, nvl(t.f3_1272,0), nvl(t.f4_1272,0),t.F11_1272
                        from wind.TB_OBJECT_1272 t, wind.tb_object_1090 t1
                        where t.f1_1272 = t1.f2_1090  and t1.f4_1090 = 'J'
                        and t1.f16_1090 = '%(fund_code)s'  order by t.f3_1272
                    ''' % {'fund_code': fund_code}

            fund_manager = pd.DataFrame(cu.execute(query_sql).fetchall(),
                                        columns=['fundid', 'name', 'start', 'end', 'managerid'])


            for j in range(len(fund_manager.values)):
                fund_benchmark = fund_manager.values[j][0]

                print("fund_benchmark", fund_benchmark)
                benchmark_code = fund_benchmark + 'BI.WI'

                begin_date = fund_manager.values[j][2]
                manager_enddate = fund_manager.values[j][3]
                end_date = manager_enddate
                trade_date = '0'


                print('fundcode:{}  fund_manager:{}  begin: {}  end: {}'.format(fund_code,
                                                                              fund_manager.values[j][4],
                                                                              begin_date, end_date))


                # 删除还在任职的
                del_sql = '''delete from FUND_BENCHMARK_PERFORMANCE_bak t 
                             where t.fundcode= '%(fund_code)s'
                             and t.fundmanager= '%(fundmanager)s' 
                             and trade_date ='%(trade_date)s'
                             and t.manager_startdate = '%(manager_startdate)s'
                             and t.update_time = '%(update_time)s'
                             ''' % {'fund_code': fund_benchmark,
                                    'fundmanager': fund_manager.values[j][1],
                                    'trade_date': trade_date,
                                    'manager_startdate': fund_manager.values[j][2],
                                    'update_time': update_time}

                cu1.execute(del_sql)
                fund_db_pra.commit()

                # 查询是否存在
                record_sql = '''select manager_enddate 
                                from FUND_BENCHMARK_PERFORMANCE_bak t 
                                where t.fundcode= '%(fund_code)s'
                                and t.fundmanager= '%(fundmanager)s'
                                and trade_date ='%(trade_date)s'
                                and t.manager_startdate = '%(manager_startdate)s'
                                and t.update_time = '%(update_time)s' 
                                 ''' % {'fund_code': fund_benchmark,
                                        'fundmanager': fund_manager.values[j][1],
                                        'trade_date': trade_date,
                                        'manager_startdate': fund_manager.values[j][2],
                                        'update_time': update_time
                                        }

                manager_performance = pd.DataFrame(cu1.execute(record_sql).fetchall())

                flag = manager_performance.empty

                if (not flag):
                    print('任期已计算')
                else:

                    # 日期处理
                    if end_date == "0":
                        # 日期 为 0 补充为最新日期 前一天
                        end_date = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')

                    if (data_kind.成立日[i] == begin_date):
                        begin_date = get_tradedate(begin_date)
                    else:
                        tansferdate = (datetime.datetime.strptime(begin_date, '%Y%m%d') + timedelta(days=-1)).strftime('%Y%m%d')

                        if (get_tradedate(tansferdate) == 0):
                            start = tansferdate
                        else:
                            start = get_tradedate(tansferdate)

                    if data_kind.一级分类[i] == 'QDII':
                        end_date = get_tradedate(
                            (datetime.datetime.strptime(end_date, '%Y%m%d') + datetime.timedelta(days=-1)).strftime('%Y%m%d'))
                    elif data_kind.一级分类[i] == 'QDII':
                        end_date = get_tradedate(end_date)

                    # 计算
                    result_hc = max_down_fund(benchmark_code, begin_date, end_date)
                    result_nhbd = zhoubodong(benchmark_code, begin_date, end_date)
                    if (math.isnan(result_nhbd)):
                        result_nhbd = 0
                    result_hb = fund_performance(benchmark_code, begin_date, end_date)
                    rec = []
                    rec.append(data_kind.基金代码[i])
                    rec.append(data_kind.基金简称[i])
                    rec.append(data_kind.二级分类[i])
                    rec.append(fund_manager.values[j][1])
                    rec.append(result_hc)
                    rec.append(result_nhbd)
                    rec.append(result_hb)
                    rec.append(fund_manager.values[j][2])
                    rec.append(fund_manager.values[j][3])
                    rec.append(benchmark_code)
                    rec.append(fund_manager.values[j][4])
                    rec.append(trade_date)
                    rec.append(update_time)
                    insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                                 "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date,update_time) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)"
                    cu1.execute(insert_sql, rec)

                    if (data_kind.二级分类[i] == '可转债'):
                        result_hc = max_down_fund_zh('000832', begin_date, end_date)
                        result_nhbd = zhoubodong_zh('000832', begin_date, end_date)
                        if (math.isnan(result_nhbd)):
                            result_nhbd = 0
                        result_hb = fund_performance_zh('000832', begin_date, end_date)
                        rec = []
                        rec.append(data_kind.基金代码[i])
                        rec.append(data_kind.基金简称[i])
                        rec.append(data_kind.二级分类[i])
                        rec.append(fund_manager.values[j][1])
                        rec.append(result_hc)
                        rec.append(result_nhbd)
                        rec.append(result_hb)
                        rec.append(fund_manager.values[j][2])
                        rec.append(fund_manager.values[j][3])
                        rec.append('000832')
                        rec.append(fund_manager.values[j][4])
                        rec.append(trade_date)
                        rec.append(update_time)

                        insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                                     "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date,update_time) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)"
                        cu1.execute(insert_sql, rec)
                    elif (data_kind.二级分类[i] == '激进债券型' or data_kind.二级分类[i] == '普通债券型' or data_kind.二级分类[i] == '纯债'):
                        result_hc = max_down_fund_qz('CBA00203', begin_date, end_date)
                        result_nhbd = zhoubodong_qz('CBA00203', begin_date, end_date)
                        if (math.isnan(result_nhbd)):
                            result_nhbd = 0
                        result_hb = fund_performance_qz('CBA00203', begin_date, end_date)
                        rec = []
                        rec.append(data_kind.基金代码[i])
                        rec.append(data_kind.基金简称[i])
                        rec.append(data_kind.二级分类[i])
                        rec.append(fund_manager.values[j][1])
                        rec.append(result_hc)
                        rec.append(result_nhbd)
                        rec.append(result_hb)
                        rec.append(fund_manager.values[j][2])
                        rec.append(fund_manager.values[j][3])
                        rec.append('CBA00203')
                        rec.append(fund_manager.values[j][4])
                        rec.append(trade_date)
                        rec.append(update_time)
                        insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                                     "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date,update_time) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)"
                        cu1.execute(insert_sql, rec)
                    elif (data_kind.二级分类[i] == '股票型' or data_kind.二级分类[i] == '激进配置型' or
                          data_kind.二级分类[i] == '标准配置型' or data_kind.二级分类[i] == '保守配置型' or data_kind.二级分类[i] == '灵活配置型'):
                        code_list = ['000300', '000905', '000906', 'CBA00203']
                        for index in range(len(code_list)):
                            if (code_list[index] == 'CBA00203'):
                                result_hc = max_down_fund_qz(code_list[index], begin_date, end_date)
                                result_nhbd = zhoubodong_qz(code_list[index], begin_date, end_date)
                                if (math.isnan(result_nhbd)):
                                    result_nhbd = 0
                                result_hb = fund_performance_qz(code_list[index], begin_date, end_date)
                            else:
                                result_hc = max_down_fund_zh(code_list[index], begin_date, end_date)
                                result_nhbd = zhoubodong_zh(code_list[index], begin_date, end_date)
                                if (math.isnan(result_nhbd)):
                                    result_nhbd = 0
                                result_hb = fund_performance_zh(code_list[index], begin_date, end_date)
                            rec = []
                            rec.append(data_kind.基金代码[i])
                            rec.append(data_kind.基金简称[i])
                            rec.append(data_kind.二级分类[i])
                            rec.append(fund_manager.values[j][1])
                            rec.append(result_hc)
                            rec.append(result_nhbd)
                            rec.append(result_hb)
                            rec.append(fund_manager.values[j][2])
                            rec.append(fund_manager.values[j][3])
                            rec.append(code_list[index])
                            rec.append(fund_manager.values[j][4])
                            rec.append(trade_date)
                            rec.append(update_time)

                            insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                                         "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date,update_time) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)"
                            cu1.execute(insert_sql, rec)
                    else:
                        result_hc = max_down_fund_zh('000300', begin_date, end_date)
                        result_nhbd = zhoubodong_zh('000300', begin_date, end_date)
                        if (math.isnan(result_nhbd)):
                            result_nhbd = 0
                        result_hb = fund_performance_zh('000300', begin_date, end_date)
                        rec = []
                        rec.append(data_kind.基金代码[i])
                        rec.append(data_kind.基金简称[i])
                        rec.append(data_kind.二级分类[i])
                        rec.append(fund_manager.values[j][1])
                        rec.append(result_hc)
                        rec.append(result_nhbd)
                        rec.append(result_hb)
                        rec.append(fund_manager.values[j][2])
                        rec.append(fund_manager.values[j][3])
                        rec.append('000300')
                        rec.append(fund_manager.values[j][4])
                        rec.append(trade_date)
                        rec.append(update_time)
                        insert_sql = "INSERT INTO FUND_BENCHMARK_PERFORMANCE_bak( fundcode, fundname,fundtype, fundmanager,rrin_hc," \
                                     "rrin_nhbd,rrin_hb,manager_startdate,manager_enddate,index_code,fundmanager_id,trade_date,update_time) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)"
                        cu1.execute(insert_sql, rec)
                    fund_db_pra.commit()

        except Exception as e:
            print(e)
            logging.exception(str(fund_code))
            logging.exception(e)
            continue


    cu.close()
    cu1.close()
    fund_db_pra.close()
    fund_db_wind.close()

if __name__ == '__main__':

    # 复制前一天的数据
    copy_after_data()

    import multiprocessing

    data_kind = data_kind


    # main(data_kind)

    # 开启多进程计算
    import multiprocessing

    for i in range(0, len(data_kind), len(data_kind)//32):
        j = i + len(data_kind)//32
        mp = multiprocessing.Process(target=main, args=(data_kind[i:j],))
        mp.start()

