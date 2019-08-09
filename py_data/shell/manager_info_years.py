#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/30 14:34
# @Author  : GaoJian
# @File    : aps_info_all_insert.py

import os
import datetime
from datetime import timedelta

import logging
from base_utils import *

filename = os.path.split(__file__)[-1].split(".")[0]
logging.basicConfig(level=logging.DEBUG,  # 控制台打印的日志级别
                    filename=filename + '.log',
                    filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                    # a是追加模式，默认如果不写的话，就是追加模式
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    # 日志格式
                    )


def main(df):
    """

    :param df:
    :return:
    """
    for index, row in df.iterrows():
        # 计算每个年度

        if row.end == '0':
            date_iter = deal_time(str(row.start), '20181231')
        else:
            date_iter = deal_time(str(row.start), str(row.end))

        for year, date in date_iter:

            print("fundcode: {}  managerid {} year:{} start {} end {}".format(row.fundcode,
                                                                              row.managerid,
                                                                              year, date[0], date[1]))
            # 判断生产日期是否正确
            if len(date[0]) != 8 or len(date[1]) != 8:
                print("该年份 开始结束日期有误")
                continue

            # 判断该年份是否计算
            exists_sql = '''
                           select * from MANAGER_INFO_RANK_bak t
                           where t.fundmanager = '%(fundmanager)s' 
                           and t.fundcode = '%(fund_code)s'
                           and t.cycle_value = '%(cycle_value)s'
                           and t.manager_startdate = '%(manager_startdate)s' 
                           ''' % {'fundmanager': row.managername,
                                  'fund_code': row.fundcode,
                                  'cycle_value': str(year),
                                  'manager_startdate': row.start}

            if (len(cu1.execute(exists_sql).fetchall()) != 0):
                print('{}年度已计算'.format(year))
                continue

            try:
                start = date[0]
                end = date[1]

                # 日期处理
                if end == "0":
                    # 日期 为 0 补充为最新日期 前一天
                    end = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')

                if (row.clr == row.start):
                    start = get_tradedate(start)
                else:
                    tansferdate = (datetime.datetime.strptime(start, '%Y%m%d') + timedelta(days=-1)).strftime('%Y%m%d')

                    if (get_tradedate(tansferdate) == 0):
                        start = tansferdate
                    else:
                        start = get_tradedate(tansferdate)

                if (row.yjfl == 'QDII'):
                    end = get_tradedate(
                        (datetime.datetime.strptime(end, '%Y%m%d') + datetime.timedelta(days=-1)).strftime('%Y%m%d'))
                elif (row.yjfl != 'QDII'):
                    end = get_tradedate(end)
                # 查询指标 并入库

                obj = RankInfo(row.fundcode, row.ejfl, start, end)
                hc = obj.hc
                hc_rank = obj.hc_rank

                hb = obj.hb
                hb_rank = obj.hb_rank

                bd = obj.bd
                bd_rank = obj.bd_rank

                rec = []
                rec.append(row.fundcode)
                rec.append(row.fundname)
                rec.append(row.ejfl)
                rec.append(row.managername)
                rec.append(0)
                rec.append(str(year))
                rec.append(hc)
                rec.append(hc_rank)
                rec.append(bd)
                rec.append(bd_rank)
                rec.append(hb)
                rec.append(hb_rank)
                rec.append(row.start)
                rec.append(row.end)
                rec.append(row.clr)
                rec.append(row.managerid)
                insert_sql = "INSERT INTO MANAGER_INFO_RANK_bak( fundcode, fundname,fundtype, fundmanager,cycle_type,cycle_value,rrin_hc,rrin_hc_rank," \
                             "rrin_nhbd,rrin_nhbd_rank,rrin_hb,rrin_hb_rank,manager_startdate,manager_enddate,founddate,fund_manager_id) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16)"
                # print(rec)
                cu1.execute(insert_sql, rec)
                fund_db_pra.commit()

                rec.clear()

            except Exception as e:
                logging.exception(str(row.values))
                logging.exception(insert_sql)
                logging.exception(e)

                rec.clear()
                continue

    cu.close()
    cu1.close()
    fund_db_pra.close()
    fund_db_wind.close()




if __name__ == '__main__':


    print("----数据准备中----")

    df = find_all_code()
    fundcode_list = df.fundcode.values
    # fundcode_list = ['000263']

    manage_df = pd.DataFrame()
    for i in fundcode_list:
        m_df = find_manager(i)
        manage_df = pd.concat([manage_df, m_df])

    df = pd.merge(df, manage_df, how="outer", on='fundcode')

    # 删除没有基金经理的基金
    df.dropna(subset=['managername', 'managerid'], inplace=True)


    # 取出 end = 0 的df
    # df = df.loc[df['end'] == '0']

    print("----总数：", len(df))
    print("----begin----")

    # main(df)


    # 开启多进程计算
    import multiprocessing


    for i in range(0, len(df), len(df) // 24):
        j = i + len(df) // 24
        print(i, j)

        mp = multiprocessing.Process(target=main, args=(df[i:j],))
        mp.start()






