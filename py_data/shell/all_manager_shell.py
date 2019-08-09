#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/30 14:34
# @Author  : GaoJian
# @File    : aps_info_all_insert.py
import multiprocessing
import os
import datetime
from datetime import timedelta
import logging
from queue import Queue
from concurrent.futures import ProcessPoolExecutor
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


def copy_after_data():
    """
    复制已经离任的基金经理
    :return:
    """

    update_time = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    sql = "select max(t.update_time) from manager_info_rank_bak t where t.cycle_value='0'"
    try:
        after_time = cu1.execute(sql).fetchall()[0][0]
    except Exception as e:
        print("no max (update_time)")
        return

    # 查询是否有记录 没有就copy
    exists_sql = '''
                    select * from MANAGER_INFO_RANK_bak t
                    where t.cycle_value = '0'
                    and t.update_time = '{}'
                  '''.format(update_time)

    if (len(cu1.execute(exists_sql).fetchall()) != 0):
        print("don't copy, {} already have data".format(update_time))
        return

    sql = """
          select fundcode,
          fundname,
          fundtype,
          fundmanager,
          cycle_type,
          cycle_value,
          rrin_hc,
          rrin_hc_rank,
          rrin_nhbd,
          rrin_nhbd_rank,
          rrin_hb,
          rrin_hb_rank,
          manager_startdate,
          manager_enddate,
          founddate,
          fund_manager_id,
          update_time
          from MANAGER_INFO_RANK_bak t
          where t.cycle_value = '0'
          and t.update_time = '{}'
          """.format(after_time)

    df = pd.DataFrame(cu1.execute(sql).fetchall(), columns=['fundcode',
                                                            'fundname',
                                                            'fundtype',
                                                            'fundmanager',
                                                            'cycle_type',
                                                            'cycle_value',
                                                            'rrin_hc',
                                                            'rrin_hc_rank',
                                                            'rrin_nhbd',
                                                            'rrin_nhbd_rank',
                                                            'rrin_hb',
                                                            'rrin_hb_rank',
                                                            'manager_startdate',
                                                            'manager_enddate',
                                                            'founddate',
                                                            'fund_manage_id',
                                                            'update_time'])

    # df.loc[df['end'] == '0', 'end'] = '20181231'
    if df.empty:
        print("after_time:{}  no value".format(after_time))
        return

    df['update_time'] = update_time

    insert_sql = "INSERT INTO MANAGER_INFO_RANK_bak(fundcode, fundname,fundtype, fundmanager,cycle_type,cycle_value,rrin_hc,rrin_hc_rank," \
                 "rrin_nhbd,rrin_nhbd_rank,rrin_hb,rrin_hb_rank,manager_startdate,manager_enddate,founddate,fund_manager_id,update_time) VALUES(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17)"

    param = [tuple(x) for x in df.values]
    cu1.executemany(insert_sql, param)
    fund_db_pra.commit()
    print("copy OK")


def insert_since_all(row):
    """ 插入基金经理任职以来数据"""

    try:
        update_time = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')

        # 查询是否存在
        exists_sql = '''
                        select * from MANAGER_INFO_RANK_bak t
                        where t.fundmanager = '%(fundmanager)s' 
                        and t.fundcode = '%(fund_code)s' 
                        and t.cycle_value = '%(cycle_value)s'
                        and t.manager_startdate = '%(manager_startdate)s' 
                        and t.update_time = '%(update_time)s'
                        ''' % {'fundmanager': row.managername,
                               'fund_code': row.fundcode,
                               'cycle_value': '0',
                               'manager_startdate': row.start,
                               'update_time': update_time}

        if (len(cu1.execute(exists_sql).fetchall()) != 0):
            print("dont't insert")
            return

        start = row.start
        end = row.end

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
        rec.append('0')
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
        rec.append(update_time)
        insert_sql = "INSERT INTO MANAGER_INFO_RANK_bak( fundcode, fundname,fundtype, " \
                     "fundmanager,cycle_type,cycle_value,rrin_hc,rrin_hc_rank," \
                     "rrin_nhbd,rrin_nhbd_rank,rrin_hb,rrin_hb_rank,manager_startdate," \
                     "manager_enddate,founddate,fund_manager_id,update_time) VALUES(:1, " \
                     ":2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17)"

        cu1.execute(insert_sql, rec)
        fund_db_pra.commit()
        print("insert", rec)
        logging.exception(str(rec))

    except Exception as e:
        print(e)
        logging.exception(str(row.values))
        logging.exception(e)


def update_since_all(row):
    """"""
    try:
        update_time = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')

        start = row.start
        end = row.end

        # 查询是否计算过
        exists_sql = '''
                   select * from MANAGER_INFO_RANK_bak t
                   where t.fundmanager = '%(fundmanager)s' 
                   and t.fundcode = '%(fund_code)s' 
                   and t.cycle_value = '%(cycle_value)s'
                   and t.manager_startdate = '%(manager_startdate)s'
                   and t.manager_enddate = '0'
                   and t.update_time = '%(update_time)s'
                   ''' % {'fundmanager': row.managername,
                          'fund_code': row.fundcode,
                          'cycle_value': '0',
                          'manager_startdate': row.start,
                          'update_time': update_time}

        if (len(cu1.execute(exists_sql).fetchall()) != 1):
            print("don't update")
            return


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

        # 查指标 更新入库
        obj = RankInfo(row.fundcode, row.ejfl, start, end)
        hc = obj.hc
        hc_rank = obj.hc_rank

        hb = obj.hb
        hb_rank = obj.hb_rank

        bd = obj.bd
        bd_rank = obj.bd_rank

        update_sql = '''
                           update MANAGER_INFO_RANK_bak t
                           set t.rrin_hc = '%(rrin_hc)s',
                           t.rrin_hc_rank = '%(rrin_hc_rank)s',
                           t.rrin_nhbd = '%(rrin_nhbd)s',
                           t.rrin_nhbd_rank  = '%(rrin_nhbd_rank)s',
                           t.rrin_hb         = '%(rrin_hb)s',
                           t.rrin_hb_rank    = '%(rrin_hb_rank)s',
                           t.manager_enddate = '%(manager_enddate)s',
                           t.founddate= '%(founddate)s',
                           t.fund_manager_id= '%(fund_manager_id)s',
                           t.update_time= '%(update_time)s'
                           where t.fundcode = '%(fund_code)s'
                           and t.fundmanager = '%(fundmanager)s'
                           and t.cycle_value = '0'
                           and t.cycle_type = '0'
                           and t.manager_startdate = '%(manager_startdate)s'
                           and t.update_time= '%(update_time)s'
                           ''' % {'rrin_hc': hc, 'rrin_hc_rank': hc_rank,
                                  'rrin_nhbd': bd, 'rrin_nhbd_rank': bd_rank,
                                  'rrin_hb': hb, 'rrin_hb_rank': hb_rank,
                                  'manager_enddate': row.end,
                                  'fund_code': row.fundcode,
                                  'fundmanager': row.managername,
                                  'founddate': row.clr,
                                  'fund_manager_id': row.managerid,
                                  'manager_startdate': row.start,
                                  'update_time': update_time
                                  }
        cu1.execute(update_sql)
        fund_db_pra.commit()

        print("fundcode:{} fundmanager:{} update_time:{} already update".format(row.fundcode,
                                                                                row.managername,
                                                                                update_time))
    except Exception as e:
        print("fundcode:{} fundmanager:{} update_time:{} already update".format(row.fundcode,
                                                                                row.managername,
                                                                                update_time))
        print(e)
        logging.exception(str(row.values))
        logging.exception(e)


def main(df):


    for index, row in df.iterrows():
        # 先插入
        insert_since_all(row)

        # 再更新
        update_since_all(row)



if __name__ == '__main__':

    print("----数据准备中----")

    # 复制前一天的数据
    copy_after_data()

    queue = Queue()

    df = find_all_code()

    fundcode_list = df.fundcode.values

    # fundcode_list = ["502049", "511220", "501301"]
    # fundcode_list = ["501303"]

    manage_df = pd.DataFrame()
    for i in fundcode_list:
        m_df = find_manager(i)
        manage_df = pd.concat([manage_df, m_df])

    df = pd.merge(df, manage_df, how="outer", on='fundcode')

    # 删除没有基金经理的基金
    df.dropna(subset=['managername', 'managerid'], inplace=True)

    print("总数：{}".format(len(df)))

    f = 16
    print("每份个数：{}".format(f))

    steps = math.ceil(len(df) / f)

    print("份数：{}".format(steps))

    for i in range(0, len(df), f):
        j = i + f
        queue.put(df[i:j])
        print(i, j)

    t1 = datetime.datetime.now()

    with ProcessPoolExecutor(24) as exe:

        for i in range(steps):
            exe.submit(main, queue.get())

    t2 = datetime.datetime.now()

    cu.close()
    cu1.close()
    fund_db_pra.close()
    fund_db_wind.close()

    print("times: {}".format(t2 - t1))
