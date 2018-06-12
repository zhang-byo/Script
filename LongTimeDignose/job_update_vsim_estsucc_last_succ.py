# -*- coding:utf-8 -*-
###
# Created Date: 2018-06-05 9:57:33
# Author: Chen Yongle
# -----
# Last Modified: 2018-06-12, 16:47:21
# Modified By: Chen Yongle
# -----
# Copyright (c) 2018 ukl
# 
###
from database import database
from utils import (mkdatetime, format_datetime, tick_time,
                datetime_timestamp, timestamp_datetime, mcc_country)
from function import choose_perf_collection, generic_query

import pymysql
import pymongo
import datetime
import json

@tick_time
def update_vsim_estsucc_last_succ():
    begin_datetime = generic_query(database('GSVC_SQL').get_db(),
                                    "SELECT MAX(update_time) FROM `t_term_vsim_estsucc_last_succ`")[0][0]
    begin_datetime = mkdatetime(str(begin_datetime))
    now = datetime.datetime.now()
    today = datetime.datetime(year=now.year, month=now.month, day=now.day)
    mgo = database('PERF_MGO').get_db()
    succ_info = {}
    # 1. 循环更新succ信息, 按天避免查询范围过大。返回最终要插入的表
    while begin_datetime < today:
        begin_time = datetime_timestamp(begin_datetime)
        end_time = datetime_timestamp(begin_datetime + datetime.timedelta(days=1))
        # choose collection返回的是list, 此场景下只有一个值
        col = choose_perf_collection(begin_datetime,
                        begin_datetime + datetime.timedelta(days=1),
                        prefix='t_term_vsim_estsucc')[0]

        match = {'createTime': {'$gte': begin_time, '$lt': end_time}}
        # $吐槽: 为了使用mongo自带的project重命名, 写pipeline的代价可是真的大
        pipeline = [{'$match': match},
                    {'$sort': {'createTime': -1}},
                    {'$group': {
                        '_id': '$vsimImsi',
                        'succ_time': {'$first': '$succTime'},
                        'succ_mcc': {'$first': '$mcc'},
                        'succ_mnc': {'$first': '$mnc'},
                        'succ_lac': {'$first': '$lac'},
                    }},
                    {'$project': {
                        '_id':0,
                        'imsi': '$_id',
                        'succ_time': 1,
                        'succ_mcc': 1,
                        'succ_mnc': 1,
                        'succ_lac': 1
                    }}]
        
        for info in mgo[col].aggregate(pipeline, allowDiskUse=True):
            if info['imsi'] == "":
                continue

            info['update_time'] = today # 记录更新日期
            info['create_date'] = today # 插入数据库时生效，update不生效
            if len(str(info['succ_time'])) != 13:
                info['succ_time']  = mkdatetime('1900-01-01')
            else:
                info['succ_time'] = timestamp_datetime(info['succ_time'])
            info['succ_country'] = mcc_country(info['succ_mcc'], info['succ_mnc'], info['succ_lac'])

            if info['imsi'] in succ_info.keys():
                succ_info[info['imsi']] = info
            else:
                succ_info.update({info['imsi']: info})

        begin_datetime += datetime.timedelta(days=1)

    # 2. format查询结果，便于插入与update
    succ_list = [x for x in succ_info.values()]
    succ_tuple_key = succ_list[1].keys()
    succ_tuple_val = [tuple(v.values()) for v in succ_list]

    update_stmt = ("INSERT INTO `{target_table}` (`{colname}`) VALUES ({placeholder}) ON DUPLICATE KEY "
                   "UPDATE `succ_time` = values(`succ_time`), "
                   "`update_time` = values(`update_time`),"
                   "`succ_lac` = values(`succ_lac`),"
                   "`succ_mcc` = values(`succ_mcc`),"
                   "`succ_country` = values(`succ_country`),"
                   "`succ_mnc` = values(`succ_mnc`)").format(
                        target_table='t_term_vsim_estsucc_last_succ',
                        colname="`,`".join(succ_tuple_key),
                        placeholder=','.join(['%s' for x in range(len(succ_tuple_key))])
                   )
    con = database('GSVC_SQL_ADMIN').get_db()
    try:
        with con.cursor() as cur:
            effect_row = cur.executemany(update_stmt, succ_tuple_val)
        con.commit()
    except Exception as e:
        con.close()
        print('INSERT ROWS:{}'.format(effect_row))
        raise e
    print('INSERT ROWS:{}'.format(effect_row))
    con.close()

if __name__ == '__main__':
    update_vsim_estsucc_last_succ()