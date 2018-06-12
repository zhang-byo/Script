#!/usr/bin/env python3
# -*- coding:utf-8 -*-
###
# Created Date: 2018-06-04 11:38:32
# Author: Chen Yongle
# -----
# Last Modified: 2018-06-12, 16:47:50
# Modified By: Chen Yongle
# -----
# Copyright (c) 2018 ukl
# 
###
from database import database
from utils import mkdatetime, format_datetime, tick_time, datetime_timestamp, timestamp_datetime
import pymysql
import pymongo
import datetime
import json

def choose_perf_collection(begin_datetime, end_datetime, prefix):
    begin_time = datetime_timestamp(begin_datetime)
    end_time = datetime_timestamp(end_datetime)
    ## 1. 动态确定查询分表
    # 注意条件中begin_time和end_time关系颠倒
    col_condition = {'type': prefix,
                     'endTime': {'$gte': begin_time},
                     'beginTime': {'$lt': end_time}}

    perf_mgo = database('PERF_MGO').get_db()
    sub_collections = list(perf_mgo.get_collection('t_db_sub_collections').find(col_condition))
    # 没有查到数据, 可能分表失败，默认从最新的表中提取数据
    if not sub_collections:
        sub_collections = list(perf_mgo.get_collection('t_db_sub_collections').
                                find({'type': prefix}, limit=1).
                                sort('endTime', pymongo.DESCENDING))
    collections = []
    for item in sub_collections:
        collections.append(item['collectionName'])

    return collections

def generic_query(con, query, close=True, cursorclass=pymysql.cursors.Cursor):
    # close: 是否关闭连接
    try:
        with con.cursor(cursorclass) as cur:
            cur.execute(query)
            qdata = list(cur.fetchall())
    except Exception as e:
        con.close()
        raise e
    if close:
        con.close()

    return qdata

def generic_insert(data, target_table):
    # 数据库查出的字段不存在不规则字典的情况
    # 但如果是mongo过来的不规则字典，则最好在插入前做数据清理
    # 每20000条数据插入一次, 20000条数据预期为2-3M大小
    i = 0
    count = 0
    con = database('GSVC_SQL_ADMIN').get_db()
    cur = con.cursor()

    key_name = data[0].keys()
    
    while i < len(data):
        subdata = [tuple(x.values()) for x in data[i:(i + 20000)]]
    
        insert_stmt = "INSERT INTO `{target_table}` (`{colname}`) VALUES ({values})".format(
            target_table=target_table,
            colname="`,`".join(key_name),
            values=','.join(['%s' for x in range(len(key_name))])
        )
        if i == 0:
            print(insert_stmt)

        try:
            effect_row = cur.executemany(insert_stmt, subdata)
            count += effect_row
            con.commit()
        except pymysql.err.Error as e:
            print('INSERT ROWS: {}'.format(count))
            cur.close()
            con.close()
            raise e
        i += 20000

    print('INSERT ROWS: {}'.format(count))
    cur.close()
    con.close()

# 返回{imsi: flow}
def query_imsi_flow_group(imsi_list, begin_datetime, end_datetime, con=None):
    # 处理非整天的情况
    begin_datetime_day = ''
    end_datetime_day = ''
    if begin_datetime.hour != 0:
        begin_datetime_day = datetime.datetime(year=begin_datetime.year,
                                               month=begin_datetime.month,
                                               day=begin_datetime.day) + datetime.timedelta(days=1)
    if end_datetime.hour != 0:
        end_datetime_day = datetime.datetime(year=end_datetime.year,
                                               month=end_datetime.month,
                                               day=end_datetime.day)
    total_flow = {}
    if not con:
        con = database('OSS_MGO').get_db()
    # 第一天
    if begin_datetime_day:
        flow_begin = _query_flow_hour_group(imsi_list, begin_datetime, begin_datetime_day)
        total_flow.update(flow_begin)
    # 区分时间跨天的情况
    if end_datetime.day > begin_datetime.day:
        # 最后一天
        if end_datetime_day:
            flow_end = _query_flow_hour_group(imsi_list, end_datetime_day, end_datetime, con=con)
            for imsi, flow in flow_end.items():
                if imsi in total_flow.keys():
                    total_flow[imsi] += flow
                else:
                    total_flow.update({imsi:flow})
        if end_datetime.day - begin_datetime.day > 1:
            # 中间的天
            flow_between = _query_flow_day_group(imsi_list, begin_datetime, end_datetime, con=con)
            for imsi, flow in flow_between.items():
                if imsi in total_flow.keys():
                    total_flow[imsi] += flow
                else:
                    total_flow.update({imsi:flow})
    # flow_list = [{'imsi':k, 'flow': v} for k, v in total_flow.items()]
    return total_flow

# begin_datetime和end_datetime之差不超过1天, 不会跨表
# 因为限制严格，不对外开放调用
# 为方便求和，返回dict
def _query_flow_hour_group(imsi_list, begin_datetime, end_datetime, con=None):
    begin_time = datetime_timestamp(begin_datetime)
    end_time = datetime_timestamp(end_datetime)
    pipeline = [{'$match': {'countTime': {'$gte': begin_time, '$lt': end_time}, 'imsi':{'$in':imsi_list}}},
                {'$group': {'_id': '$imsi', 'total': {'$sum':'$total'},
                            'userTotal': {'$sum': '$userTotal'}, 'cardTotal': {'$sum': '$cardTotal'}}},
                {'$project': {'_id':1, 'flow':{'$add':['$userTotal', '$total', '$cardTotal']}}}]
    if not con:
        con = database('OSS_MGO').get_db()
    col = 't_terminal_flow_count_hour_{}'.format(format_datetime(begin_datetime, '%Y%m01'))
    flowdata = {}
    for doc in con[col].aggregate(pipeline, allowDiskUse=True):
        flowdata.update({doc['_id']:doc['flow']})
    return flowdata

# begin_datetime 和 end_datetime均为0点, 会根据月份跨表
# 因为限制严格，不对外开放调用
# 为方便求和，返回dict
def _query_flow_day_group(imsi_list, begin_datetime, end_datetime, con=None):
    flowdata = {}
    if not con:
        con = database('OSS_MGO').get_db()
    # 处理时间跨月的情况
    while begin_datetime < end_datetime:
        begin_time = datetime_timestamp(begin_datetime)
        end_time = datetime_timestamp(end_datetime)
        pipeline = [{'$match': {'countTime': {'$gte': begin_time, '$lt': end_time}, 'imsi':{'$in':imsi_list}}},
                    {'$group': {'_id': '$imsi', 'total': {'$sum':'$total'},
                            'userTotal': {'$sum': '$userTotal'}, 'cardTotal': {'$sum': '$cardTotal'}}},
                    {'$project': {'_id':1, 'flow':{'$add':['$userTotal', '$total', '$cardTotal']}}}]
        col = 't_terminal_flow_count_day_{}'.format(format_datetime(begin_datetime, '%Y%m01'))
        for doc in con[col].aggregate(pipeline, allowDiskUse=True):
            if doc['_id'] in flowdata.keys():
                flowdata[doc['_id']] += doc['flow']
            else:
                flowdata.update({doc['_id']:doc['flow']})
        if begin_datetime.month + 1 > 12:
            begin_datetime = datetime.datetime(year=begin_datetime.year + 1,
                                            month=1,
                                            day=1)
        else: 
            begin_datetime = datetime.datetime(year=begin_datetime.year,
                                            month=begin_datetime.month + 1,
                                            day=1)
    return flowdata
