# -*- coding:utf-8 -*-
# author: chenyongle
# description:
# DB通用工具
# 考虑了一下, 还是从utils里剥离出一个DB相关的工具列表
# 主要是这里的工具会随着研发mongo和sql的变更而变更, 不像utils那样几乎全局通用

from utils import format_datetime, datetime_timestamp, timestamp_datetime, tick_time,mkdatetime
from database import database
import pymysql
import pymongo

D_PERF_DB_SUB_COL = 't_db_sub_collections'

# 根据时间, 获取perflog的分表信息. 返回值是列表
def choose_perf_collection(begin_time, end_time, prefix):
    ## 1. 动态确定查询分表
    # 注意条件中begin_time和end_time关系颠倒
    col_condition = {'type': prefix,
                     'endTime': {'$gte': begin_time},
                     'beginTime': {'$lt': end_time}}

    perf_mgo = database('PERF_MGO').get_db()
    sub_collections = list(perf_mgo.get_collection(D_PERF_DB_SUB_COL).find(col_condition))
    # 没有查到数据, 可能分表失败，默认从最新的表中提取数据
    if not sub_collections:
        sub_collections = list(perf_mgo.get_collection(D_PERF_DB_SUB_COL).
                                find({'type': prefix}, limit=1).
                                sort('endTime', pymongo.DESCENDING))
    collections = []
    for item in sub_collections:
        collections.append(item['collectionName'])

    return collections