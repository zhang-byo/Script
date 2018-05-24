# -*- coding:utf-8 -*-
###
# @Author: yongle.chen 
# @Date: 2018-05-24 11:19:35 
# @Last Modified by:   yongle.chen 
# @Last Modified time: 2018-05-24 11:19:35 
# @Description: 
# 整理17年流量日志表数据
# 抽取mongo中的数据，按月分表放入SQL中

from database import database
from utils import mkdatetime, format_datetime, tick_time, datetime_timestamp, timestamp_datetime

import json
import threading
import datetime
import pyexcel
import pymysql

# $todo: 考虑一下， 能否做成类似修饰器那样，能抽象循环抽取的逻辑。
def recursive():
    pass

# 返回{imei:{t_type:type, t_orgid:orgid}}的字典
def get_imei_org():
    query = "SELECT imei, type AS t_type, orgid AS t_orgid FROM `t_teminal`"
    ums = database('RMS_SQL').get_db()
    with ums.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(query)
        rdata = list(cur.fetchall())
    ums.close()
    imei_org = {}
    for row in rdata:
        _id = row.pop('imei')
        tmp = {_id: row}
        imei_org.update(tmp)
    with open('terminal_info.json', 'w') as f:
        json.dump(imei_org, f)

def load_imei_org():
    with open('terminal_info.json', 'r') as f:
        imei_org = json.load(f)
    return imei_org

def fetchDayFlow():
    begin_datetime = mkdatetime('2017-01-01')
    end_datetime = mkdatetime('2017-01-10')
    begin_time = datetime_timestamp(begin_datetime)
    end_time = datetime_timestamp(end_datetime)

    pipeline = [
        {'$match':{'createtime': {'$gte': begin_time, '$lt': end_time}}},
        {'$project': {'_id': 0, 'createtime': 1, 'mcc': 1,
                      'lac': 1, 'plmn': 1, 'imei': 1,
                      'imsi': 1, 'userFlow': '$userFlower', 'cardFlow': 1,
                      'sysFlow': '$sysFlower'}}
    ]
    mgo = database('GSVC_MGO').get_db()
    rdata = list(mgo.get_collection('dayChargeFlower').aggregate(pipeline, allowDiskUse = True))
    keys = ['createtime', 'lac', 'mcc', 'plmn', 'imei', 'imsi', 'userFlow', 'cardFlow', 'sysFlow']

    imei_org = load_imei_org()
    # format data, and deal with missing data
    for i in range(len(rdata)):
        # deal with missing key
        missing_key = list(set(keys).difference(set(rdata[i].keys())))
        for k in missing_key:
            if k in ['lac', 'imsi', 'plmn', 'mcc', 'imei']:
                rdata[i][k] = 'null'
            else:
                rdata[i][k] = 0
        # add t_orgid info
        if rdata[i]['imei'] != 'null' & (rdata[i]['imei'] in imei_org.keys()):
            rdata[i]['t_orgid'] = imei_org[rdata[i]['imei']]['t_orgid']
            rdata[i]['t_type'] = imei_org[rdata[i]['imei']]['t_type']
        else:
            rdata[i]['t_orgid'] = 'null'
            rdata[i]['t_type'] = 'null'
    return rdata

def insertTable(data, target_table):
    gsvc = database('GSVC_SQL_ADMIN').get_db()
    cur = gsvc.cursor()
    for i in range(len(data)):
        insert_stmt = "INSERT INTO `{target}`(createtime, lac, mcc, plmn, imei, imsi, "\
                    "userFlow, cardFlow, sysFlow, t_type, t_orgid) VALUES ("\
                    "'{createtime}','{lac}','{mcc}','{plmn}','{imei}','{imsi}','{userFlow}','{cardFlow}','{sysFlow}','{t_type}','{t_orgid}')".format(
                        target=target_table,
                        createtime=data[i]['createtime'],
                        lac=data[i]['lac'],
                        mcc=data[i]['mcc'],
                        plmn=data[i]['plmn'],
                        imei=data[i]['imei'],
                        imsi=data[i]['imsi'],
                        userFlow=data[i]['userFlow'],
                        cardFlow=data[i]['cardFlow'],
                        sysFlow=data[i]['sysFlow'],
                        t_type=data[i]['t_type'],
                        t_orgid=data[i]['t_orgid']
                        )
        cur.execute(insert_stmt)
    gsvc.commit()
    cur.close()
    gsvc.close()

if __name__ == '__main__':
    # 0. 准备imei-org数据
    # get_imei_org()
    # 1. 抽取mongo数据，增加type，orgid字段
    flowdata = fetchDayFlow()
    # 2. 多线程，分块插入数据
    i = 0
    sema = threading.Semaphore(2)
    with sema:
        while i < len(flowdata):
            subdata = flowdata[i:(i + 5000)]
            t = threading.Thread(target=insertTable, args=(subdata, 't_terminal_flow_count_day_201701'))
            t.start()
            i += 5000