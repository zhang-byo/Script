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

def load_mcc_country():
    with open('mcc_country.json', 'r') as f:
        mcc_country = json.load(f)
    return mcc_country

# 内存消耗大的版本，程序好像存在循环抽取的bug
def fetch_day_flow(begin_datetime, end_datetime):
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
    mcc_country = load_mcc_country()
    # format data, and deal with missing data
    for i in range(len(rdata)):
        # deal with missing key
        missing_key = list(set(keys).difference(set(rdata[i].keys())))
        for k in missing_key:
            if k in ['lac', 'imsi', 'plmn', 'mcc', 'imei']:
                rdata[i][k] = 'NaN'
            else:
                rdata[i][k] = 0
        # add imei info
        if rdata[i]['imei'] != 'NaN' and (rdata[i]['imei'] in imei_org.keys()):
            rdata[i]['t_orgid'] = imei_org[rdata[i]['imei']]['t_orgid']
            rdata[i]['t_type'] = imei_org[rdata[i]['imei']]['t_type']
        else:
            rdata[i]['t_orgid'] = 'NaN'
            rdata[i]['t_type'] = 'NaN'

        # add country info
        if rdata[i]['mcc'] != 'NaN' and (rdata[i]['mcc'] in mcc_country.keys()):
            # deal GU and SP
            if rdata[i]['mcc'] == '310':
                if rdata[i]['plmn'] != 'NaN':
                    mnc = rdata[i]['plmn'][-3:]
                    if (mnc in ['470', '140']) and \
                        (rdata[i]['lac'] in ['208','171','1', '10', '23', '24', '60', '66']):
                        rdata[i]['country'] = 'SPGU'
                    else:
                        rdata[i]['country'] = 'US'
                else:
                    rdata[i]['country'] = 'US'
            else:
                rdata[i]['country'] = mcc_country[rdata[i]['mcc']]
        else:
            rdata[i]['country'] = 'NaN'
    return rdata

# 优化内存消耗的版本
def fetch_day_flow_cursor(begin_datetime, end_datetime):
    begin_time = datetime_timestamp(begin_datetime)
    end_time = datetime_timestamp(end_datetime)
    
    match = {'createtime': {'$gte': begin_time, '$lt': end_time}}
    project = {'_id': 0}
    mgo = database('GSVC_MGO').get_db()
    cur = mgo.get_collection('dayChargeFlower').find(match, project)
    keys = ['createtime', 'lac', 'mcc', 'plmn', 'imei', 'imsi', 'userFlower', 'cardFlow', 'sysFlower']
    count = 0
    tmp = [] # data to be insert
    imei_org = load_imei_org()
    mcc_country = load_mcc_country()
    while True:
        try:
            doc = cur.next()
        except StopIteration:
            # 最后一次插入            
            if tmp:
                insertTable(tmp, 't_terminal_flow_count_day_201701')
            break
        # 处理缺失的key
        missing_key = list(set(keys).difference(set(doc.keys())))
        for k in missing_key:
            if k in ['lac', 'imsi', 'plmn', 'mcc', 'imei']:
                doc[k] = 'NaN'
            else:
                doc[k] = 0
        # 丢弃异常流量数据
        # 不在数据库做限制，是因为加上这个条件后，查询和数据抽取的性能低下
        if doc['userFlower'] > 2147483648:
            continue
        # 增加imei信息
        if doc['imei'] != 'NaN' and (doc['imei'] in imei_org.keys()):
            doc['t_orgid'] = imei_org[doc['imei']]['t_orgid']
            doc['t_type'] = imei_org[doc['imei']]['t_type']
        else:
            doc['t_orgid'] = 'NaN'
            doc['t_type'] = 'NaN'
        # 增加国家信息
        if doc['mcc'] != 'NaN' and (doc['mcc'] in mcc_country.keys()):
            # deal GU and SP
            if doc['mcc'] == '310':
                if doc['plmn'] != 'NaN':
                    mnc = doc['plmn'][-3:]
                    if (mnc in ['470', '140']) and \
                        (doc['lac'] in ['208','171','1', '10', '23', '24', '60', '66']):
                        doc['country'] = 'SPGU'
                    else:
                        doc['country'] = 'US'
                else:
                    doc['country'] = 'US'
            else:
                doc['country'] = mcc_country[doc['mcc']]
        else:
            doc['country'] = 'NaN'
        
        tmp.append(doc)
        count += 1
        # 每2000条做一次数据插入
        if count % 2000 == 0:
            insertTable(tmp, 't_terminal_flow_count_day_201701')
            print('insert {}\n'.format(count))
            tmp = []
        
def insertTable(data, target_table):
    gsvc = database('GSVC_SQL_ADMIN').get_db()
    cur = gsvc.cursor()
    for i in range(len(data)):
        insert_stmt = "INSERT INTO `{target}`(createtime, lac, mcc, plmn, imei, imsi, "\
                    "userFlow, cardFlow, sysFlow, t_type, t_orgid, country) VALUES ("\
                    "'{createtime}','{lac}','{mcc}','{plmn}','{imei}',"\
                    "'{imsi}','{userFlow}','{cardFlow}','{sysFlow}','{t_type}',"\
                    "'{t_orgid}','{country}')".format(
                        target=target_table,
                        createtime=data[i]['createtime'],
                        lac=data[i]['lac'],
                        mcc=data[i]['mcc'],
                        plmn=data[i]['plmn'],
                        imei=data[i]['imei'],
                        imsi=data[i]['imsi'],
                        userFlow=data[i]['userFlower'],
                        cardFlow=data[i]['cardFlow'],
                        sysFlow=data[i]['sysFlower'],
                        t_type=data[i]['t_type'],
                        t_orgid=data[i]['t_orgid'],
                        country=data[i]['country']
                    )
        cur.execute(insert_stmt)
    gsvc.commit()
    cur.close()
    gsvc.close()

if __name__ == '__main__':
    # 0. 准备imei-org数据
    # get_imei_org()
    # 1. 抽取mongo数据，增加type，orgid字段
    begin_datetime = mkdatetime('2017-01-15')
    end_datetime = mkdatetime('2017-02-01')
    fetch_day_flow_cursor(begin_datetime, end_datetime)
