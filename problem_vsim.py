# -*- coding:utf-8 -*-
from utils import format_datetime, datetime_timestamp, timestamp_datetime, tick_time,mkdatetime
from database import database
from DBUtils import choose_perf_collection

import pyexcel
import json
import pymysql
import pymongo
import datetime

def init_vsim_estfail():
    pipeline = [
        {'$match':{'createTime':{'$gte':1521849600000, '$lt':1521936000000}}},
        {'$group':{'_id':'$vsimImsi', 'count':{'$sum':1}}},
        {'$group':{'_id':'null', 'count':{'$sum':1}}}
    ]
    con = database('PERF_MGO').get_db()
    count = list(con.get_database('oss_perflog').get_collection(
        't_term_vsim_estsucc_20180323').aggregate(pipeline))
    con.close()
    print(count)

# @tick_time
def init_vsim_estfail2():
    pipeline = {'createTime':{'$gte':1521763200045, '$lt':1521770400000}}
    begin_time = 1521763200000
    delta = 60 * 60 * 1000 * 2
    # end_time = begin_time + delta + 1
    end_time = 1521943200000 + 1
    rdata = []
    while (begin_time + delta < end_time):
        pipeline = [{'$match':{'createTime':{'$gte':begin_time, '$lt': begin_time + delta}}},
            {'$group':{'_id':{'type':'$errType', 'code':'$errCode'},'count':{'$sum':1}}}]
        con = database('PERF_MGO').get_db()
        qdata = list(con.get_database('oss_perflog').get_collection('t_term_vsim_estfail_20180323').aggregate(
            pipeline))
        for d in qdata:
            d.update(d.pop('_id'))
            d['date'] = format_datetime(timestamp_datetime(begin_time))
        rdata.append(qdata[0])
        begin_time += delta
    
    pyexcel.save_as(records=rdata, dest_file_name="problem_vsim/est_fail.xls")
    con.close()

def tick_estsucc_imsi(begin_time, end_time):
    # 1. 时间区间可能横跨分表, 可以选择在函数外处理, 也可以选择在函数内处理. 此处选在函数内处理
    # 感觉在函数外处理, 逻辑可能更好??

    pipeline = [{
            '$match':{'createTime': {'$gte': begin_time, '$lt': end_time}, }
        }, {
            '$sort':{'creatTime':-1, 'vsimImsi':1}
        }, {
            '$group':{  
                '_id':'$vsimImsi',
                'recent': {'$first': '$createTime'},
                'mcc': {'$first': '$mcc'},
                'mnc': {'$first': '$mnc'},
                'lac': {'$first': '$lac'},
            }
        }]
    con = database('PERF_MGO').get_db()
    collections = choose_perf_collection(begin_time, end_time, 't_term_vsim_estsucc')
    succ_data = []
    imsi_idx = {} # {imsi: idx}
    for col in collections:
        tmp = list(
            con.get_database('oss_perflog').get_collection(col).aggregate(
                pipeline, allowDiskUse=True))

        for item in tmp:
            if item['_id'] not in imsi_idx.keys():
                succ_data.append(item)
                imsi_idx.update({item['_id']:(len(imsi_idx) + 1)})
            else:
                # vsim重合的信息处理, d的imsi应该是和item的imsi是一致的
                d = succ_data[imsi_idx[item['_id']]]
                if item['recent'] > d['recent']:
                    d['recent'] = item['recent']
                    d['mcc'] = item['mcc']
                    d['mnc'] = item['mnc']
                    d['lac'] = item['lac']
        
    for d in succ_data:
        d['datetime'] = format_datetime(timestamp_datetime(d['recent']))

    return succ_data

@tick_time
def fetch_estfail():
    table = 'd_estfail_records'
    begin_datetime = mkdatetime('2018-03-30 08:00:00')
    end_datetime = mkdatetime('2018-03-30 09:00:00')
    # end_datetime = mkdatetime('2018-04-01 08:00:00')
    delta = datetime.timedelta(hours=1)
    mgo = database('PERF_MGO').get_db()
    while begin_datetime + delta <= end_datetime:
        begin_time = datetime_timestamp(begin_datetime)
        end_time = datetime_timestamp(begin_datetime + delta)
        pipeline = {'createTime':{'$gte': begin_time, '$lt': end_time}}
        pipeline2 = {'vsimImsi':1, 'lac': 1, 'mcc':1, 'mnc':1, 'errCode':1, 'errType':1,
                     'errorTime':1, '_id':0}

        fail_data = list(
                    mgo.get_database('oss_perflog').get_collection(
                        't_term_vsim_estfail_20180330').find(pipeline, pipeline2)
                )
        imsi_list = []
        for i in range(len(fail_data)):
            imsi_list.append(fail_data[i]['vsimImsi'])
        query_stmt = ('SELECT imsi FROM `d_last_estsucc_record` '
                      'WHERE last_succ_time > "{0}" '
                      'AND imsi IN ({1}) '
                      ).format(
                        format_datetime(begin_datetime), "'" + "','".join(imsi_list) + "'"
                      )
        con = database('GSVC_SQL_ADMIN').get_db()
        succ_imsi = []
        with con.cursor(pymysql.cursors.SSCursor) as cursor:
            cursor.execute(query_stmt)
            for row in cursor:
                succ_imsi.append(row[0])

        with open('problem_vsim/succ_imsi.json', 'w') as f:
            f.write(json.dumps(succ_imsi))
        pdata = []
        tmp = []
        for i, x in enumerate(imsi_list):
            if x not in succ_imsi:
                tmp.append(fail_data[i])

        start_idx = 0
        
        for item in tmp:
            fail_keys = item.keys()
            if len(item['vsimImsi']) == 15:
                if 'lac' not in fail_keys or not item['lac']:
                    item['lac'] = 'null'
                if 'mcc' not in fail_keys or not item['mcc']:
                    item['mcc'] = 'null'
                if 'mnc' not in fail_keys or not item['mnc']:
                    item['mnc'] = 'null'
                if 'errType' not in fail_keys or not item['errType']:
                    item['errType'] = 'null'
                if 'errCode' not in fail_keys or not item['errCode']:
                    item['errCode'] = 'null'
                if 'errorTime' not in fail_keys or not item['errorTime']:
                    item['errorTime'] = str(begin_time)
                
                tmp = tuple([item['vsimImsi'], item['lac'], str(item['mcc']), item['mnc'],
                        format_datetime(timestamp_datetime(item['errorTime'])),
                        str(item['errType']), str(item['errCode']),
                        format_datetime(datetime.datetime.now())])
                pdata.append(tmp)
        with open('problem_vsim/pdata.json', 'w') as f:
            f.write(json.dumps(pdata))
        while(start_idx <= len(pdata)):
            end_idx = start_idx + 200
            s = pdata[start_idx:end_idx]
            insert_stmt = ("INSERT INTO `{table}` "
                          "(imsi, lac, mcc, mnc, errorTime, errType, errCode, update_time) "
                          "VALUES {values} ").format(
                              table=table, 
                              values = str(s).replace("[","").replace("]","")
                          )
            if start_idx < 100:
                print(insert_stmt)
            with con.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(insert_stmt)
            con.commit()
            start_idx = end_idx
        con.close()
        begin_datetime += delta
        
@tick_time
def update_estsucc_imsi():
    TABLE = 'd_last_estsucc_record'

    # 1. 获取上次更新时间
    con = database('GSVC_SQL_ADMIN').get_db()
    with con.cursor() as cursor:
        cursor.execute('SELECT MAX(last_succ_time) AS t FROM `{}`'.format(TABLE))
        # cursor.execute('SELECT MAX(update_time) AS t FROM `{}`'.format(TABLE))
        begin_datetime = cursor.fetchall()[0]
    end_datetime = format_datetime(datetime.datetime.now, '%Y-%m-%d 00:00:00')

    delta = datetime.timedelta(hours=8)

    # 2. 按每8小时的区间提取succ表的数据
    while begin_datetime + delta <= end_datetime:
        update_data = []
        begin_time = datetime_timestamp(begin_datetime)
        end_time = datetime_timestamp(begin_datetime + delta)
        est_data = tick_estsucc_imsi(begin_time, end_time)

        for item in est_data:
            if len(item['_id']) == 15:
                if not item['lac']:
                    item['lac'] = 'null'
                if not item['mcc']:
                    item['mnc'] = 'null'
                if not item['recent']:
                    item['recent'] = str(begin_time)
                tmp = tuple([
                        item['_id'], # imsi
                        item['lac'],
                        str(item['mcc']),
                        item['mnc'],
                        format_datetime(timestamp_datetime(item['recent'])), # last_succ_time
                        format_datetime(datetime.datetime.now()) # update_time
                    ])
                update_data.append(tmp)

        update_count = 0
        # 3. 更新succ表
        while(update_count <= len(update_data)):
            next_count = update_count + 1000
            s = update_data[update_count: next_count]
            # with open('problem_vsim/tmp' + str(next_count) + '.json', 'w') as f:
            #     f.write(json.dumps(s))
            update_stmt = ("INSERT INTO `{table}` "
                          "(imsi, lac, mcc, mnc, last_succ_time, update_time) "
                          "VALUES {values} "
                          "ON DUPLICATE KEY UPDATE "
                          "lac=VALUES(lac), mcc=VALUES(mcc), mnc=VALUES(mnc), "
                          "last_succ_time=VALUES(last_succ_time), update_time=VALUES(update_time)").format(
                              table=TABLE, 
                              values = str(s).replace("[","").replace("]","")
                          )
            with con.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(update_stmt)
            con.commit()
            update_count = next_count
        begin_datetime += delta
        
    con.close()

def update_estsucc_imsi2():
    table = 'd_estsucc_record'
    con = database('GSVC_SQL').get_db()
    with con.cursor() as cursor:
        cursor.execute('SELECT update_time from `{0}` ORDER BY update_time DESC'.format(table))
        begin_datetime = cursor.fetchone()[0]

    begin_time = datetime_timestamp(begin_datetime)
    end_time = datetime_timestamp(datetime.datetime.now())

    qdata = tick_estsucc_imsi(begin_time,end_time)

    for i in range(len(qdata)):
        pass
