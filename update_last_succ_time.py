from utils import format_datetime, datetime_timestamp, timestamp_datetime, tick_time,mkdatetime
from DBUtils import choose_perf_collection
from database import database

import pymysql
import pymongo
import datetime

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
        est_data = fetch_estsucc_record(begin_time, end_time)

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
        # 3. 更新succ表, 替换掉旧的记录
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

def fetch_estsucc_record(begin_time, end_time):
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
