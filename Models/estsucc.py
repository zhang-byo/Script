from .DBUtils import choose_perf_collection
from ..database import database
from ..utils import datetime_timestamp, format_datetime

COLLECTION = 't_term_vsim_estsucc'

def get_succ_data(imsi_list, begin_datetime, end_datetime):
    begin_time = datetime_timestamp(begin_datetime)
    end_time = datetime_timestamp(end_datetime)
    ## 1. 获取分表
    collections = choose_perf_collection(begin_datetime, end_datetime, COLLECTION)


    ## 2. 查询成功数据
    succ_data = []
    succ_condition = {'vsimImsi': {'$in': imsi_list},
                      'succTime': {'$gte': begin_time, '$lte': end_time}
                    }

    pipeline = [{'$match': succ_condition},
                {'$group': {
                    '_id': '$vsimImsi',
                    'succTime': {'$max': '$succTime'},
                    'count': {'$sum': 1}
                }}]

    mgo = database('PERF_MGO').get_db()
    imsi_idx = {} # {imsi:idx}
    for collection in collections:
        rv = list(mgo.get_collection(collection).aggregate(pipeline))
        for item in rv:
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

    return succ_data