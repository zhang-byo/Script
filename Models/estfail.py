from .DBUtils import choose_perf_collection
from ..database import database
from ..utils import datetime_timestamp, format_datetime

COLLECTION = 't_term_vsim_estfail'

# $bug: 没有对多表查询结果数据做整合, 因为太啰嗦, 性能预计很差. 所以放弃
def get_fail_data(imsi_list, begin_datetime, end_datetime):
    begin_time = datetime_timestamp(begin_datetime)
    end_time = datetime_timestamp(end_datetime)
    ## 1. 获取分表
    collections = choose_perf_collection(begin_datetime, end_datetime, COLLECTION)

    ## 2. 查询错误数据
    estfail = []
    fail_condition = {'vsimImsi': {'$in': imsi_list},
                     'errorTime': {'$gte': begin_time, '$lte': end_time}
                    }

    pipeline = [{'$match': fail_condition},
                {'$group': {
                    '_id': {'vsimImsi': '$vsimImsi', 'errType': '$errType', 'errCode': '$errCode'},
                    'count': {'$sum': 1}
                }}]

    mgo = database('PERF_MGO').get_db()
    for collection in collections:
        fail_data = list(mgo.get_collection(collection).aggregate(pipeline))
        tmp = []
        for d in fail_data:
            d.update(d.pop('_id'))
            tmp.append(d)

        estfail.extend(tmp)  
    return estfail