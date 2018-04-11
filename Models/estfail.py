from DBUtils import choose_perf_collection
from ..database import database

COLLECTION = 't_term_vsim_estfail'
def get_imsi_fail(imsi_list, begin_time, end_time):
    ## 1. 获取分表
    collections = choose_perf_collection(begin_time, end_time, COLLECTION)

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