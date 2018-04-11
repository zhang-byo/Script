from .DBUtils import choose_hourflow_collection
from ..utils import datetime_timestamp, format_datetime
from ..database import database


# flow(MB)
def get_imsi_flow(imsi_list, begin_datetime, end_datetime):
    begin_time = datetime_timestamp(begin_datetime)
    end_time = datetime_timestamp(end_datetime)
    pipeline = [{"$match": {'countTime': {'$gte': begin_time, '$lt': end_time},
                            'imsi': {'$in': imsi_list}
                            }
                },
                {"$group": {"_id": {'imsi': "$imsi"},
                            "flow": {'$sum':  {'$add': ["$userTotal", "$cardTotal"]}
                                       }
                            }
                }
                ]
    mgo_con = database('OSS_MGO').get_db()

    collection = choose_hourflow_collection(begin_datetime)
    mgo_cursor = mgo_con.get_collection(collection).aggregate(pipeline)
    flow_data = {}
    for doc in mgo_cursor:
        flow_data.update({doc.pop('_id')['imsi']: round(doc['flow']/1024/1024, 2)})
    return flow_data