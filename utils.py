# -*- coding:utf-8 -*-
# author: chenyongle
from datetime import datetime
import time
import json

def mkdatetime(date):
    if isinstance(date, str):
        try:
            if len(date) == 10:
                dt = datetime.strptime(date.replace('/', '-'), '%Y-%m-%d')
            elif len(date) == 19:
                dt = datetime.strptime(date.replace('/', '-'), '%Y-%m-%d %H:%M:%S')
            else:
                raise ValueError()
        except ValueError:
            raise ValueError(
                "{0} is not a valid datetime format." \
                "dt Format example: 'yyyy-mm-dd' or yyyy-mm-dd HH:MM:SS".format(date)
            )
    else:
        raise TypeError(
            "date type not supported. date expect Format string: 'yyyy-mm-dd' or yyyy-mm-dd HH:MM:SS"
        )
    return dt
    
def timestamp_datetime(ts):
    if isinstance(ts, (int, float, str)):
        try:
            ts = int(ts)
        except ValueError:
            raise

        if len(str(ts)) == 13:
            ts = int(ts / 1000)
        if len(str(ts)) != 10:
            raise ValueError
    else:
        raise ValueError()

    return datetime.fromtimestamp(ts)

def datetime_timestamp(dt, type='ms'):
    if isinstance(dt, str):
        try:
            if len(dt) == 10:
                dt = datetime.strptime(dt.replace('/', '-'), '%Y-%m-%d')
            elif len(dt) == 19:
                dt = datetime.strptime(dt.replace('/', '-'), '%Y-%m-%d %H:%M:%S')
            else:
                raise ValueError()
        except ValueError:
            raise ValueError(
                "{0} is not supported datetime format." \
                "dt Format example: 'yyyy-mm-dd' or yyyy-mm-dd HH:MM:SS".format(dt)
            )

    if isinstance(dt, time.struct_time):
        dt = datetime.strptime(time.strftime('%Y-%m-%d %H:%M:%S', dt), '%Y-%m-%d %H:%M:%S')

    if isinstance(dt, datetime):
        if type == 'ms':
            ts = int(dt.timestamp()) * 1000
        else:
            ts = int(dt.timestamp())
    else:
        raise ValueError(
            "dt type not supported. dt Format example: 'yyyy-mm-dd' or yyyy-mm-dd HH:MM:SS"
        )
    return ts

def format_datetime(dt, fmt='%Y-%m-%d %H:%M:%S'):
    return dt.strftime(fmt)

# decorator
def tick_time(func):
    def tick(**kwgs):
        time0 = time.clock()
        print('Execute: ', format_datetime(datetime.now()))
        try:
            func(**kwgs)
        finally:
            print('Take time: ', time.clock() - time0)
        print('Take time: ', time.clock() - time0)

    return tick

# $todo
def json_to_csv(jsondata):
    pass

def unittest():
    print('Now: %s [%s in epoch]' %
          (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), datetime_timestamp(time.localtime())))

    print('1495432800000 expect to be ', time.strftime('%Y-%m-%d %H:%M:%S', timestamp_datetime(1495432800000)))

def load_org_map():
    with open('DB/ORG_MAP.json') as f:
        data = json.load(f)
    return data

def load_imei_map():
    with open('DB/imei_device.json') as f:
        data = json.load(f)
    return data

if __name__ == '__main__':
    unittest()
