# -*- coding:utf-8 -*-
###
# Created Date: 2018-06-04 10:32:24
# Author: Chen Yongle
# -----
# Last Modified: 2018-06-12, 16:48:22
# Modified By: Chen Yongle
# -----
# Copyright (c) 2018 ukl
# 
###

# 1. 查询上次分卡成功时间在7天前的imsi
# 2. 查询当前卡状态仍为活跃的imsi
# 3. 对于两者的交集, 查询上次分卡成功后，分卡次数
# 4. 对于两者的交集, 查询上次分卡成功后，产生流量
# 5. 获取这些imsi的err code， insert 并统计
# 6. 获取imsi的package信息
# 7. 将这些数据合并

import datetime
import json

import pymongo
import pymysql
import pyexcel
import time
import re

from database import database
from function import (query_imsi_flow_group, generic_query,
                                    choose_perf_collection, generic_insert)
from utils import (datetime_timestamp, format_datetime, mkdatetime, tick_time,
                   timestamp_datetime, mcc_country)

# 2. 获取还在活跃的imsi
def fetch_active_imsi():
    query = ("SELECT t1.imsi FROM `t_css_vsim` AS t1 "
            "LEFT JOIN `t_css_vsim_packages` AS t2 "
            "ON t1.`imsi` = t2.`imsi` "
            "WHERE t1.`available_status` = '0' "
            "AND t2.`next_update_time` IS NOT NULL "
            "AND t2.`next_update_time` > DATE(NOW()) "
            "AND t2.`package_status` IN (0,1) "
            "AND t1.business_status IN (0,4,5)")
    qdata = generic_query(database('CSS_SQL').get_db(), query)
    active_imsi = [str(i[0]) for i in qdata]
    return active_imsi

# $diss:为什么不先求active_imsi, 然后再通过in imsi的query语句去查询succ_data?
# active_imsi有10万条, 放在数据库执行in是否合适？
# 而且放在本地执行开销不大，不通过数据库in查询

# 一次查询<5s
# [{imsi:12, ....}]
def query_err_info_data(imsi_list, begin_datetime, end_datetime, con=None):
    default_time = mkdatetime('1900-01-01')
    update_time = datetime.datetime.now().replace(hour=0, minute=0, second=0,microsecond=0)
    err_info = {}
    if not con:
        con = database('PERF_MGO').get_db()
    
    begin_time = datetime_timestamp(begin_datetime)
    end_time = datetime_timestamp(end_datetime)
    
    collection = choose_perf_collection(begin_datetime,
                        end_datetime,
                        prefix='t_term_vsim_estfail')
    
    for col in collection:
        match = {'createTime': {'$gte': begin_time, '$lt': end_time}, 'vsimImsi': {'$in': imsi_list}}
        project = {'_id': 0, 'mcc': 1, 'mnc': 1, 'lac': 1, 'errType': 1, 'errCode': 1, 'vsimImsi': 1, 'errorTime': 1}
        for info in con[col].find(match, project):
            # 没有结果，结束
            if not info:
                break
            # imsi为空，跳过
            if info['vsimImsi'] == "":
                continue
            if 'mcc' not in info.keys():
                info['mcc'] = ''
            if 'mnc' not in info.keys():
                info['mnc'] = ''
            if 'lac' not in info.keys():
                info['lac'] = ''
            if 'errCode' not in info.keys():
                info['errCode'] = ''
            if 'errType' not in info.keys():
                info['errType'] = ''
            if 'errorTime' not in info.keys():
                info['errorTime'] = ''
                
            tmp = {}
            tmp['fail_mcc'] = info['mcc']
            tmp['fail_mnc'] = info['mnc']
            tmp['fail_lac'] = info['lac']
            tmp['err_code'] = info['errCode']
            tmp['err_type'] = info['errType']
            tmp['imsi'] = info['vsimImsi']
            tmp['update_time'] = update_time

            if len(str(info['errorTime'])) != 13:
                tmp['err_time']  = default_time
            else:
                tmp['err_time'] = timestamp_datetime(info['errorTime'])
            tmp['fail_country'] = mcc_country(info['mcc'], info['mnc'], info['lac'])

            if tmp['imsi'] in err_info.keys():
                # 添加记录
                err_info[tmp['imsi']].append(tmp)
            else:
                err_info.update({tmp['imsi']: [tmp]})

    fail_data = []
    for v in err_info.values():
        fail_data.extend(v)
    return fail_data

# {imsi:{info}}
def query_imsi_err_stat():
    query = ("SELECT imsi, fail_country, plmn, "
	         "GROUP_CONCAT('(', err, ':', count, ')') AS err "
             "FROM( SELECT imsi, fail_country, "
             "CONCAT(fail_mcc, fail_mnc) AS plmn, "
             "CONCAT(err_type, ',', err_code) AS err, "
             "COUNT(*) AS count "
		     "FROM `t_term_vsim_estfail_err_record` "
		     "WHERE update_time = ( "
				"SELECT update_time "
				"FROM `t_term_vsim_estfail_err_record` "
				"ORDER BY id DESC LIMIT 1) "
		    "GROUP BY imsi, fail_country, fail_mcc, "
                "fail_mnc, err ) AS t "
            "GROUP BY imsi, fail_country, plmn ")
    err_info = generic_query(database('GSVC_SQL').get_db(), query, cursorclass=pymysql.cursors.DictCursor)
    imsi_err = {}
    for item in err_info:
        imsi = item.pop('imsi')
        if imsi in imsi_err.keys():
            imsi_err[imsi].append(item)
        else:
            imsi_err.update({imsi: [item]})
    return imsi_err

# count只能是整天的count，不存在小时的count
# 由于不确定css_user_vsim_log的数据删除规律，不能回到css做小时查询
# 先按天处理
# 返回{imsi:count}
# $todo:考虑imsi count是否增加一个hour count字段，1-24小时使用","连接
def query_imsi_count_group(imsi_list, begin_datetime, con=None):
    begin_datetime = begin_datetime.replace(hour=0)
    query = ("SELECT imsi, SUM(count) AS count FROM `css_user_vsim_log_imsi_day` "
             "WHERE date >= '{0}' "
             "AND imsi in ('{1}') GROUP BY imsi").format(begin_datetime, "','".join(imsi_list))
    if con is None:
        con = database('GSVC_SQL').get_db()
    qdata = generic_query(con, query, close=False)
    imsi_count = {}
    for q in qdata: 
        imsi_count.update({q[0]: q[1]})
    return imsi_count
    
# 国家，套餐名称，归属机构
# 返回[{}]
def get_package_info(imsi_list):
    query = ("SELECT a.`imsi` AS `imsi`, "
             "c.`name` AS `PackageName`, "
             "e.`org_name` AS `ORG`, "
             "a.`iso2` AS `country` "
             "FROM `t_css_vsim` AS a "
             "LEFT JOIN `t_css_vsim_packages` AS b ON a.`imsi` = b.`imsi` "
             "LEFT JOIN `t_css_group` AS e ON a.`group_id` = e.`id` "
             "LEFT JOIN `t_css_package_type` AS c  ON c.`id` = b.`package_type_id` "
             "WHERE a.`imsi` in ('{}') ORDER BY PackageName, imsi").format("','".join(imsi_list))
    imsi_info = generic_query(database('CSS_SQL').get_db(), query, cursorclass=pymysql.cursors.DictCursor)
    return imsi_info

# 前提1：user_vsim_log_day_imsi 更新
# 前提2：succ data last succ 更新
if __name__ == '__main__':
    ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')
    now = datetime.datetime.now()
    today = datetime.datetime(year=now.year, month=now.month, day=now.day)
    
    query = ("SELECT imsi, DATE_FORMAT(succ_time, '%Y-%m-%d %H:00:00') AS succ_time FROM `t_term_vsim_estsucc_last_succ` "
             "WHERE succ_time >= '2018-05-06' AND succ_time < '{}' "
             "ORDER BY succ_time ").format(today - datetime.timedelta(days=5))
    succ_data = generic_query(database('GSVC_SQL').get_db(), query, cursorclass=pymysql.cursors.DictCursor)
    # imsi_list = [succ['imsi'] for succ in succ_data]
    time_imsi = {}
    imsi_list = [succ['imsi'] for succ in succ_data]
    active_imsi = fetch_active_imsi()
    imsi_diff = list(set(imsi_list) & set(active_imsi)) # 目标imsi
    imsi_time = {}
    # 将imsi按查询开始时间分组，430组
    for data in succ_data:
        # 丢弃不活跃的imsi
        if data['imsi'] not in imsi_diff:
            continue
        imsi_time.update({str(data['imsi']): data['succ_time']})
        
        if data['succ_time'] in time_imsi.keys():
            time_imsi[data['succ_time']].append(str(data['imsi']))
        else:
            time_imsi.update({data['succ_time']:[str(data['imsi'])]})
    # 查询分卡次数
    # 返回{imsi:count,imsi2:count}
    time0 = time.clock()
    print('1. imsi count execute: ', format_datetime(datetime.datetime.now()))
    imsi_count = {}
    con = database('GSVC_SQL').get_db()
    for bg_dt, sub_imsi in time_imsi.items():
        sub_count = query_imsi_count_group(sub_imsi, mkdatetime(bg_dt), con=con)
        imsi_count.update(sub_count)
    con.close()
    print('Take time: ', time.clock() - time0)
    # 查询产生流量
    # {imsi:flow}
    time0 = time.clock()
    print('2. imsi flow execute: ', format_datetime(datetime.datetime.now()))
    imsi_flow = {}
    mgo = database('OSS_MGO').get_db()
    # keys 是str，不是datetime
    for bg_dt in time_imsi.keys():
        sub_imsi = time_imsi[bg_dt]
        flowdata = query_imsi_flow_group(sub_imsi, mkdatetime(bg_dt), today, con=mgo)
        imsi_flow.update(flowdata)
        
    print('Take time: ', time.clock() - time0)
    
    # 获取err统计信息
    # 查询及统计错误日志
    # 每次都直接去数据库查询, 不管性能和缓存问题
    time0 = time.clock()
    print('3. imsi err execute: ', format_datetime(datetime.datetime.now()))
    # mgo = database('PERF_MGO').get_db()
    # fail_data = []
    # for bg_dt, sub_imsi in time_imsi.items():
    #     qdata = query_err_info_data(sub_imsi, mkdatetime(bg_dt), today, con=mgo)
    #     fail_data.extend(qdata)
    # generic_insert(fail_data, 't_term_vsim_estfail_err_record')
    
    imsi_err = query_imsi_err_stat()
    print('Take time: ', time.clock() - time0)
    # 获取套餐、国家等信息
    time0 = time.clock()
    print('4. imsi package execute: ', format_datetime(datetime.datetime.now()))
    imsi_pkg = get_package_info(imsi_diff)
    print('Take time: ', time.clock() - time0)
    # 合并数据
    result = []
    # 为了方便pyexcel写，按顺序做成list
    colnames = ['imsi', '套餐名称','ORG','归属国家','上次成功调用','上次成功后调用次数','flow_MB','err国家','plmn','errCode,errType']
    result.append(colnames)
    for item in imsi_pkg:
        imsi = str(item['imsi'])
        tmp = []
        tmp.append(imsi)
        tmp.append(item['PackageName'])
        tmp.append(item['ORG'])
        tmp.append(item['country'])
        
        tmp.append(imsi_time[imsi])

        if imsi in imsi_count.keys():
            if imsi_count[imsi] < 7:
                continue
            else:
                tmp.append(imsi_count[imsi])
        else:
            continue

        if imsi in imsi_flow.keys():
            if imsi_flow[imsi]/1024/1024 > 50:
                continue
            else:
                tmp.append(round(imsi_flow[imsi]/1024/1024, 2))
        else:
            continue
            
        if imsi in imsi_err.keys():
            _err = imsi_err[imsi]
            for sub_err in _err:
                _tmp = tmp[:]
                if ILLEGAL_CHARACTERS_RE.match(sub_err['fail_country']):
                    sub_err['fail_country'] = 'ERR'
                if ILLEGAL_CHARACTERS_RE.match(sub_err['plmn']):
                    sub_err['plmn'] = 'ERR'
                if ILLEGAL_CHARACTERS_RE.match(sub_err['err']):
                    sub_err['err'] = 'ERR'
                _tmp.append(sub_err['fail_country'])
                _tmp.append(sub_err['plmn'])
                _tmp.append(sub_err['err'])
                result.append(_tmp)
        else:
            tmp.append('-')
            tmp.append('-')
            tmp.append('-')
            result.append(tmp)
    # with open('temp.txt', 'w') as f:
    #     for r in result:
    #         f.write(str(r) + '\n')
    pyexcel.save_as(array=result,
        dest_file_name='long_time_err_vsim_{}.xlsx'.format(format_datetime(today, '%Y%m%d')),
        sheet_name='long_time_err_vsim')
   
    # last_succ_time之后的分卡次数统计
    # 结果：imsi，上次分卡成功时间(√)，此后的流量(√), 报错信息合并(√)，上次成功后的分卡次数(√)
    