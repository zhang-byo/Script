###
 # @Author: yongle.chen 
 # @Date: 2018-05-29 11:19:39 
 # @Last Modified by:   yongle.chen 
 # @Last Modified time: 2018-05-29 11:19:39 
 # @Description: 
 # 按天对国家-org-devtype对数据进行聚合,整合到一个表中
 # CREATE TABLE `terminal_flow_count_by_country_org_day_2017` (
#   `id` int(16) NOT NULL AUTO_INCREMENT,
#   `date` datetime NOT NULL,
#   `country` varchar(4) DEFAULT NULL,
#   `t_orgid` varchar(30) DEFAULT NULL,
#   `t_type` varchar(10) NOT NULL,
#   `active_count` int(12) NOT NULL,
#   `flow` bigint(20) NOT NULL,
#   PRIMARY KEY (`id`),
#   KEY `date_org_country_idx` (`date`,`t_orgid`,`country`) USING BTREE,
#   KEY `date_country_idx` (`date`,`country`) USING BTREE
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8;


from database import database
from utils import mkdatetime, format_datetime, tick_time, datetime_timestamp, timestamp_datetime

import json
import threading
import datetime
import pyexcel
import pymysql

# 实际发现， 聚合到这几个维度之后，再通过这个表进行聚合，按天SUM(count)的数据远比在日志表中COUNT(DISTINCT imei)的数据要大的多
# 因此需要依次分开统计，不能通过中间表聚合求值

# 不支持自定义部分插入，在外部处理完之后再调用函数
# $todo: 将generic_insert整合到database方法中
def generic_insert(idata, target_table):
    # 数据库查出的字段不存在不规则字典的情况
    # 但如果是mongo过来的不规则字典，则需要在插入前做数据清理
    

    # 每10000条数据插入一次
    i = 0
    count = 0
    con = database('GSVC_SQL_ADMIN').get_db()
    cur = con.cursor()

    key_name = idata[1].keys()
    
    while i < len(idata):
        data = [tuple(x.values()) for x in idata[i:(i + 10000)]]
    
        insert_stmt = "INSERT INTO `{target_table}` (`{colname}`) VALUES ({values})".format(
            target_table=target_table,
            colname="`,`".join(key_name),
            values=','.join(['%s' for x in range(len(key_name))])
        )
        if i == 0:
            print(insert_stmt)

        try:
            effect_row = cur.executemany(insert_stmt, data)
            count += effect_row
            con.commit()
        except pymysql.err.Error as e:
            print('insert rows: {}'.format(count))
            cur.close()
            con.close()
            raise e
        i += 10000

    print('insert rows: {}'.format(count))
    try:
        cur.close()
        con.close()
    except pymysql.err.Error as e:
        # 认为连接已经关闭，不需要做任何操作
        pass

# 生成timestamp-datetime映射表，在数据库进行匹配操作
def generate_timestamp_datetime():
    begin_datetime = mkdatetime('2017-01-01 08:00:00')
    end_datetime = mkdatetime('2018-01-01 08:00:00')
    tmp = [['timestamp', 'date']]
    while begin_datetime < end_datetime:
        ts = datetime_timestamp(begin_datetime)
        tmp.append([ts, begin_datetime])
        begin_datetime += datetime.timedelta(1)
    pyexcel.save_as(array = tmp, dest_file_name = '2017_datetime_timestamp.csv')

def flowlog_by_day(table_name):
    query = ("SELECT DATE(t2.date) AS date, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, COUNT(DISTINCT imei) AS active_count, SUM(sysFlow + userFlow)/1024 AS flow_KB "
             "FROM `{}` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "AND userFlow > 0 "
             "GROUP BY createtime) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp").format(table_name)

    con = database('GSVC_SQL').get_db()
    with con.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(query)
        qdata = cur.fetchall()
    con.close()
    return qdata

def flowlog_by_day_country(table_name):
    query = ("SELECT DATE(t2.date) AS date, t1.country, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, country, COUNT(DISTINCT imei) AS active_count, (SUM(sysFlow) + SUM(userFlow))/1024 AS flow_KB "
             "FROM `{}` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "GROUP BY createtime, country) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp").format(table_name)

    con = database('GSVC_SQL').get_db()
    with con.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(query)
        qdata = cur.fetchall()
    con.close()
    return qdata

def flowlog_by_day_org(table_name):
    query = ("SELECT DATE(t2.date) AS date, t1.t_orgid, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, t_orgid, COUNT(DISTINCT imei) AS active_count, (SUM(sysFlow) + SUM(userFlow))/1024 AS flow_KB "
             "FROM `{}` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "GROUP BY createtime, t_orgid) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp").format(table_name)

    con = database('GSVC_SQL').get_db()
    with con.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(query)
        qdata = cur.fetchall()
    con.close()
    return qdata

def flowlog_by_day_type(table_name):
    query = ("SELECT DATE(t2.date) AS date, t1.t_type, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, t_type, COUNT(DISTINCT imei) AS active_count, (SUM(sysFlow) + SUM(userFlow))/1024 AS flow_KB "
             "FROM `{}` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "GROUP BY createtime, t_type) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp").format(table_name)

    con = database('GSVC_SQL').get_db()
    with con.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(query)
        qdata = cur.fetchall()
    con.close()
    return qdata

def flowlog_by_day_country_org(table_name):
    query = ("SELECT DATE(t2.date) AS date, t1.country, t1.t_orgid, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, country, t_orgid, COUNT(DISTINCT imei) AS active_count, (SUM(sysFlow) + SUM(userFlow))/1024 AS flow_KB "
             "FROM `{}` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "GROUP BY createtime, country, t_orgid) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp").format(table_name)

    con = database('GSVC_SQL').get_db()
    with con.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(query)
        qdata = cur.fetchall()
    con.close()
    return qdata

def flowlog_by_day_country_type(table_name):
    query = ("SELECT DATE(t2.date) AS date, t1.country, t1.t_type, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, country, t_type, COUNT(DISTINCT imei) AS active_count, (SUM(sysFlow) + SUM(userFlow))/1024 AS flow_KB "
             "FROM `{}` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "GROUP BY createtime, country, t_type) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp").format(table_name)

    con = database('GSVC_SQL').get_db()
    with con.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(query)
        qdata = cur.fetchall()
    con.close()
    return qdata

def flowlog_by_day_org_type(table_name):
    query = ("SELECT DATE(t2.date) AS date, t1.t_orgid, t1.t_type, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, t_orgid, t_type, COUNT(DISTINCT imei) AS active_count, (SUM(sysFlow) + SUM(userFlow))/1024 AS flow_KB "
             "FROM `{}` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "GROUP BY createtime, t_orgid, t_type) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp").format(table_name)

    con = database('GSVC_SQL').get_db()
    with con.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(query)
        qdata = cur.fetchall()
    con.close()
    return qdata

def flowlog_by_day_country_org_type(table_name):
    query = ("SELECT DATE(t1.date) AS date, t2.country, t2.t_orgid, t2.t_type, t2.active_count, t2.flow_KB FROM "
            "(SELECT createtime, country, t_orgid, t_type, "
            "COUNT(DISTINCT imei) AS active_count, SUM(sysFlow + userFlow)/1024 AS flow_KB "
            "FROM `{}` "
            "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
            "AND userFlow > 0 "
            "GROUP BY createtime, country, t_orgid, t_type) AS t2 "
            "LEFT JOIN `2017_datetime_timestamp` AS t1 ON t2.createtime = t1.timestamp").format(table_name)
    con = database('GSVC_SQL').get_db()
    with con.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(query)
        qdata = cur.fetchall()
    con.close()
    return qdata

# 数据修正
# 由于数据量不大，考虑了一下，不如清空数据表，重新跑程序。保证磁盘数据在时间上的连贯性
# 注意在处理完全部异常之后再清空重跑
# $tobecontinue
def data_amend():
    # MY 10-14系统流量异常
    # 日志表数据已经手动修正，通过脚本修正统计数据
    con = database('GSVC_SQL_ADMIN').get_db()
    cur = con.cursor(pymysql.cursors.DictCursor)
    delete = ["DELETE FROM `terminal_flow_count_2017_by_country_day` WHERE date = '2017-10-14' AND country = 'MY'"
            ,"DELETE FROM `terminal_flow_count_2017_by_country_org_day` WHERE date = '2017-10-14' AND country = 'MY'"
            ,"DELETE FROM `terminal_flow_count_2017_by_country_org_type_day` WHERE date = '2017-10-14' AND country = 'MY'"
            ,"DELETE FROM `terminal_flow_count_2017_by_country_type_day` WHERE date = '2017-10-14' AND country = 'MY'"
            ,"DELETE FROM `terminal_flow_count_2017_by_day` WHERE date = '2017-10-14'"
            ,"DELETE FROM `terminal_flow_count_2017_by_org_day` WHERE date = '2017-10-14'"
            ,"DELETE FROM `terminal_flow_count_2017_by_org_type_day` WHERE date = '2017-10-14'"
            ,"DELETE FROM `terminal_flow_count_2017_by_type_day` WHERE date = '2017-10-14'"]
    for d in delete:
        cur.execute(d)
    con.commit()
    # 1 - terminal_flow_count_2017_by_country_day
    re_query = ("SELECT DATE(t2.date) AS date, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, COUNT(DISTINCT imei) AS active_count, SUM(sysFlow + userFlow)/1024 AS flow_KB "
             "FROM `terminal_flow_count_2017_by_country_day` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "AND userFlow > 0 "
             "GROUP BY createtime) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp")
    cur.execute(re_query)
    rqdata = cur.fetchall()
    generic_insert(rqdata, 'terminal_flow_count_2017_by_country_day')

    # 2 - terminal_flow_count_2017_by_country_org_day
    re_query = ("SELECT DATE(t2.date) AS date, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, COUNT(DISTINCT imei) AS active_count, SUM(sysFlow + userFlow)/1024 AS flow_KB "
             "FROM `terminal_flow_count_2017_by_country_day` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "AND userFlow > 0 "
             "GROUP BY createtime) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp")
    cur.execute(re_query)
    rqdata = cur.fetchall()
    generic_insert(rqdata, 'terminal_flow_count_2017_by_country_day')
    # 3 - terminal_flow_count_2017_by_country_org_type_day
    re_query = ("SELECT DATE(t2.date) AS date, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, COUNT(DISTINCT imei) AS active_count, SUM(sysFlow + userFlow)/1024 AS flow_KB "
             "FROM `terminal_flow_count_2017_by_country_day` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "AND userFlow > 0 "
             "GROUP BY createtime) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp")
    cur.execute(re_query)
    rqdata = cur.fetchall()
    generic_insert(rqdata, 'terminal_flow_count_2017_by_country_day')
    # 4 - terminal_flow_count_2017_by_country_type_day
    re_query = ("SELECT DATE(t2.date) AS date, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, COUNT(DISTINCT imei) AS active_count, SUM(sysFlow + userFlow)/1024 AS flow_KB "
             "FROM `terminal_flow_count_2017_by_country_day` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "AND userFlow > 0 "
             "GROUP BY createtime) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp")
    cur.execute(re_query)
    rqdata = cur.fetchall()
    generic_insert(rqdata, 'terminal_flow_count_2017_by_country_day')
    # 5 - terminal_flow_count_2017_by_day
    re_query = ("SELECT DATE(t2.date) AS date, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, COUNT(DISTINCT imei) AS active_count, SUM(sysFlow + userFlow)/1024 AS flow_KB "
             "FROM `terminal_flow_count_2017_by_country_day` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "AND userFlow > 0 "
             "GROUP BY createtime) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp")
    cur.execute(re_query)
    rqdata = cur.fetchall()
    generic_insert(rqdata, 'terminal_flow_count_2017_by_country_day')
    # 6 - terminal_flow_count_2017_by_org_day
    re_query = ("SELECT DATE(t2.date) AS date, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, COUNT(DISTINCT imei) AS active_count, SUM(sysFlow + userFlow)/1024 AS flow_KB "
             "FROM `terminal_flow_count_2017_by_country_day` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "AND userFlow > 0 "
             "GROUP BY createtime) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp")
    cur.execute(re_query)
    rqdata = cur.fetchall()
    generic_insert(rqdata, 'terminal_flow_count_2017_by_country_day')
    # 7 - terminal_flow_count_2017_by_org_type_day
    re_query = ("SELECT DATE(t2.date) AS date, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, COUNT(DISTINCT imei) AS active_count, SUM(sysFlow + userFlow)/1024 AS flow_KB "
             "FROM `terminal_flow_count_2017_by_country_day` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "AND userFlow > 0 "
             "GROUP BY createtime) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp")
    cur.execute(re_query)
    rqdata = cur.fetchall()
    generic_insert(rqdata, 'terminal_flow_count_2017_by_country_day')
    # 8 - terminal_flow_count_2017_by_type_day
    re_query = ("SELECT DATE(t2.date) AS date, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, COUNT(DISTINCT imei) AS active_count, SUM(sysFlow + userFlow)/1024 AS flow_KB "
             "FROM `terminal_flow_count_2017_by_country_day` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "AND userFlow > 0 "
             "GROUP BY createtime) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp")
    cur.execute(re_query)
    rqdata = cur.fetchall()
    generic_insert(rqdata, 'terminal_flow_count_2017_by_country_day')

    cur.close()
    con.close()

if __name__ == '__main__':
    month = [i + 1 for i in range(12)] 
    
    # 1 - by day
    for m in month:
        qdata = flowlog_by_day('t_terminal_flow_count_day_2017{m:>02d}'.format(m=m))
        generic_insert(qdata, 'terminal_flow_count_2017_by_day')
    # 2 - by day, country 
        qdata = flowlog_by_day_country('t_terminal_flow_count_day_2017{m:>02d}'.format(m=m))
        generic_insert(qdata, 'terminal_flow_count_2017_by_country_day')
    # 3 - by day, org
        qdata = flowlog_by_day_org('t_terminal_flow_count_day_2017{m:>02d}'.format(m=m))
        generic_insert(qdata, 'terminal_flow_count_2017_by_org_day')
    # 4 - by day, type
        qdata = flowlog_by_day_type('t_terminal_flow_count_day_2017{m:>02d}'.format(m=m))
        generic_insert(qdata, 'terminal_flow_count_2017_by_type_day')
    # 5 - by day, country, org
        qdata = flowlog_by_day_country_org('t_terminal_flow_count_day_2017{m:>02d}'.format(m=m))
        generic_insert(qdata, 'terminal_flow_count_2017_by_country_org_day')
    # 6 - by day, country, type
        qdata = flowlog_by_day_country_type('t_terminal_flow_count_day_2017{m:>02d}'.format(m=m))
        generic_insert(qdata, 'terminal_flow_count_2017_by_country_type_day')
    # 7 - by day, org, type
        qdata = flowlog_by_day_org_type('t_terminal_flow_count_day_2017{m:>02d}'.format(m=m))
        generic_insert(qdata, 'terminal_flow_count_2017_by_org_type_day')
    # 8 - by day, country, org, type
        qdata = flowlog_by_day_country_org_type('t_terminal_flow_count_day_2017{m:>02d}'.format(m=m))
        generic_insert(qdata, 'terminal_flow_count_2017_by_country_org_type_day')
