###
 # @Author: yongle.chen 
 # @Date: 2018-05-29 11:19:39
 # @Last Modified by:   yongle.chen 
 # @Last Modified time: 2018-05-30 17:11:48 
 # @Description: 
 # 按天对country-org-devtype对数据进行聚合,整合到一个表中
 # 由于数据逻辑关系，并不需要对全部维度都进行聚合
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
import datetime
import pyexcel
import pymysql

# $todo: 将generic_insert整合到database工具中
@tick_time
def generic_insert(data, target_table):
    # 数据库查出的字段不存在不规则字典的情况
    # 但如果是mongo过来的不规则字典，则最好在插入前做数据清理
    # 每20000条数据插入一次, 20000条数据预期为2-3M大小
    i = 0
    count = 0
    con = database('GSVC_SQL_ADMIN').get_db()
    cur = con.cursor()

    key_name = data[1].keys()
    
    while i < len(data):
        subdata = [tuple(x.values()) for x in data[i:(i + 20000)]]
    
        insert_stmt = "INSERT INTO `{target_table}` (`{colname}`) VALUES ({values})".format(
            target_table=target_table,
            colname="`,`".join(key_name),
            values=','.join(['%s' for x in range(len(key_name))])
        )
        if i == 0:
            print(insert_stmt)

        try:
            effect_row = cur.executemany(insert_stmt, subdata)
            count += effect_row
            con.commit()
        except pymysql.err.Error as e:
            print('INSERT ROWS: {}'.format(count))
            cur.close()
            con.close()
            raise e
        i += 20000

    print('INSERT ROWS: {}'.format(count))
    cur.close()
    con.close()

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

# 数据可以在org-type-day表统计
# 单独day, 单独type, 单独org,或者org-type的数据,都可以通过中间表day-org-type二次计算获得
@tick_time
def flowlog_by_day_org_type(table_name):
    query = ("SELECT DATE(t2.date) AS date, t1.country, t1.t_type, t1.active_count, t1.flow_KB FROM ( "
             "SELECT createtime, country, t_type, COUNT(DISTINCT imei) AS active_count, (SUM(sysFlow) + SUM(userFlow))/1024 AS flow_KB "
             "FROM `{}` "
             "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
             "AND userFlow > 0 "
             "AND sysFlow < 2 * 1024 * 1024 * 1024 "
             "AND sysFlow > 0 "
             "GROUP BY createtime, country, t_type) AS t1 "
             "LEFT JOIN `2017_datetime_timestamp` AS t2 ON t1.createtime = t2.timestamp").format(table_name)

    con = database('GSVC_SQL').get_db()
    with con.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(query)
        qdata = cur.fetchall()
    con.close()
    return qdata

# 由于存在imei一天在多个国家登录的场景, 对中间表二次计算来获得国家活跃设备数会有误差
# 按天SUM(count)的数据远比在日志表中COUNT(DISTINCT imei)的数据要大的多
# 因此国家的信息需要单独聚合
@tick_time
def flowlog_by_day_country_org_type(table_name):
    query = ("SELECT DATE(t1.date) AS date, t2.country, t2.t_orgid, t2.t_type, t2.active_count, t2.flow_KB FROM "
            "(SELECT createtime, country, t_orgid, t_type, "
            "COUNT(DISTINCT imei) AS active_count, SUM(sysFlow + userFlow)/1024 AS flow_KB "
            "FROM `{}` "
            "WHERE userFlow < 2 * 1024 * 1024 * 1024 "
            "AND userFlow > 0 "
            "AND sysFlow < 2 * 1024 * 1024 * 1024 "
            "AND sysFlow > 0 "
            "GROUP BY createtime, country, t_orgid, t_type) AS t2 "
            "LEFT JOIN `2017_datetime_timestamp` AS t1 ON t2.createtime = t1.timestamp").format(table_name)
    con = database('GSVC_SQL').get_db()
    with con.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(query)
        qdata = cur.fetchall()
    con.close()
    return qdata

# 数据修正
# 由于数据量不大，考虑了一下，不如清空数据表，重新跑程序。保证磁盘数据在时间和空间的连续性
# 注意在处理完全部异常之后再清空重跑
def amend():
    pass

if __name__ == '__main__':
    month = [i + 1 for i in range(12)] 
    # 1 - by day, org, type
    for m in month:
        qdata = flowlog_by_day_org_type('t_terminal_flow_count_day_2017{m:>02d}'.format(m=m))
        generic_insert(qdata, 'terminal_flow_count_2017_by_org_type_day')
    # 2 - by day, country, org, type
    # 处理完第一个表再处理第二个表，方便数据异常处理
    for m in month:
        qdata = flowlog_by_day_country_org_type('t_terminal_flow_count_day_2017{m:>02d}'.format(m=m))
        generic_insert(qdata, 'terminal_flow_count_2017_by_country_org_type_day')
