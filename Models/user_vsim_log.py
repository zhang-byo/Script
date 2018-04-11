from ..utils import format_datetime, mkdatetime
from ..database import database
import pymysql

TABLE = 't_css_vsim'
T_CSS_VSIM_PACKAGES = 't_css_vsim_packages'
T_CSS_PACKAGE_TYPE = 't_css_package_type'
T_CSS_USER_VSIM_LOG = 't_css_user_vsim_log'
T_CSS_PLMNSET = 't_css_plmnset'

def get_vsim_dispatch_record(imsi_list, begin_datetime, end_datetime):
    query_vsim_info = (
        "SELECT "
        "a.`iso2` AS 'country', "
        "(CAST(a.`imsi` AS CHAR)) AS 'imsi', "
        "a.`iccid`, "
        "e.`name` AS package_type_name, "
        "DATE_FORMAT(b.`next_update_time`, '%Y-%m-%d %H:%i:%s') AS 'next_update_time', "
        "a.`bam_id` AS 'bam', "
        "p.`name` AS 'sim_agg', "
        "COUNT(c.`imsi`)AS 'imsi_count', "
        "COUNT(DISTINCT c.`imei`)AS 'imei_count' "
        "FROM `t_css_vsim` AS a "
        "LEFT JOIN `t_css_vsim_packages` b "
        "  ON a.`imsi`=b.`imsi`  "
        "LEFT JOIN `t_css_user_vsim_log` AS c "
        "  ON c.`imsi`=a.`imsi` "
        "LEFT JOIN `t_css_package_type` AS e "
        "  ON e.`id` = b.`package_type_id` "
        "LEFT JOIN `{css_plmnset}` AS p "
        "  ON a.`plmnset_id`=p.`id` "
        "WHERE  "
        "  a.`imsi` IN ({imsi_list})"
        "  AND a.`bam_status`='0' "
        "  AND a.`slot_status`='0' "
        "  AND a.`available_status`='0' "
        "  AND c.`create_time`>= '{begin_datetime}'"
        "  AND c.`create_time`< '{end_datetime}'"
        "GROUP BY a.`iso2`, "
        "a.`imsi`, "
        "a.`iccid`, "
        "e.`name`, "
        "b.`next_update_time` "
    ).format(
        imsi_list=",".join(imsi_list),
        begin_datetime=format_datetime(begin_datetime),
        end_datetime=format_datetime(end_datetime),
        css_plmnset=T_CSS_PLMNSET
    )
    css_con = database('CSS_SQL').get_db()
    try:
        with css_con.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query_vsim_info)
            imsi_info = list(cursor.fetchall())
    finally:
        css_con.close()
    
    return imsi_info