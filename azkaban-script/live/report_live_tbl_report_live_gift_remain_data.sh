#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 直播打赏用户留存统计 start #####################"
date=$1

mysql_table="tbl_report_live_daily_data"
tmp_mysql_remain_table="tmp_tbl_report_live_daily_data_gift_remain_data"

next_remain_date=`date -d "-1 day $date" +%Y-%m-%d`
third_remain_date=`date -d "-2 day $date" +%Y-%m-%d`
seven_remain_date=`date -d "-6 day $date" +%Y-%m-%d`

create_tmp_mysql_remain_table_sql="CREATE TABLE IF NOT EXISTS ${tmp_mysql_remain_table} (stat_date CHAR(10),biz_name VARCHAR(100),data_source VARCHAR(10),remain_count INT(11),remain_day TINYINT(2))"
execSqlOnMysql "${create_tmp_mysql_remain_table_sql}"

next_remain_hive_sql="SELECT '${next_remain_date}',a.biz_name,a.data_source,COUNT(DISTINCT a.user_id) AS remain_count,'2' AS remain_day
FROM (
    SELECT user_id,biz_name,data_source
    FROM live.tbl_ex_send_gift_new_user_snapshot
    WHERE dt='${next_remain_date}'
    GROUP BY user_id,biz_name,data_source
) a
JOIN (
    SELECT user_id,biz_name,data_source
    FROM live.tbl_ex_send_gift_active_user_snapshot
    WHERE dt='${date}'
    GROUP BY user_id,biz_name,data_source
) b
ON a.user_id=b.user_id AND a.biz_name=b.biz_name AND a.data_source=b.data_source
GROUP BY a.biz_name,a.data_source"

third_remain_hive_sql="SELECT '${third_remain_date}',a.biz_name,a.data_source,COUNT(DISTINCT a.user_id) AS remain_count,'3' AS remain_day
FROM (
    SELECT user_id,biz_name,data_source
    FROM live.tbl_ex_send_gift_new_user_snapshot
    WHERE dt='${third_remain_date}'
    GROUP BY user_id,biz_name,data_source
) a
JOIN (
    SELECT user_id,biz_name,data_source
    FROM live.tbl_ex_send_gift_active_user_snapshot
    WHERE dt='${date}'
    GROUP BY user_id,biz_name,data_source
) b
ON a.user_id=b.user_id AND a.biz_name=b.biz_name AND a.data_source=b.data_source
GROUP BY a.biz_name,a.data_source"

seven_remain_hive_sql="SELECT '${seven_remain_date}',a.biz_name,a.data_source,COUNT(DISTINCT a.user_id) AS remain_count,'7' AS remain_day
FROM (
    SELECT user_id,biz_name,data_source
    FROM live.tbl_ex_send_gift_new_user_snapshot
    WHERE dt='${seven_remain_date}'
    GROUP BY user_id,biz_name,data_source
) a
JOIN (
    SELECT user_id,biz_name,data_source
    FROM live.tbl_ex_send_gift_active_user_snapshot
    WHERE dt='${date}'
    GROUP BY user_id,biz_name,data_source
) b
ON a.user_id=b.user_id AND a.biz_name=b.biz_name AND a.data_source=b.data_source
GROUP BY a.biz_name,a.data_source"

hive_sql="${next_remain_hive_sql} UNION ALL ${third_remain_hive_sql} UNION ALL ${seven_remain_hive_sql}"

hiveSqlToMysqlNoDelete "${hive_sql}" "${tmp_mysql_remain_table}" "stat_date,biz_name,data_source,remain_count,remain_day"

update_next_remain_sql="UPDATE $mysql_table a
JOIN (
  SELECT stat_date,biz_name,data_source,remain_count,remain_day
  FROM $tmp_mysql_remain_table
  WHERE stat_date='${next_remain_date}' AND remain_day='2'
) b
ON a.stat_date=b.stat_date AND a.biz_name=b.biz_name AND a.data_source = b.data_source
SET a.send_gift_next_remain=b.remain_count
WHERE a.stat_date='${next_remain_date}';"
execSqlOnMysql "${update_next_remain_sql}"

update_third_remain_sql="UPDATE $mysql_table a
JOIN (
  SELECT stat_date,biz_name,data_source,remain_count,remain_day
  FROM $tmp_mysql_remain_table
  WHERE stat_date='${third_remain_date}' AND remain_day='3'
) b
ON a.stat_date=b.stat_date AND a.biz_name=b.biz_name AND a.data_source = b.data_source
SET a.send_gift_third_remain=b.remain_count
WHERE a.stat_date='${third_remain_date}';"
execSqlOnMysql "${update_third_remain_sql}"

update_seven_remain_sql="UPDATE $mysql_table a
JOIN (
  SELECT stat_date,biz_name,data_source,remain_count,remain_day
  FROM $tmp_mysql_remain_table
  WHERE stat_date='${seven_remain_date}' AND remain_day='7'
) b
ON a.stat_date=b.stat_date AND a.biz_name=b.biz_name AND a.data_source = b.data_source
SET a.send_gift_seven_remain=b.remain_count
WHERE a.stat_date='${seven_remain_date}';"
execSqlOnMysql "${update_seven_remain_sql}"

drop_tmp_mysql_remain_table_sql="DROP TABLE ${tmp_mysql_remain_table}"
execSqlOnMysql "${drop_tmp_mysql_remain_table_sql}"

echo "############### 直播打赏用户留存统计 end #####################"
