#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 微博发微博活跃留存统计 start #####################"
date=$1

mysql_table="tbl_report_weibo_daily_data"
tmp_mysql_remain_table="tmp_tbl_report_weibo_daily_data_remain_data"

next_remain_date=`date -d "-1 day $date" +%Y-%m-%d`
third_remain_date=`date -d "-2 day $date" +%Y-%m-%d`
seven_remain_date=`date -d "-6 day $date" +%Y-%m-%d`

create_tmp_mysql_remain_table_sql="CREATE TABLE IF NOT EXISTS ${tmp_mysql_remain_table} (stat_date CHAR(10),remain_count INT(11),remain_day TINYINT(2))"
execSqlOnMysql "${create_tmp_mysql_remain_table_sql}"

next_remain_hive_sql="SELECT '${next_remain_date}',COUNT(DISTINCT a.user_id) AS remain_count,'2' AS remain_day
FROM (
    SELECT a.user_id
    FROM sns.tbl_ex_weibo_user_action_snapshot a
    JOIN sns.weibo_monitor_user b
    ON a.user_id=b.weibo_id
    WHERE a.dt='${next_remain_date}' AND CAST(a.send_count AS bigint) > 0
) a
JOIN (
  SELECT a.user_id
  FROM sns.tbl_ex_weibo_user_action_snapshot a
  JOIN sns.weibo_monitor_user b
  ON a.user_id=b.weibo_id
  WHERE a.dt='${date}' AND CAST(a.send_count AS bigint) > 0
) b
ON a.user_id=b.user_id"

third_remain_hive_sql="SELECT '${third_remain_date}',COUNT(DISTINCT a.user_id) AS remain_count,'3' AS remain_day
FROM (
    SELECT a.user_id
    FROM sns.tbl_ex_weibo_user_action_snapshot a
    JOIN sns.weibo_monitor_user b
    ON a.user_id=b.weibo_id
    WHERE a.dt='${third_remain_date}' AND CAST(a.send_count AS bigint) > 0
) a
JOIN (
  SELECT a.user_id
  FROM sns.tbl_ex_weibo_user_action_snapshot a
  JOIN sns.weibo_monitor_user b
  ON a.user_id=b.weibo_id
  WHERE a.dt='${date}' AND CAST(a.send_count AS bigint) > 0
) b
ON a.user_id=b.user_id"

seven_remain_hive_sql="SELECT '${seven_remain_date}',COUNT(DISTINCT a.user_id) AS remain_count,'7' AS remain_day
FROM (
    SELECT a.user_id
    FROM sns.tbl_ex_weibo_user_action_snapshot a
    JOIN sns.weibo_monitor_user b
    ON a.user_id=b.weibo_id
    WHERE a.dt='${seven_remain_date}' AND CAST(a.send_count AS bigint) > 0
) a
JOIN (
  SELECT a.user_id
  FROM sns.tbl_ex_weibo_user_action_snapshot a
  JOIN sns.weibo_monitor_user b
  ON a.user_id=b.weibo_id
  WHERE a.dt='${date}' AND CAST(a.send_count AS bigint) > 0
) b
ON a.user_id=b.user_id"

hive_sql="${next_remain_hive_sql} UNION ALL ${third_remain_hive_sql} UNION ALL ${seven_remain_hive_sql}"

hiveSqlToMysqlNoDelete "${hive_sql}" "${tmp_mysql_remain_table}" "stat_date,remain_count,remain_day"

update_next_remain_sql="UPDATE $mysql_table a
JOIN (
  SELECT stat_date,remain_count,remain_day
  FROM $tmp_mysql_remain_table
  WHERE stat_date='${next_remain_date}' AND remain_day='2'
) b
ON a.stat_date=b.stat_date
SET a.send_user_next_remain=b.remain_count
WHERE a.stat_date='${next_remain_date}';"
execSqlOnMysql "${update_next_remain_sql}"

update_third_remain_sql="UPDATE $mysql_table a
JOIN (
  SELECT stat_date,remain_count,remain_day
  FROM $tmp_mysql_remain_table
  WHERE stat_date='${third_remain_date}' AND remain_day='3'
) b
ON a.stat_date=b.stat_date
SET a.send_user_third_remain=b.remain_count
WHERE a.stat_date='${third_remain_date}';"
execSqlOnMysql "${update_third_remain_sql}"

update_seven_remain_sql="UPDATE $mysql_table a
JOIN (
  SELECT stat_date,remain_count,remain_day
  FROM $tmp_mysql_remain_table
  WHERE stat_date='${seven_remain_date}' AND remain_day='7'
) b
ON a.stat_date=b.stat_date
SET a.send_user_seven_remain=b.remain_count
WHERE a.stat_date='${seven_remain_date}';"
execSqlOnMysql "${update_seven_remain_sql}"

drop_tmp_mysql_remain_table_sql="DROP TABLE ${tmp_mysql_remain_table}"
execSqlOnMysql "${drop_tmp_mysql_remain_table_sql}"

echo "############### 微博发微博活跃留存统计 end #####################"
