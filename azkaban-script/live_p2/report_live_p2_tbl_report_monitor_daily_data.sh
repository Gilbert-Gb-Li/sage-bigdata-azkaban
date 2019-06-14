#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1

echo "############### 直播监控 每天报表统计 start #####################"

last_seven_day=`date -d "-7 day $day" +%Y-%m-%d`

mysql_app_table="tbl_live_p2_monitor_app_daily_data_new"
mysql_daily_table="tbl_live_p2_monitor_daily_data_new"

execSqlOnMysql "DELETE FROM ${mysql_app_table} WHERE dt='${day}'"

hive_sql="
SELECT j.location,
       i.dt,
       i.biz_name,
       i.data_source,
       j.name,
       i.live_count,
       i.audience_count,
       i.income,
       i.violation_count,
       i.live_count_avg,
       i.audience_count_avg,
       i.income_avg,
       i.violation_count_avg
FROM
(
  SELECT '${day}' AS dt,
          x.biz_name,
          x.data_source,
          x.live_count,
          cast(cast(x.audience_count as bigint)/2 as INT) as audience_count,
          x.income,
          x.violation_count,
          if (y.live_count_avg IS NOT NULL, y.live_count_avg, 0) AS live_count_avg,
          if (y.audience_count_avg IS NOT NULL, y.audience_count_avg, 0) AS audience_count_avg,
          if (y.income_avg IS NOT NULL, y.income_avg, 0) AS income_avg,
          if (y.violation_count_avg IS NOT NULL, y.violation_count_avg, 0) AS violation_count_avg
  FROM
  (
  SELECT * FROM
    (
      SELECT a.*, row_number() OVER (PARTITION BY biz_name,data_source ORDER BY record_time DESC) num
      FROM live_p2.tbl_ex_platform_daily_snapshot AS a
      WHERE dt='${day}'
    ) r where r.num=1
  ) x
  LEFT JOIN
  (
    SELECT biz_name,
           cast(avg(live_count) AS INT) AS live_count_avg,
           cast((avg(audience_count)/2) AS INT) AS audience_count_avg,
           avg(income) AS income_avg,
           cast(avg(violation_count) AS INT) AS violation_count_avg
        FROM live_p2.tbl_ex_platform_daily_snapshot AS a
        WHERE dt>='${last_seven_day}' AND dt<'${day}'
        GROUP BY biz_name
  ) y
  ON x.biz_name=y.biz_name
) i
LEFT JOIN
(
  SELECT biz_name,location,max(name) AS name FROM live_p2.tbl_live_p2_app_location
  GROUP BY biz_name,location
) j
ON i.biz_name=j.biz_name
"

hiveSqlToMysql "${hive_sql}" "${day}" "${mysql_app_table}" "location,dt,biz_name,data_source,name,live_count,audience_count,income,violation_count,live_count_avg,audience_count_avg,income_avg,violation_count_avg" "dt"

sql1="
INSERT INTO ${mysql_app_table}(location,dt,biz_name,data_source,name,live_count,audience_count,income,violation_count,live_count_avg,audience_count_avg,income_avg,violation_count_avg)
  SELECT '全国',
         '${day}',
         biz_name,
         data_source,
         max(name),
         avg(live_count),
         avg(audience_count),
         avg(income),
         avg(violation_count),
         avg(live_count_avg),
         avg(audience_count_avg),
         avg(income_avg),
         avg(violation_count_avg)
  FROM ${mysql_app_table}
  WHERE dt='${day}' AND location!='境外'
  GROUP BY biz_name,data_source
"

execSqlOnMysqlUTF8 "${sql1}"


echo "############### 直播监控 每天报表整体表 start #####################"

execSqlOnMysql "DELETE FROM ${mysql_daily_table} WHERE dt='${day}'"

sql2="
INSERT INTO ${mysql_daily_table}(location,dt,app_count,live_count,audience_count,income,violation_count,live_count_avg,audience_count_avg,income_avg,violation_count_avg)
  SELECT location,
         '${day}',
         count(1),
         sum(live_count),
         sum(audience_count),
         sum(income),
         sum(violation_count),
         sum(live_count_avg),
         sum(audience_count_avg),
         sum(income_avg),
         sum(violation_count_avg)
  FROM ${mysql_app_table}
  WHERE dt='${day}'
  GROUP BY location
"
execSqlOnMysqlUTF8 "${sql2}"


echo "############### 直播监控 每天报表整体表 end #####################"

echo "############### 直播监控 每天报表统计 end #####################"
