#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1

echo "############### 直播监控 黄播app 报表统计 start #####################"

mysql_table="tbl_live_p2_monitor_violation_app"
mysql_tmp_table="tmp_tbl_live_p2_monitor_sex_app"

create_tmp_table="
CREATE TABLE ${mysql_tmp_table}(
  biz_name VARCHAR(100),
  start_time BIGINT,
  end_time BIGINT,
  audience_count BIGINT,
  income DOUBLE,
  gift_count INT,
  message_count INT,
  live_count INT,
  video TEXT
)
"

execSqlOnMysql "DROP TABLE IF EXISTS ${mysql_tmp_table}"
execSqlOnMysql "${create_tmp_table}"

echo "############### 直播监控 黄播app 临时record表 start #####################"

hive_sql1="
SELECT *
 FROM ias_p2.tbl_ex_live_record_data_origin_orc
 WHERE dt='${day}' AND app_package_name NOT IN (${sex_live_app_list})
"

tmp_table=$(hiveSqlToTmpHive "${hive_sql1}" "tmp_record_data")

echo "############### 直播监控 黄播app 临时record表 end #####################"

echo "############### 直播监控 黄播app 临时 start #####################"

hive_sql="
SELECT a.app_package_name,
       a.start_time,
       a.end_time,
       if (c.audience_count IS NOT NULL, c.audience_count, 0),
       if (e.income IS NOT NULL, e.income, 0),
       if (e.gift_count IS NOT NULL, e.gift_count, 0),
       if (d.message_count IS NOT NULL, d.message_count, 0),
       if (b.live_count IS NOT NULL, b.live_count, 0),
       CONCAT(
           '[',
           '{',
           '\"search_id\":', '\"',a.search_id,'\"',
           ',',
           '\"order_id\":', '\"',a.order_id,'\"',
           ',',
           '\"video_url\":', '\"',a.video_url,'\"',
           ',',
           '\"video_length\":',a.video_length,
           ',',
           '\"start_time\":',a.start_time,
           ',',
           '\"end_time\":',a.end_time,
           ',',
           '\"result_code\":',a.result_code,
           '}',
           ']'
         ) AS video_json
FROM
(
  ${tmp_table} AS a
  LEFT JOIN
  (SELECT a.app_package_name,a.order_id,
           count(DISTINCT b.user_id) AS live_count
    FROM
    ${tmp_table} AS a
    LEFT JOIN
    (SELECT *
     FROM ias_p2.tbl_ex_live_id_list_data_origin_orc
     WHERE dt='${day}' AND app_package_name NOT IN (${sex_live_app_list})
     ) b
    ON a.app_package_name=b.app_package_name
    WHERE (b.client_time >= a.start_time) AND (b.client_time <= a.end_time)
    GROUP BY a.app_package_name,a.order_id
  ) b
  ON a.app_package_name=b.app_package_name AND a.order_id=b.order_id
  LEFT JOIN
  (SELECT a.app_package_name,a.order_id,
           sum(b.audience_count) AS audience_count
    FROM
    ${tmp_table} AS a
    LEFT JOIN
    (SELECT app_package_name, avg(online_num) AS audience_count,avg(client_time) AS client_time
     FROM ias_p2.tbl_ex_live_record_audience_count_data_origin_orc
     WHERE dt='${day}' AND is_live=1 AND app_package_name NOT IN (${sex_live_app_list})
     GROUP BY app_package_name, user_id
     ) b
    ON a.app_package_name=b.app_package_name
    WHERE (b.client_time >= a.start_time) AND (b.client_time <= a.end_time)
    GROUP BY a.app_package_name,a.order_id
  ) c
  ON a.app_package_name=c.app_package_name AND a.order_id=c.order_id
  LEFT JOIN
  (SELECT a.app_package_name,a.order_id,
           count(1) AS message_count
    FROM
    ${tmp_table} AS a
    LEFT JOIN
    (SELECT biz_name,data_generate_time
     FROM live_p2.tbl_ex_message_info_snapshot
     WHERE dt='${day}' AND biz_name NOT IN (${sex_live_app_list})
     ) b
    ON a.app_package_name=b.biz_name
    WHERE (b.data_generate_time >= a.start_time) AND (b.data_generate_time <= a.end_time)
    GROUP BY a.app_package_name,a.order_id
  ) d
  ON a.app_package_name=d.app_package_name AND a.order_id=d.order_id
  LEFT JOIN
  (SELECT a.app_package_name,a.order_id,
           sum(b.gift_val) AS income,
           count(1) AS gift_count
    FROM
    ${tmp_table} AS a
    LEFT JOIN
    (SELECT biz_name,data_generate_time,gift_val
     FROM live_p2.tbl_ex_gift_info_snapshot
     WHERE dt='${day}' AND biz_name NOT IN (${sex_live_app_list})
     ) b
    ON a.app_package_name=b.biz_name
    WHERE (b.data_generate_time >= a.start_time) AND (b.data_generate_time <= a.end_time)
    GROUP BY a.app_package_name,a.order_id
  ) e
  ON a.app_package_name=e.app_package_name AND a.order_id=e.order_id
)
"

liveHiveSqlToMysqlNoConvert "${hive_sql}" "" "0" "${mysql_tmp_table}" "biz_name,start_time,end_time,audience_count,income,gift_count,message_count,live_count,video" "biz_name" "start_time"

dropHiveTable "${tmp_table}" "default"

echo "############### 直播监控 黄播app 临时表 end #####################"

sql1="
INSERT INTO ${mysql_table}(
  biz_name,
  app_name,
  version,
  version_code,
  apk_url,
  apk_size,
  apk_hash,
  app_type,
  app_icon_url,
  location,
  ip_info,
  contact_info,
  start_time,
  end_time,
  audience_count,
  income,
  gift_count,
  message_count,
  live_count,
  video,
  dt)
  SELECT b.biz_name,
         b.name,
         b.version,
         b.version_code,
         b.download_url,
         b.size,
         b.hash,
         b.type,
         b.icon_url,
         b.location,
         b.ip_info,
         b.contact_info,
         a.start_time,
         a.end_time,
         a.audience_count,
         a.income,
         a.gift_count,
         a.message_count,
         a.live_count,
         a.video,
         '${day}'
  FROM
  ${mysql_tmp_table} AS a
  LEFT JOIN
  (SELECT * FROM tbl_live_p2_app_info WHERE biz_name NOT IN (${sex_live_app_list})) b
  ON a.biz_name=b.biz_name
"

execSqlOnMysql "DELETE FROM ${mysql_table} WHERE dt='${day}'"
execSqlOnMysql "${sql1}"

execSqlOnMysql "DROP TABLE ${mysql_tmp_table}"


echo "############### 直播监控 黄播app 报表统计 start #####################"
