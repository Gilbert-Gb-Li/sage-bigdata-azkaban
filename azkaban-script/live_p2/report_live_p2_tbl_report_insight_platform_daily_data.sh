#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1

echo "############### 行业洞察-行业统计 每天报表统计 start #####################"

mysql_table="tbl_live_p2_insight_platform_daily_data"

hive_sql="
SELECT '${day}',
        r.biz_name,
        r.data_source,
        r.live_count,
        r.live_time,
        r.active_user_count,
        r.new_active_user_count,
        r.recv_gift_live_count,
        r.recv_gift_user_count,
        r.new_recv_gift_user_count,
        r.recv_message_live_count,
        r.recv_message_user_count,
        r.new_recv_message_user_count,
        r.income,
        r.gift_count,
        r.message_count,
        r.audience_count,
        r.send_gift_audience_count,
        r.new_send_gift_audience_count,
        r.send_message_audience_count,
        r.new_send_message_audience_count,
        r.active_audience_count
FROM
(
  SELECT a.*, row_number() OVER (PARTITION BY biz_name,data_source ORDER BY record_time DESC) num
  FROM live_p2.tbl_ex_platform_daily_snapshot AS a
  WHERE dt='${day}'
) r where r.num=1
"

echo ${hive_sql}

hiveSqlToMysql "${hive_sql}" "${day}" "${mysql_table}" "dt,biz_name,data_source,live_count,live_time,active_user_count,new_active_user_count,recv_gift_live_count,recv_gift_user_count,new_recv_gift_user_count,recv_message_live_count,recv_message_user_count,new_recv_message_user_count,income,gift_count,message_count,audience_count,send_gift_audience_count,new_send_gift_audience_count,send_message_audience_count,new_send_message_audience_count,active_audience_count" "dt"

echo "############### 行业洞察-行业统计 每天报表统计 end #####################"
