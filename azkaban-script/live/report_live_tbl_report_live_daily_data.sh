#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 直播每日报表统计 start #####################"
date=$1

mysql_table="tbl_report_live_daily_data"

hive_sql="select '${date}',biz_name,data_source,max(user_count),max(new_user_count),max(gift_send_count),max(send_message_user_count)
from (
  select biz_name,data_source,count(distinct user_id) as user_count,0 as new_user_count,0 as gift_send_count,0 as send_message_user_count
  from live.tbl_ex_anchor_active_snapshot where dt='${date}' group by biz_name,data_source
  union all
  select biz_name,data_source,0 as user_count,0 as new_user_count,count(distinct user_id) as gift_send_count,0 as send_message_user_count
  from live.tbl_ex_send_gift_active_user_snapshot where dt='${date}' group by biz_name,data_source
  union all
  select biz_name,data_source,0 as user_count,count(distinct user_id) as new_user_count,0 as gift_send_count,0 as send_message_user_count
  from live.tbl_ex_anchor_new_snapshot where dt='${date}' group by biz_name,data_source
  union all
  select biz_name,data_source,0 as user_count,0 as new_user_count,0 as gift_send_count,count(distinct user_id) as send_message_user_count
  from live.tbl_ex_message_info_snapshot
  where dt='${date}' group by biz_name,data_source
) as t
group by biz_name,data_source"

hiveSqlToMysql "${hive_sql}" "${date}" "${mysql_table}" "stat_date,biz_name,data_source,active_user_count,new_user_count,send_gift_user_count,send_message_user_count" "stat_date"

echo "############### 直播每日报表统计 end #####################"
