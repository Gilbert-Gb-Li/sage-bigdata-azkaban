#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="insert into bigdata.douyin_hot_search_list_daily_snapshot partition(dt='${date}')
select record_time,challenge_id,commodity_id,author_id,short_video_id from
(select *,row_number() over (partition by short_video_id,challenge_id order by record_time desc) as order_num from (
select record_time,challenge_id,commodity_id,author_id,short_video_id from bigdata.douyin_hot_search_list_data_origin_orc where dt = '${date}'
union all
select record_time,challenge_id,commodity_id,author_id,short_video_id from bigdata.douyin_hot_search_list_daily_snapshot where dt = '${yesterday}'
)as p)as t
where t.order_num =1;"

executeHiveCommand "${hive_sql}"