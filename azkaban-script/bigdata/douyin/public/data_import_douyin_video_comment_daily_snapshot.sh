#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="insert into bigdata.douyin_video_comment_daily_snapshot partition(dt='${date}')
select record_time,created_time,like_count,comment_total,short_video_id,user_id,comment,comment_type,reply_id,comment_id  from
(select *,row_number() over (partition by comment_id order by record_time desc) as order_num from (
select record_time,created_time,like_count,comment_total,short_video_id,user_id,comment,comment_type,reply_id,comment_id  from bigdata.douyin_video_comment_data_origin_orc where dt='${date}'
union all
select record_time,created_time,like_count,comment_total,short_video_id,user_id,comment,comment_type,reply_id,comment_id  from bigdata.douyin_video_comment_daily_snapshot where dt='${yesterday}'
)as p
)as t
where t.order_num =1;"

executeHiveCommand "${hive_sql}"
