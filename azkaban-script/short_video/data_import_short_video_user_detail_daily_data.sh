#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="insert into short_video.tbl_ex_short_video_user_detail_daily_snapshot partition(dt='${date}')
select record_time,user_id,follower_id_list,following_id_list,short_video_count,like_count,follower_count,following_count,nick_name
from
(select *,row_number() over (partition by user_id order by record_time desc) as order_num from (
select record_time,user_id,follower_id_list,following_id_list,short_video_count,like_count,follower_count,following_count,nick_name
from ias.tbl_ex_short_video_user_origin_orc
where dt='${date}' and (size(follower_id_list)>0 or size(following_id_list)>0)
union all
select record_time,user_id,follower_id_list,following_id_list,short_video_count,like_count,follower_count,following_count,nick_name
from short_video.tbl_ex_short_video_user_detail_daily_snapshot
where dt='${yesterday}'
)as p
)as t
where t.order_num =1"

executeHiveCommand "${hive_sql}"