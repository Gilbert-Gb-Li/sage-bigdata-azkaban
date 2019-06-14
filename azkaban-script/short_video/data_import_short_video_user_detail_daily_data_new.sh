#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
echo "${date}"
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
echo "${yesterday}"

hive_sql="insert into short_video.tbl_ex_short_video_user_detail_daily_snapshot partition(dt='${date}')
select record_time,user_id,follower_id_list,following_id_list,short_video_count,like_count,follower_count,following_count,nick_name,weibo_url,mplatform_followers_count,internal_uid,favoriting_video_count
from
(select *,row_number() over (partition by user_id order by record_time desc) as order_num from (
select record_time,a.user_id,b.follower_id_list,b.following_id_list,short_video_count,like_count,follower_count,following_count,b.nick_name,weibo_url,mplatform_followers_count,internal_uid,favoriting_video_count
from (
select record_time,user_id,short_video_count,like_count,follower_count,following_count,weibo_url,mplatform_followers_count,internal_uid,favoriting_video_count
from ias.tbl_ex_short_video_user_detail_origin_orc
where dt='${date}') as a
left join (
select user_id,follower_id_list,following_id_list,nick_name
from short_video.tbl_ex_short_video_user_detail_daily_snapshot
where dt='${yesterday}') as b
on (a.user_id=b.user_id)
union all
select record_time,user_id,follower_id_list,following_id_list,short_video_count,like_count,follower_count,following_count,nick_name,weibo_url,mplatform_followers_count,internal_uid,favoriting_video_count
from short_video.tbl_ex_short_video_user_detail_daily_snapshot
where dt='${yesterday}'
)as p
)as t
where t.order_num =1"
echo "${hive_sql}"

executeHiveCommand "${hive_sql}"