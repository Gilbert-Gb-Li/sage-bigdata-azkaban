#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="insert into short_video.tbl_ex_short_video_user_daily_snapshot partition(dt='${date}')
select record_time,user_id,nick_name,user_name,avatar_url,short_video_count,following_count,
follower_count,birthday,user_location,sex,like_count,signature,weibo_url,mplatform_followers_count,
custom_verify,internal_uid,favoriting_video_count,enterprise_verify_reason,verification_type,
weibo_verify,constellation,pre_uid,relationship,template_name
from
(select *,row_number() over (partition by user_id order by record_time desc) as order_num from (
select record_time,user_id,nick_name,user_name,avatar_url,short_video_count,following_count,
follower_count,birthday,user_location,sex,like_count,signature,weibo_url,mplatform_followers_count,
custom_verify,internal_uid,favoriting_video_count,enterprise_verify_reason,verification_type,
weibo_verify,constellation,pre_uid,relationship,template_name
from ias.tbl_ex_short_video_user_origin_orc 
where dt='${date}'
union all
select record_time,user_id,nick_name,user_name,avatar_url,short_video_count,following_count,
follower_count,birthday,user_location,sex,like_count,signature,weibo_url,mplatform_followers_count,
custom_verify,internal_uid,favoriting_video_count,enterprise_verify_reason,verification_type,
weibo_verify,constellation,pre_uid,relationship,template_name
from short_video.tbl_ex_short_video_user_daily_snapshot 
where dt='${yesterday}'
)as p
)as t
where t.order_num =1"

executeHiveCommand "${hive_sql}"