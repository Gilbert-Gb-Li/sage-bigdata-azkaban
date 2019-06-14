#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1

hive_sql="
insert into bigdata.douyin_hot_recommend_video_and_user_daily_snapshot partition(dt='${date}')
select
      record_time,
      resource_key,
      app_version,
      app_package_name,
      douyin_id,
      dynamic_count,
      short_video_count,
      like_video_count,
      school,
      location,
      sex,
      nick_name,
      user_id,
      signature,
      follower_count,
      like_count,
      certificate_type,
      certificate_info,
      following_count,
      shop_window,
      age,
      provice,
      city,
      commodity,
      video_desc,
      video_location,
      video_effects
from
(
    select
          *,
          row_number() over (partition by user_id,video_desc order by record_time desc) as order_num
    from bigdata.douyin_hot_recommend_video_and_user_data_origin_orc
    where dt = '${date}'
         and user_id != ''
) t
where t.order_num = 1
;"

executeHiveCommand "${hive_sql}"
