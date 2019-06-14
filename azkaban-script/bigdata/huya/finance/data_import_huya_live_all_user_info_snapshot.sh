#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

################################
########计算全量主播快照#########
################################

hive_sql1="
insert into bigdata.huya_live_all_user_info_snapshot partition(dt='${date}')
select 
      data_generate_time,
      resource_key,
      spider_version,
      app_version,
      user_id,
      user_name,
      user_age,
      user_sign,
      user_image,
      province,
      city,
      user_level,
      user_love_channel,
      user_subscribe_num,
      user_fans_num,
      favor_num,
      live_id,
      live_desc,
      user_notice,
      share_url,
      target_id
from
(
    select *,row_number() over (partition by user_id order by data_generate_time desc) as order_num from
    (
        select 
              data_generate_time,
              resource_key,
              spider_version,
              app_version,
              user_id,
              user_name,
              user_age,
              user_sign,
              user_image,
              province,
              city,
              user_level,
              user_love_channel,
              user_subscribe_num,
              user_fans_num,
              favor_num,
              live_id,
              live_desc,
              user_notice,
              share_url,
              target_id
        from bigdata.huya_live_active_user_info_snapshot 
        where dt = '${date}'
        
        UNION ALL
        
        select 
              data_generate_time,
              resource_key,
              spider_version,
              app_version,
              user_id,
              user_name,
              user_age,
              user_sign,
              user_image,
              province,
              city,
              user_level,
              user_love_channel,
              user_subscribe_num,
              user_fans_num,
              favor_num,
              live_id,
              live_desc,
              user_notice,
              share_url,
              target_id
        from bigdata.huya_live_all_user_info_snapshot 
        where dt = '${yesterday}'
    ) s
) m
where m.order_num = 1;
"

executeHiveCommand "${hive_sql1}"