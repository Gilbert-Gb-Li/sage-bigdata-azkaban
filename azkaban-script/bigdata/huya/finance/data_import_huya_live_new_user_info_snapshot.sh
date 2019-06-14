#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

################################
########计算新增主播快照#########
################################

hive_sql1="
insert into bigdata.huya_live_new_user_info_snapshot partition(dt='${date}')
select 
      a.data_generate_time,
      a.resource_key,
      a.spider_version,
      a.app_version,
      a.user_id,
      a.user_name,
      a.user_age,
      a.user_sign,
      a.user_image,
      a.province,
      a.city,
      a.user_level,
      a.user_love_channel,
      a.user_subscribe_num,
      a.user_fans_num,
      a.favor_num,
      a.live_id,
      a.live_desc,
      a.user_notice,
      a.share_url,
      a.target_id,
      a.live_start_time,
      a.live_end_time,
      a.live_duration,
      a.live_num,
      a.max_online_num,
      a.income,
      a.pay,
      a.interact_count,
      a.contribute_count,
      a.arpu
from
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
          target_id,
          live_start_time,
          live_end_time,
          live_duration,
          live_num,
          max_online_num,
          income,
          pay,
          interact_count,
          contribute_count,
          arpu
    from bigdata.huya_live_active_user_info_snapshot 
    where dt = '${date}'
) a
left join
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
    from bigdata.huya_live_all_user_info_snapshot 
    where dt = '${yesterday}'
) b
on a.user_id = b.user_id
where b.user_id is null
;"

executeHiveCommand "${hive_sql1}"