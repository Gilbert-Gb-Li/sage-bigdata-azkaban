#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 生成kol快照表

# azkaban上系统设置 参数日期=当前日期-1天
yesterday=$1
dayBeforeYesterday=${yesterday}
dayBeforeYesterday2=`date -d "-1 day $yesterday" +%Y-%m-%d`
maxPartitionKol=${dayBeforeYesterday2}

# 增加参数粉丝数大于等于8000的kol为有效KOL
KOL_FANS_NUM=8000
QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

RECENT_DAY_ID1=$(hive -e "show partitions bigdata.douyin_advert_kol_data_snapshot;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -ur|head -n 1)

y1=`date -d "$dayBeforeYesterday2" +%s`
r1=`date -d "$RECENT_DAY_ID1" +%s`
if [[ ${y1} -gt ${r1} ]] ;then
    maxPartitionKol=${RECENT_DAY_ID1}
fi

hive_sql1="
insert overwrite table bigdata.douyin_advert_kol_data_snapshot partition(dt='${dayBeforeYesterday}')
select record_time,
       douyin_id,
       short_video_count,
       like_video_count,
       location,
       sex,
       nick_name,
       user_id,
       follower_count,
       like_count,
       following_count,
       age,
       province,
       city,
       app_version,
       app_package_name,
       signature,
       certificate_type,
       certificate_info
  from (select t.*,
               row_number() over(partition by user_id order by record_time desc) order_seq
          from (select record_time,
                       douyin_id,
                       short_video_count,
                       like_video_count,
                       location,
                       sex,
                       nick_name,
                       user_id,
                       follower_count,
                       like_count,
                       following_count,
                       age,
                       province,
                       city,
                       app_version,
                       app_package_name,
                       signature,
                       certificate_type,
                       certificate_info
                  from bigdata.douyin_advert_kol_data_snapshot
                 where dt = '${maxPartitionKol}'
                union all
                select record_time,
                       douyin_id,
                       short_video_count,
                       like_video_count,
                       location,
                       sex,
                       nick_name,
                       user_id,
                       follower_count,
                       like_count,
                       following_count,
                       age,
                       province,
                       city,
                       app_version,
                       app_package_name,
                       signature,
                       certificate_type,
                       certificate_info
                  from bigdata.douyin_user_data_origin_orc
                 where dt = '${dayBeforeYesterday}'
                   and follower_count >= ${KOL_FANS_NUM}
                   and user_id is not null
                   and user_id != ''
                union all
                select record_time,
                       douyin_id,
                       short_video_count,
                       like_video_count,
                       location,
                       sex,
                       nick_name,
                       user_id,
                       follower_count,
                       like_count,
                       following_count,
                       age,
                       provice,
                       city,
                       app_version,
                       app_package_name,
                       signature,
                       certificate_type,
                       certificate_info
                  from bigdata.douyin_hot_recommend_video_and_user_data_origin_orc
                 where dt = '${dayBeforeYesterday}'
                   and user_id is not null
                   and user_id != '') t) p
 where p.order_seq = 1;
"
executeHiveCommand "${COMMON_VAR}${hive_sql1}"