#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 生成kol补充表

# azkaban上系统设置 参数日期=当前日期-1天
yesterday=$1
dayBeforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive_sql1="
insert overwrite table bigdata.douyin_advert_kol_snapshot partition
  (dt = '${dayBeforeYesterday}')
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
         coalesce(t1.video_count, 0),
         case short_video_count <= coalesce(t1.video_count, 0)
           when true then
            1
           else
            0
         end as ifvalid,
         t1.avatar_url
    from (select *
            from bigdata.douyin_advert_kol_data_snapshot
           where dt = '${dayBeforeYesterday}') t
    left join (select author_id, avatar_url, video_count
                 from (select author_id,
                              avatar_url,
                              count(1) over(partition by author_id) video_count,
                              row_number() over(partition by author_id order by record_time desc) orderSeq
                         from bigdata.douyin_advert_content_snapshot
                        where dt = '${dayBeforeYesterday}') t
                where t.orderSeq = 1) t1
      on t.user_id = t1.author_id;
"

executeHiveCommand "${COMMON_VAR}${hive_sql1}"