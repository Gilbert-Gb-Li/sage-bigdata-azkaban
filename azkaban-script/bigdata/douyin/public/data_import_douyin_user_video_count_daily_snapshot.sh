#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="
insert into bigdata.douyin_user_video_count_daily_snapshot partition(dt='${date}')
select
      record_time,
      user_id,
      dynamic_count,
      short_video_count,
      like_video_count,
      nick_name
from
(
    select
          *,
          row_number() over (partition by user_id order by record_time desc) as order_num
    from
    (
        select
              record_time,
              user_id,
              dynamic_count,
              short_video_count,
              like_video_count,
              nick_name
        from
        (
            select
                  *,
                  row_number() over (partition by user_id order by record_time desc) as order_num
            from bigdata.douyin_user_video_count_data_origin_orc
            where dt = '${date}'
        ) t
        where t.order_num = 1
        UNION ALL
        select
              record_time,
              user_id,
              dynamic_count,
              short_video_count,
              like_video_count,
              nick_name
        from bigdata.douyin_user_video_count_daily_snapshot
        where dt = '${yesterday}'
    ) s
) a
where a.order_num = 1
;"

executeHiveCommand "${hive_sql}"