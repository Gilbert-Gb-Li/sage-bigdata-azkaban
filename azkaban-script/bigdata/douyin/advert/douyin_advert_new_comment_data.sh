#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

today=$1
date=`date -d "-1 day $today" +%Y-%m-%d`
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive_sql="insert overwrite table bigdata.douyin_advert_new_comment_data partition(dt='${date}')
select comment_id,created_time,short_video_id,user_id,comment,split(comment_modle,':')[0] as comment_brand_modle,split(comment_modle,':')[1] as comment_categroy_modle from
(select a.comment_id,a.created_time,a.short_video_id,a.user_id,a.comment,bigdata.word_split(a.comment) as comment_modle from
(select comment_id,created_time,short_video_id,user_id,comment from bigdata.douyin_video_comment_daily_snapshot where dt = '${date}') a
left join
(select comment_id from bigdata.douyin_video_comment_daily_snapshot where dt = '${yesterday}' group by comment_id) b
on a.comment_id = b.comment_id
where b.comment_id is null) t;"

executeHiveCommand "${COMMON_VAR}${hive_sql}"