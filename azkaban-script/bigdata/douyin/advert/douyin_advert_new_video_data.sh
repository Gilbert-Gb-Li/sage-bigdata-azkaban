#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

today=$1
date=`date -d "-1 day $today" +%Y-%m-%d`
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive_sql="insert overwrite table bigdata.douyin_advert_new_video_data partition(dt='${date}')
select short_video_id,description,content,video_create_time,author_id,split(content_modle,':')[0] as content_brand_modle,split(content_modle,':')[1] as content_categroy_modle
,split(description_modle,':')[0] as description_brand_modle,split(description_modle,':')[1] as description_categroy_modle from
(select a.short_video_id,a.description,a.content,a.video_create_time,a.author_id,bigdata.word_split(a.content) as content_modle,bigdata.word_split(a.description) as description_modle from
(select short_video_id,description,video_create_time,author_id,content from bigdata.douyin_advert_content_snapshot where dt = '${date}') a
left join
(select short_video_id from bigdata.douyin_advert_content_snapshot where dt = '${yesterday}' group by short_video_id) b
on a.short_video_id = b.short_video_id
where b.short_video_id is null) t;"

executeHiveCommand "${COMMON_VAR}${hive_sql}"