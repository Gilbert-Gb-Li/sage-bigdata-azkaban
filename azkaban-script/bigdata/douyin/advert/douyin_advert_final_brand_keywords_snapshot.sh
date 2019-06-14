#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

today=$1
yesterday=`date -d "-1 day $today" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive_sql1="INSERT INTO TABLE bigdata.douyin_advert_final_brand_keywords_snapshot PARTITION(dt='${yesterday}')
select a.brand_id,concat('[',a.brand_path,']'),
case when b.category_path like concat(a.category_path,'%') then b.category_id else a.category_id end category_id,
case when b.category_path like concat(a.category_path,'%') then concat('[',b.category_path,']') else concat('[',a.category_path,']') end category_path,a.keyword_id,a.keyword,a.type,a.resource_id,a.short_video_id,a.comment_id,a.kol_id,a.source_time,a.video_create_time,a.comment_create_time,a.keyword_num
from
    (select brand_id,concat_ws(',',brand_1,brand_2,brand_3,brand_4,brand_5,brand_6,brand_7,brand_8,brand_9,brand_10) as brand_path,
category_id,concat_ws(',',pl_1,pl_2,pl_3,pl_4,pl_5,pl_6,pl_7,pl_8,pl_9,pl_10) as category_path,keyword_id,keyword,type,resource_id,
short_video_id,comment_id,kol_id,source_time,video_create_time,comment_create_time,keyword_num from
bigdata.douyin_advert_brand_keywords_snapshot where dt = '${yesterday}') a
left join
(select category_id,concat_ws(',',pl_1,pl_2,pl_3,pl_4,pl_5,pl_6,pl_7,pl_8,pl_9,pl_10) as category_path,keyword_id,keyword,type,resource_id from
bigdata.douyin_advert_categroy_keywords_snapshot where dt = '${yesterday}') b
on a.resource_id = b.resource_id and a.type = b.type;"

executeHiveCommand "${COMMON_VAR}${hive_sql1}"

