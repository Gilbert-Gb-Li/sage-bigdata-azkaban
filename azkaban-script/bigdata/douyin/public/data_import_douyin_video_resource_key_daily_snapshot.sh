#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="INSERT INTO bigdata.douyin_video_resource_key_daily_snapshot partition(dt='${date}')
SELECT record_time,author_id,short_video_id,resource_key,resource_type,app_version,app_package_name FROM
(SELECT *,row_number() over (partition by author_id,short_video_id,resource_key order by record_time desc) as order_num FROM
(SELECT record_time,author_id,short_video_id,resource_key,split(resource_key,' ')[1] as resource_type,app_version,app_package_name
FROM (SELECT *,row_number() over (partition by author_id,short_video_id,resource_key order by record_time desc) as order_num FROM (SELECT record_time, author_id, short_video_id, resource_key,app_version,app_package_name FROM bigdata.douyin_video_data_origin_orc WHERE dt = '${date}') a) s where s.order_num = 1
UNION ALL
SELECT record_time,author_id,short_video_id,resource_key,resource_type,app_version,app_package_name FROM bigdata.douyin_video_resource_key_daily_snapshot WHERE dt = '${yesterday}') t1) t2
WHERE t2.order_num = 1;"

executeHiveCommand "${hive_sql}"