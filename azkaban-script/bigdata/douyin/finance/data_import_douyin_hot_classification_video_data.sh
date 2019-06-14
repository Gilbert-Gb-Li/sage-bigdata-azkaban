#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
stat_date=`date -d "$yesterday" +%Y%m%d`

echo "++++++++++++++++++++++++++++++++计算生成热门分类数据中间表++++++++++++++++++++++++++++++++++++++"
hive_sql1="insert into bigdata.douyin_hot_classification_video_data partition(dt='${yesterday}')
select 'douyin' as meta_app_name,'hot_classification' as meta_table_name,hot_type,count(distinct short_video_id) as hot_video_count from bigdata.douyin_video_daily_snapshot where dt = '${yesterday}' and hot_type is not null group by hot_type;;"

executeHiveCommand "${hive_sql1}"

echo "++++++++++++++++++++++++++++++++导出热门分类数据到ES++++++++++++++++++++++++++++++++++++++"
hive_sql2="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;insert into bigdata.douyin_hot_classification_video_es_data partition(dt='${yesterday}')
select '${stat_date}',unix_timestamp(dt, 'yyyy-MM-dd')*1000,meta_app_name,meta_table_name,hot_type,video_count 
from bigdata.douyin_hot_classification_video_data where dt = '${yesterday}'"
executeHiveCommand "${hive_sql2}"