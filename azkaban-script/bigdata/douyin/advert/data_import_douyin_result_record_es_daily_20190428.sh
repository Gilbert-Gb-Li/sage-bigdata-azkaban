#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
#source ${base_path}/util.sh

today=$1
yesterday=`date -d "-0 day $today" +%Y-%m-%d`
yesterday_format=`date -d "-0 day $today" +%Y%m%d`
hive -e "add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
insert into bigdata.advert_douyin_user_kol_result_record_import_es partition(dt='${yesterday}')
select '${yesterday_format}' AS stat_day, unix_timestamp( '${yesterday}', 'yyyy-MM-dd' ) * 1000,
dt,user_cn,valid_user_cn,day_valid_user_cn,vedio_cn,valid_vedio_cn,day_valid_vedio_cn,
topic_cn,valid_topic_cn,day_valid_topic_cn,comment_cn,valid_comment_cn,day_valid_comment_cn,
day_kol_cn,kol_cn,valid_kol_cn,day_add_valid_kol_cn,day_valid_kol_vedio_cn,valid_kol_vedio_cn,
valid_kol_valid_vedio_cn,day_add_valid_kol_valid_vedio_cn,valid_kol_topic_cn,
day_valid_kol_topic_cn,valid_kol_valid_topic_cn,day_add_valid_kol_valid_topic_cn,
valid_kol_comment_cn,day_valid__kol_comment_cn,valid_kol_valid_comment_cn,
day_add_valid_kol_valid_comment_cn
from bigdata.advert_douyin_record_count_result_daily_snapshot
where dt='${yesterday}';
"
echo "执行完成"
