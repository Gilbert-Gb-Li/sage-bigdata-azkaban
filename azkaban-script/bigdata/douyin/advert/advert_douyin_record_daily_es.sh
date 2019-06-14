#!/bin/sh
#source /etc/profile
#source ${AZKABAN_HOME}/conf/env.conf
#source ${base_path}/util.sh

today=$1
dayBeforeYesterday=`date -d "-2 day $today" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive -e "${COMMON_VAR}add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
use bigdata;
insert into bigdata.advert_douyin_user_count_record_es partition(dt='${dayBeforeYesterday}')
select '${dayBeforeYesterday}' AS stat_day, unix_timestamp( '${dayBeforeYesterday}', 'yyyy-MM-dd' )*1000,
dt,user_cn,valid_user_cn,add_valid_user_cn,vedio_cn,valid_vedio_cn,add_valid_vedio_cn,topic_cn,
valid_topic_cn,add_valid_topic_cn,comment_cn,valid_comment_cn,kol_cn,valid_kol_cn,add_valid_kol_cn,
kol_vedio_cn,kol_valid_vedio_cn,add_kol_valid_vedio_cn,kol_topic_cn,kol_valid_topic_cn,add_kol_valid_topic_cn,
kol_comment_cn,kol_valid_comment_cn
from bigdata.advert_douyin_count_result_daily_snapshot
where dt='${dayBeforeYesterday}';
"
echo "执行完成"
