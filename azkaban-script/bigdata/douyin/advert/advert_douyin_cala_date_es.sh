#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 抖音计算完成日期

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;"

es_sql="${es_sql}insert into bigdata.advert_douyin_cala_date_es select * from (select 'douyin','${dayBeforeYesterday}') t;"

executeHiveCommand "${COMMON_VAR}${es_sql}"

echo "advert douyin es finnish"