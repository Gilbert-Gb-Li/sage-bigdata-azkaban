#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
stat_date=`date -d " $date" +%Y%m`

hive_sql="
add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
INSERT INTO bigdata.boyu_jd_goods_info_dst_es_snapshot
SELECT
  dt,
  '${stat_date}' as stat_date,
  'goods_info' as meta_table_name,
  concat('boyu_jd_goods_info_${date}_',indicator_id) as meta_id,
  indicator_id,
  indicator_name,
  indicator_value
FROM
  bigdata.boyu_jd_goods_stats_snapshot
WHERE
  dt = '${date}';
"

executeHiveCommand "${hive_sql}"