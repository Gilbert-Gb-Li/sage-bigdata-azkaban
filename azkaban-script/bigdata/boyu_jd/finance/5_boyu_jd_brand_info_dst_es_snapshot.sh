#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
esIndex=`date -d " $date" +%Y%m`
stat_date=`date -d " $date" +%Y%m`

hive_sql="
use bigdata;

add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;

INSERT INTO bigdata.boyu_jd_brand_info_dst_es_snapshot
SELECT
  dt,
  '${stat_date}' as stat_date,
  'brand_info' as meta_table_name,
  concat('boyu_jd_goods_info_${date}_',brand) as meta_id,
  brand,
  goods_sum,
  shop_sum,
  price_avg,
  assess_num,
  goods_is_self_num

FROM


  bigdata.boyu_jd_goods_brand_snapshot


WHERE
  dt = '${date}';
"

executeHiveCommand "${hive_sql}"