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
INSERT INTO bigdata.boyu_jd_goods_es_origin
SELECT
  dt,
  '${stat_date}' as stat_date,
  'goods_origin' as meta_table_name,
  occur_timestamp,
  occur_url,
  brand,
  goods_id,
  goods_name,
  goods_is_self,
  goods_is_market,
  goods_price,
  goods_sale,
  goods_assess_num,
  shop_id,
  shop_name,
  isNo,
  is_food
FROM
  bigdata.boyu_jd_goods_origin where dt ='${date}';
"

executeHiveCommand "${hive_sql}"
