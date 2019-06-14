#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
date=$1
yesterday=`date -d "-0 day $date" +%Y-%m-%d`
preyesterday=`date -d "-1 day $date" +%Y-%m-%d`
weekday=`date -d "-6 day $date" +%Y-%m-%d`
month2day=`date -d "-29 day $date" +%Y-%m-%d`
echo ${yesterday}
echo ${preyesterday}
echo ${weekday}
echo ${month2day}




echo "=================================生成每天去重后的总量数据表==================================="

hive_sql="
INSERT INTO table bigdata.boyu_jd_goods_mix_snapshot PARTITION(dt='${date}')
SELECT
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
FROM (
       SELECT
         *,
         row_number()
         OVER ( PARTITION BY goods_id
           ORDER BY occur_timestamp DESC ) AS row_num
       FROM (
              SELECT *
              FROM bigdata.boyu_jd_goods_day_snapshot a
              WHERE a.dt = '${date}' AND a.goods_id IS NOT NULL

              UNION ALL

              SELECT *
              FROM bigdata.boyu_jd_goods_mix_snapshot b
              WHERE b.dt = '${preyesterday}' AND b.goods_id IS NOT NULL
            ) t2
     ) t1
WHERE t1.row_num = 1;
"





executeHiveCommand "${hive_sql}"