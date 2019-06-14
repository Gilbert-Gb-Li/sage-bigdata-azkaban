#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
weekday=`date -d "-6 day $date" +%Y-%m-%d`

hive_sql="
use bigdata;

INSERT INTO bigdata.boyu_jd_goods_day_snapshot partition(dt='${date}')
select occur_timestamp,
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
from  (
        select *, row_number() over (partition by goods_id order by occur_timestamp desc) as row_num
        from  bigdata.boyu_jd_goods_origin
        where dt='${date}' and goods_id is not null and goods_id != '' and isNo=0
      ) t1
where t1.row_num =1;
"

executeHiveCommand "${hive_sql}"
