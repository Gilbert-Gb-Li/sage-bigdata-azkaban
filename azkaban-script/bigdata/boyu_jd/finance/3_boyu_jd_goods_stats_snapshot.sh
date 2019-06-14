#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
monthAgo=`date -d "-30 day $date" +%Y-%m-%d`
esIndex=`date -d " $date" +%Y%m%d`



stat_date=`date -d "$date" +%Y%m%d`
stat_month=`date -d "${date}" +%Y%m`
week=`date -d "${date}" +%w`
date_reduce_6=`date -d "-6 day $date" +%Y-%m-%d`
date_reduce_7=`date -d "-7 day $date" +%Y-%m-%d`



hive_sql="
use bigdata;

insert into bigdata.boyu_jd_goods_stats_snapshot partition(dt='${date}')

select
  'jd001',
  '累计店铺数',
  count(distinct shop_id)
from bigdata.boyu_jd_goods_mix_snapshot where dt='${date}'

union all
select
  'jd002',
  '店铺数(当天)',
  count(distinct shop_id)
from bigdata.boyu_jd_goods_day_snapshot where dt='${date}'


union all

select 'jd003',
    '新增店铺数(当天)',
    count(distinct t1.shop_id)
    from
    (select * from bigdata.boyu_jd_goods_day_snapshot where dt='${date}') t1
     where t1.shop_id not in
    (select shop_id from bigdata.boyu_jd_goods_mix_snapshot  where dt = '${yesterday}')


union all

select
  'jd004',
  '累计商品数',
   count(distinct goods_id)
from bigdata.boyu_jd_goods_mix_snapshot
where dt='${date}' and goods_id is not null and goods_id != ''

union all

select
  'jd005',
  '商品数(当天)',
  count(distinct goods_id)
from bigdata.boyu_jd_goods_day_snapshot
where dt = '${date}' and goods_id is not null and goods_id != ''

union all

select 'jd006',
    '新增商品数',
    count(distinct t3.goods_id)
    from
    (select * from bigdata.boyu_jd_goods_day_snapshot where dt = '${date}') t3
     where t3.goods_id not in
    (select goods_id from bigdata.boyu_jd_goods_mix_snapshot  where dt = '${yesterday}')

union all

select
  'jd007',
  '自营商品数(累计)',
   count(distinct goods_id)
from bigdata.boyu_jd_goods_mix_snapshot
where dt='${date}' and goods_id is not null and goods_id != '' and goods_is_self = 1


union all

select
  'jd008',
  '京东超市商品数(累计)',
  count(distinct goods_id)
from bigdata.boyu_jd_goods_mix_snapshot
where dt='${date}'  and goods_id is not null and goods_id != '' and goods_is_market = 1;

"


hive_sql2="

use bigdata;

INSERT INTO bigdata.boyu_jd_goods_brand_snapshot partition(dt='${date}')

SELECT
  brand,
  count(goods_id)  AS goods_sum,
  count(shop_id)   AS shop_sum,
  avg(goods_price) AS avg_price,
  sum(goods_assess_num) AS assess_sum,
  sum(case goods_is_self when 1 then 1 else 0 end) AS goods_is_self_sum

FROM bigdata.boyu_jd_goods_day_snapshot
WHERE dt = '${date}' AND brand IS NOT NULL AND brand != ''

GROUP BY brand;
"
executeHiveCommand "${hive_sql}"
executeHiveCommand "${hive_sql2}"
