#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
stat_date=`date -d "$date" +%Y%m%d`
stat_month=`date -d "${date}" +%Y%m`
week=`date -d "${date}" +%w`
date_reduce_6=`date -d "-6 day $date" +%Y-%m-%d`

################################
#######计算每周统计指标数据#######
################################

hive_sql="
INSERT INTO table bigdata.tmall_boyu_goods_brand_snapshot PARTITION(dt='${date}')
select
      brand,
      count(distinct goods_id) as goods_sum,
      count(distinct shop_id) as shop_sum,
      sum(goods_month_sale) as month_sale,
      0 as sale_sum1,
      0 as sale_sum2,
      0 as sale_avg1,
      0 as sale_avg2,
      0 as avg_price1,
      0 as avg_price2,
      sum(goods_comment_num) as total_comment
from
(
select
      *,
      ROW_NUMBER() OVER ( PARTITION BY goods_id ORDER BY data_generate_time DESC ) AS order_num
from bigdata.tmall_boyu_goods_origin
where dt >= '${date_reduce_6}' 
      and dt <= '${date}'
      and goods_type = '1'
      and goods_status = 'true'
) t
where t.order_num = 1
group by t.brand
;"



hive_sql1="
add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
insert into bigdata.tmall_boyu_goods_brand_es_data
select
      concat('${stat_date}','_','brand','_',brand),
      '${stat_month}',
      unix_timestamp('${date}', 'yyyy-MM-dd')*1000,
      'tmall',
      'brand',
      brand,
      goods_sum,
      shop_sum,
      month_sale,
      sale_sum1,
      sale_sum2,
      sale_avg1,
      sale_avg2,
      avg_price1,
      avg_price2,
      total_comment
from bigdata.tmall_boyu_goods_brand_snapshot
where dt = '${date}'
;"

if [ ${week} -eq '0' ]
    then
      executeHiveCommand "${hive_sql}"
      executeHiveCommand "${hive_sql1}"
fi