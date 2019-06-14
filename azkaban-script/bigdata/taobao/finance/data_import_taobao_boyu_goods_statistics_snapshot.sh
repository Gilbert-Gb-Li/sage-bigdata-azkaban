#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
echo "${date}"
stat_date=`date -d "$date" +%Y%m%d`
stat_month=`date -d "${date}" +%Y%m`
week=`date -d "${date}" +%w`
echo "${week}"
date_reduce_6=`date -d "-6 day $date" +%Y-%m-%d`

################################
#######计算每周统计指标数据#######
################################

hive_sql="
INSERT INTO table bigdata.taobao_boyu_goods_statistics_snapshot PARTITION(dt='${date}')
select
      MAX(total_shop_count) as total_shop_count,
      MAX(shop_count) as shop_count,
      MAX(new_shop_count) as new_shop_count,
      MAX(total_goods_count) as total_goods_count,
      MAX(goods_count) as goods_count,
      MAX(new_goods_count) as new_goods_count,
      MAX(goods_trade_amount_low) as goods_trade_amount_low,
      MAX(goods_trade_amount_high) as goods_trade_amount_high
from
(
    select
          count(distinct shop_id) as total_shop_count,
          0 as shop_count,
          0 as new_shop_count,
          0 as total_goods_count,
          0 as goods_count,
          0 as new_goods_count,
          0 as goods_trade_amount_low,
          0 as goods_trade_amount_high
    from bigdata.taobao_boyu_shop_all_snapshot
    where dt = '${date}'
    UNION ALL
    select
          0 as total_shop_count,
          count(distinct shop_id) as shop_count,
          0 as new_shop_count,
          0 as total_goods_count,
          0 as goods_count,
          0 as new_goods_count,
          0 as goods_trade_amount_low,
          0 as goods_trade_amount_high
    from bigdata.taobao_boyu_goods_origin
    where dt >= '${date_reduce_6}' and dt <= '${date}'
    UNION ALL
    select
          0 as total_shop_count,
          0 as shop_count,
          count(distinct shop_id) as new_shop_count,
          0 as total_goods_count,
          0 as goods_count,
          0 as new_goods_count,
          0 as goods_trade_amount_low,
          0 as goods_trade_amount_high
    from bigdata.taobao_boyu_shop_new_snapshot
    where dt >= '${date_reduce_6}' and dt <= '${date}'
    UNION ALL
    select
          0 as total_shop_count,
          0 as shop_count,
          0 as new_shop_count,
          count(distinct goods_id) as total_goods_count,
          0 as goods_count,
          0 as new_goods_count,
          0 as goods_trade_amount_low,
          0 as goods_trade_amount_high
    from bigdata.taobao_boyu_goods_all_snapshot
    where dt = '${date}'
    UNION ALL
    select
          0 as total_shop_count,
          0 as shop_count,
          0 as new_shop_count,
          0 as total_goods_count,
          count(distinct goods_id) as goods_count,
          0 as new_goods_count,
          0 as goods_trade_amount_low,
          0 as goods_trade_amount_high
    from bigdata.taobao_boyu_goods_active_snapshot
    where dt >= '${date_reduce_6}' and dt <= '${date}'
    UNION ALL
    select
          0 as total_shop_count,
          0 as shop_count,
          0 as new_shop_count,
          0 as total_goods_count,
          0 as goods_count,
          count(distinct goods_id) as new_goods_count,
          0 as goods_trade_amount_low,
          0 as goods_trade_amount_high
    from bigdata.taobao_boyu_goods_new_snapshot
    where dt >= '${date_reduce_6}' and dt <= '${date}'
) t
;"


hive_sql1="
add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
insert into bigdata.taobao_boyu_goods_statistics_es_data
select
      concat('${stat_date}','_','statistics'),
      '${stat_month}',
      unix_timestamp('${date}', 'yyyy-MM-dd')*1000,
      'taobao',
      'statistics',
      total_shop_count,
      shop_count,
      new_shop_count,
      total_goods_count,
      goods_count,
      new_goods_count,
      goods_trade_amount_low,
      goods_trade_amount_high
from bigdata.taobao_boyu_goods_statistics_snapshot
where dt = '${date}'
;"

if [ ${week} -eq '0' ]
    then
      executeHiveCommand "${hive_sql}"
      executeHiveCommand "${hive_sql1}"
fi