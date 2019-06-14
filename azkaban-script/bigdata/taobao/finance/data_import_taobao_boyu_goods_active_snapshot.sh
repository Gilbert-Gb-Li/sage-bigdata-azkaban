#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
stat_date=`date -d "$date" +%Y%m%d`
stat_month=`date -d "${date}" +%Y%m`
week=`date -d "${date}" +%w`
date_reduce_6=`date -d "-6 day $date" +%Y-%m-%d`

#############################
#######计算每天商品数据#######
#############################

hive_sql="
INSERT INTO table bigdata.taobao_boyu_goods_active_snapshot PARTITION(dt='${date}')
select 
      data_generate_time,
      url,
      brand,
      goods_id,
      goods_name,
      tm_mark,
      goods_price1,
      goods_price2,
      goods_month_sale,
      goods_succ_sum,
      goods_address,
      goods_favorite_count,
      goods_comment_num,
      goods_status,
      shop_id,
      shop_name,
      shop_location
from
(
    select
          *,
          ROW_NUMBER() OVER ( PARTITION BY goods_id ORDER BY data_generate_time DESC ) AS order_num
    from bigdata.taobao_boyu_goods_origin
    where dt = '${date}' and isNotBrand=0 and isNotExit=0
) t
where t.order_num = 1;
"

executeHiveCommand "${hive_sql}"


hive_sql1="
add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
insert into bigdata.taobao_boyu_goods_active_es_data
select
      concat('${stat_date}','_','goods','_',goods_id),
      '${stat_month}',
      unix_timestamp('${date}', 'yyyy-MM-dd')*1000,
      'taobao',
      'goods',
      url,
      brand,
      goods_id,
      goods_name,
      tm_mark,
      goods_price1,
      goods_price2,
      goods_month_sale,
      goods_succ_sum,
      goods_address,
      goods_favorite_count,
      goods_comment_num,
      goods_status,
      shop_id,
      shop_name,
      shop_location
from
(
select
      *,
      ROW_NUMBER() OVER ( PARTITION BY goods_id ORDER BY data_generate_time DESC ) AS order_num
from bigdata.taobao_boyu_goods_origin
where dt >= '${date_reduce_6}' and dt <= '${date}'  and goods_id is not null and goods_id != ''  and isNotBrand=0 and isNotExit=0
) t
where t.order_num = 1;
"

if [ ${week} -eq '0' ]
    then
      executeHiveCommand "${hive_sql1}"
fi