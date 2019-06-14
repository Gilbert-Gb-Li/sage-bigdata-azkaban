#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

#############################
#######计算新增商品数据#######
#############################

hive_sql="
INSERT INTO table bigdata.tmall_boyu_goods_new_snapshot PARTITION(dt='${date}')
select
      a.data_generate_time,
      a.url,
      a.brand,
      a.goods_id,
      a.goods_name,
      a.tm_mark,
      a.goods_price1,
      a.goods_price2,
      a.goods_month_sale,
      a.goods_address,
      a.goods_favorite_count,
      a.goods_comment_num,
      a.goods_status,
      a.shop_id,
      a.shop_name,
      a.shop_location
from
(
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
          goods_address,
          goods_favorite_count,
          goods_comment_num,
          goods_status,
          shop_id,
          shop_name,
          shop_location
    from bigdata.tmall_boyu_goods_all_snapshot
    where dt = '${date}'
) a
LEFT JOIN
(
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
      goods_address,
      goods_favorite_count,
      goods_comment_num,
      goods_status,
      shop_id,
      shop_name,
      shop_location
from bigdata.tmall_boyu_goods_all_snapshot
where dt = '${yesterday}'
) b
on a.goods_id = b.goods_id
where b.goods_id is null;
"

executeHiveCommand "${hive_sql}"