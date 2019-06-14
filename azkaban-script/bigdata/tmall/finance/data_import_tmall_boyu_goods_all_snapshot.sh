#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

#############################
#######计算全量商品数据#######
#############################

hive_sql="
INSERT INTO table bigdata.tmall_boyu_goods_all_snapshot PARTITION(dt='${date}')
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
from
(
    select
          *,
          ROW_NUMBER() OVER ( PARTITION BY goods_id ORDER BY data_generate_time DESC ) AS order_num
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
        from bigdata.tmall_boyu_goods_active_snapshot
        where dt = '${date}'
        
        UNION ALL
        
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
    ) t
) s
where s.order_num = 1
"

executeHiveCommand "${hive_sql}"