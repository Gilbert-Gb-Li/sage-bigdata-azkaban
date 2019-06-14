#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

#############################
#######计算全量商铺数据#######
#############################

hive_sql="
INSERT INTO table bigdata.taobao_boyu_shop_all_snapshot PARTITION(dt='${date}')
select
     s.data_generate_time,
     s.shop_id,
     s.shop_name,
     s.shop_location
from
(
    select
          *,
          ROW_NUMBER() OVER ( PARTITION BY shop_id ORDER BY data_generate_time DESC ) AS order_num
    from
    (
        select
              data_generate_time,
              shop_id,
              shop_name,
              shop_location
        from
        (
            select
              data_generate_time,
              shop_id,
              shop_name,
              shop_location,
              ROW_NUMBER() OVER ( PARTITION BY shop_id ORDER BY data_generate_time DESC ) AS order_num
            from bigdata.taobao_boyu_goods_origin
            where dt = '${date}'   and isNotBrand=0 and isNotExit=0
        ) t
        where t.order_num = 1 
        
        UNION ALL
        
        select
              data_generate_time,
              shop_id,
              shop_name,
              shop_location
        from
        (
            select
              data_generate_time,
              shop_id,
              shop_name,
              shop_location,
              ROW_NUMBER() OVER ( PARTITION BY shop_id ORDER BY data_generate_time DESC ) AS order_num
            from bigdata.taobao_boyu_shop_all_snapshot
            where dt = '${yesterday}'
        ) t
        where t.order_num = 1
    ) t
) s
where s.order_num = 1;
"

executeHiveCommand "${hive_sql}"