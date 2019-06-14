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
INSERT INTO table bigdata.taobao_boyu_shop_new_snapshot PARTITION(dt='${date}')
select
      a.data_generate_time,
      a.shop_id,
      a.shop_name,
      a.shop_location
from
(
    select
          data_generate_time,
          shop_id,
          shop_name,
          shop_location
    from bigdata.taobao_boyu_shop_all_snapshot
    where dt = '${date}'
) a
LEFT JOIN
(
select
      data_generate_time,
      shop_id,
      shop_name,
      shop_location
from bigdata.taobao_boyu_shop_all_snapshot
where dt = '${yesterday}'
) b
on a.shop_id = b.shop_id
where b.shop_id is null;
"

executeHiveCommand "${hive_sql}"