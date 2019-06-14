#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

######################################
#######完成礼物数据的增量更新工作#######
#####################################

hive_sql="
insert into bigdata.huya_live_all_gift_snapshot partition(dt='${date}')
select
      data_generate_time,
      resource_key,
      spider_version,
      app_version,
      gift_id,
      gift_name,
      gift_gold,
      gift_icon,
      gift_md5 
from
(
    select *,row_number() over (partition by gift_md5 order by data_generate_time desc) as order_num from
    (
        select 
              data_generate_time,
              resource_key,
              spider_version,
              app_version,
              gift_id,
              gift_name,
              gift_gold,
              gift_icon,
              gift_md5
        from bigdata.huya_live_gift_origin_orc
        where dt= '${date}'
        
        union all
        
        select 
              data_generate_time,
              resource_key,
              spider_version,
              app_version,
              gift_id,
              gift_name,
              gift_gold,
              gift_icon,
              gift_md5
        from bigdata.huya_live_all_gift_snapshot
        where dt= '${yesterday}'
    ) t
) s
where s.order_num = 1;
"

executeHiveCommand "${hive_sql}"