#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo "############ 主播id 全量快照表  start   #########"

hive_sql="insert into live_p2.tbl_ex_live_id_daily_snapshot_new partition(dt='${date}')
select record_time,app_package_name,data_source,client_time,search_id,user_id,
       user_name,live_desc,room_id,current_page,user_image,user_level
from(
    select *,row_number() over (partition by data_source,app_package_name,search_id,user_id order by client_time desc) as order_num
    from (
        select record_time,app_package_name,'${ias_source}' as data_source,client_time,search_id,
               user_id,user_name,live_desc,room_id,current_page,user_image,user_level
        from ias_p2.tbl_ex_live_id_list_data_origin_orc
        where dt='${date}' and search_id is not null and user_id is not null
        union all
        select record_time,app_package_name,data_source,client_time,search_id,user_id,user_name,
               live_desc,room_id,current_page,user_image,user_level
        from live_p2.tbl_ex_live_id_daily_snapshot_new
        where dt='${yesterday}'
    )as p
)as t
where t.order_num =1"

executeHiveCommand "${hive_sql}"

echo "############ 主播id 全量快照表  end #########"