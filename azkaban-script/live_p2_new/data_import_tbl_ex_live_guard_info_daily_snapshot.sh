#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo "############ 守护全量快照表  start   #########"

hive_sql="insert into live_p2.tbl_ex_live_guard_info_daily_snapshot partition(dt='${date}')
    select data_generate_time,app_package_name,data_source,search_id,live_id,user_id,is_live,
           guarder_id,guarder_name,guarder_contribute
    from(
        select *,row_number() over (partition by data_source,app_package_name,guarder_id,user_id order by data_generate_time desc) as order_num
        from (
            select data_generate_time,app_package_name,data_source,search_id,live_id,user_id,is_live,
                   guarder_id,guarder_name,guarder_contribute
            from ias_p2.tbl_ex_live_guard_info_data_origin_orc
            where dt='${date}' and search_id is not null and user_id is not null and guarder_id is not null
            union all
            select data_generate_time,app_package_name,data_source,search_id,live_id,user_id,is_live,
                   guarder_id,guarder_name,guarder_contribute
            from live_p2.tbl_ex_live_guard_info_daily_snapshot
            where dt='${yesterday}'
        )as p
    )as t  where t.order_num =1
  "

executeHiveCommand "${hive_sql}"

echo "############ 守护全量快照表  end #########"