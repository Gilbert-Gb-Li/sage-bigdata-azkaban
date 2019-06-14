#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo '############## 礼物信息全量快照 start  ###############'
hive_sql_2="insert into live_p2.tbl_ex_live_gift_info_daily_snapshot partition(dt='${date}')
    select  data_generate_time,app_package_name,data_source,protocol_version,spider_version,app_version,ias_client_hsn_id,
            template_version,crash_ip,normal_ip,task_create_time,task_status,
            search_id,live_id,user_id,gift_id,gift_name,gift_currency_type,gift_image,gift_gold,gift_unit_val
    from(
        select *,row_number() over (partition by data_source,app_package_name,user_id,gift_id,gift_name order by data_generate_time desc) as order_num
        from (
            select data_generate_time,app_package_name,'${ias_source}' as data_source,protocol_version,spider_version,app_version,ias_client_hsn_id,
                   template_version,crash_ip,normal_ip,task_create_time,task_status,
                   search_id,live_id,user_id,gift_id,gift_name,gift_currency_type,gift_image,gift_gold,gift_unit_val
            from ias_p2.tbl_ex_live_gift_info_data_origin
            where dt='${date}' and user_id is not null and gift_id is not null
            union all
            select data_generate_time,app_package_name,data_source,protocol_version,spider_version,app_version,ias_client_hsn_id,
                   template_version,crash_ip,normal_ip,task_create_time,task_status,
                   search_id,live_id,user_id,gift_id,gift_name,gift_currency_type,gift_image,gift_gold,gift_unit_val
            from live_p2.tbl_ex_live_gift_info_daily_snapshot
            where dt='${yesterday}'
        )as p
    )as t  where t.order_num =1 ;
  "
echo "${hive_sql_2}"

executeHiveCommand "${hive_sql_2}"

echo '############## 礼物信息全量快照 end  ###############'