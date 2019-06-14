#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p3_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo "############ 礼物全量快照表  start   #########"

hive_sql="insert into live_p3.tbl_ex_live_gift_data_daily_snapshot partition(dt='${date}')
    select
        dataSource,record_time,trace_id,schema,client_time,cloudServiceId,spiderVersion,appVersion,containerId,resourceKey,dataType,data_generate_time,appPackageName,room_id,user_id,gift_id,gift_name,gift_image,gift_gold_type,gift_gold,gift_unit_val,hour
    from(
        select *,row_number() over (partition by dataSource,appPackageName,user_id,gift_id,gift_name order by data_generate_time desc) as order_num
        from (
            select
                dataSource,record_time,trace_id,schema,client_time,cloudServiceId,spiderVersion,appVersion,containerId,resourceKey,dataType,data_generate_time,appPackageName,room_id,user_id,gift_id,gift_name,gift_image,gift_gold_type,gift_gold,gift_unit_val,hour
            from ias_p3.tbl_ex_live_gift_info_data_origin_orc
            where dt='${date}' and user_id is not null and user_id!='' and ((gift_id is not null and gift_id!='') or (gift_name is not null and gift_name!=''))
            union all
            select
                dataSource,record_time,trace_id,schema,client_time,cloudServiceId,spiderVersion,appVersion,containerId,resourceKey,dataType,data_generate_time,appPackageName,room_id,user_id,gift_id,gift_name,gift_image,gift_gold_type,gift_gold,gift_unit_val,hour
            from live_p3.tbl_ex_live_gift_data_daily_snapshot
            where dt='${yesterday}'
        )as p
    )as t
    where t.order_num =1;
  "

delete_hive_partition="
   ALTER TABLE live_p3.tbl_ex_live_gift_data_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
  "

hdfs dfs -rm -r /data/ias_p3/live/snapshot/tbl_ex_live_gift_data_daily_snapshot/dt=${date}

executeHiveCommand "
                   ${delete_hive_partition}
                   ${hive_sql}"

echo "############ 礼物全量快照表  end #########"