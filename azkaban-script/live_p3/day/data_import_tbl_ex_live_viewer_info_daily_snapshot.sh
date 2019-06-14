#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p3_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo "############ 全量观众快照表  start   #########"

hive_sql="insert into live_p3.tbl_ex_live_viewer_info_daily_snapshot partition(dt='${date}')
    select data_generate_time,appPackageName,dataSource,
           audience_id,audience_name,audience_sex,audience_hometown,
           location,current_page
    from(
        select *,row_number() over (partition by dataSource,appPackageName,audience_id order by data_generate_time desc) as order_num
        from (
            select data_generate_time,appPackageName,dataSource,
                   audience_id,audience_name,audience_sex,audience_hometown,
                   location,current_page
            from ias_p3.tbl_ex_live_viewer_list_data_origin_orc
            where dt='${date}' and audience_id is not null and audience_id!=''
            union all
            select data_generate_time,appPackageName,dataSource,
                   audience_id,audience_name,audience_sex,audience_hometown,
                   location,current_page
            from live_p3.tbl_ex_live_viewer_info_daily_snapshot
            where dt='${yesterday}'
        )as p
    )as t  where t.order_num =1;
  "

delete_hive_partition="
   ALTER TABLE live_p3.tbl_ex_live_viewer_info_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
  "

hdfs dfs -rmr /data/ias_p3/live/snapshot/tbl_ex_live_viewer_info_daily_snapshot/dt=${date}

executeHiveCommand "
                   ${delete_hive_partition}
                   ${hive_sql}"

echo "############ 全量观众快照表  end #########"