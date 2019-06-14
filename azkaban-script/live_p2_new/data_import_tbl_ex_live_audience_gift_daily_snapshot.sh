#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo '############## 打赏观众全量快照  start ###############'


hive_sql_1="insert into live_p2.tbl_ex_live_audience_gift_daily_snapshot partition(dt='${date}')
    select data_generate_time,app_package_name,data_source,audience_id,audience_name
    from(
        select *,row_number() over (partition by data_source,app_package_name,audience_id order by data_generate_time desc) as order_num
        from (
            select data_generate_time,app_package_name,data_source,audience_id,audience_name
            from ias_p2.tbl_ex_live_gift_info_orc
            where dt='${date}' and audience_id is not null
            union all
            select data_generate_time,app_package_name,data_source,audience_id,audience_name
            from live_p2.tbl_ex_live_audience_gift_daily_snapshot
            where dt='${yesterday}'
        )as p
    )as t  where t.order_num =1 ;
  "
echo "${hive_sql_1}"

executeHiveCommand "${hive_sql_1}"


echo '############## 打赏观众全量快照  end ###############'