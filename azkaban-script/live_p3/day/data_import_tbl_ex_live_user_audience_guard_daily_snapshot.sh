#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p3_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo '#############################'


hive_sql="insert into live_p3.tbl_ex_live_user_audience_guard_daily_snapshot partition(dt='${date}')
    select  g.appPackageName,g.user_id,g.audience_id,sum(g.guard_count) as guard_count
    from (
        select appPackageName,if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id,audience_id,guard_count
        from (
            SELECT appPackageName, user_id, guarder_id as audience_id,1 as guard_count
            FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
            WHERE (guarder_id IS NOT NULL AND guarder_id!='' AND user_id !='' AND guarder_id!='@system_info'
                AND dt = '${date}'
                AND appPackageName IN ('com.meelive.ingkee'))
            GROUP BY dt, appPackageName, user_id,guarder_id
            union all
            select appPackageName,user_id,audience_id,guard_count
            from live_p3.tbl_ex_live_user_audience_guard_daily_snapshot
            where dt='${yesterday}'
        ) as p
    ) as g
    group by g.appPackageName,g.user_id,g.audience_id;
    "


delete_hive_partition="
   ALTER TABLE live_p3.tbl_ex_live_user_audience_guard_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
  "

hdfs dfs -rmr /data/ias_p3/live/snapshot/tbl_ex_live_user_audience_guard_daily_snapshot/dt=${date}

executeHiveCommand "
                   ${delete_hive_partition}
                   ${hive_sql}"

echo '##############################'
