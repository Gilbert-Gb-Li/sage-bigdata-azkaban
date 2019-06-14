#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p3_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo '############## 主播打赏观众全量快照 start ###############'

hive_sql="insert into live_p3.tbl_ex_live_user_audience_gift_daily_snapshot partition(dt='${date}')
    select  g.appPackageName,g.dataSource,g.user_id,g.audience_id,sum(g.gift_val) as gift_val
    from (
        select appPackageName,dataSource,if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id,audience_id,gift_val
        from (
            select appPackageName,dataSource,user_id,audience_id,if(gift_val is null,0,gift_val) as gift_val
            from live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
            where dt='${date}' and user_id is not null and user_id!='' and audience_id is not null and audience_id!='' and gift_val>0
            union all
            select appPackageName,dataSource,user_id,audience_id,gift_val
            from live_p3.tbl_ex_live_user_audience_gift_daily_snapshot
            where dt='${yesterday}'
        ) as p
    ) as g
    group by g.appPackageName,g.dataSource,g.user_id,g.audience_id;
    "


delete_hive_partition="
   ALTER TABLE live_p3.tbl_ex_live_user_audience_gift_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
  "

hdfs dfs -rmr /data/ias_p3/live/snapshot/tbl_ex_live_user_audience_gift_daily_snapshot/dt=${date}

executeHiveCommand "
                   ${delete_hive_partition}
                   ${hive_sql}"

echo '############## 打赏观众全量快照 end ################'
