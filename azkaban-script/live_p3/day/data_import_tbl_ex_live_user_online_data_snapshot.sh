#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p3_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo "############ 主播在线时长和直播次数 快照表  start   #########"

echo "############ 计算主播在第一次开播和最后一次开播时间段内不直播时长和直播次数  #########"
user_info_online_length="CREATE TEMPORARY TABLE default.tmp_p3_user_info_online_length AS
        select a.appPackageName,a.user_id,a.dataSource,a.order_num,a.data_generate_time,b.data_generate_time as data_generate_time_2,
             if((b.data_generate_time-a.data_generate_time)>${ingkee_not_online},(b.data_generate_time-a.data_generate_time),0) as time_length,
             if((b.data_generate_time-a.data_generate_time)>${ingkee_not_online},1,0) as online_num
        from
        (
        select p.appPackageName,p.user_id,p.dataSource,p.data_generate_time,row_number() over (partition by p.appPackageName,p.user_id,p.dataSource order by data_generate_time asc) as order_num
        from (
          select distinct appPackageName,user_id,dataSource,data_generate_time from ias_p3.tbl_ex_live_id_list_data_origin_orc where dt='${date}' and user_id is not null and user_id!=''
        ) as p
        ) as a
        left join
        (
        select h.appPackageName,h.user_id,h.dataSource,h.data_generate_time,h.order_num_2 from (
          select g.appPackageName,g.user_id,g.dataSource,g.data_generate_time,(cast(g.order_num as int)-1) as order_num_2 from(
            select *,row_number() over (partition by p.appPackageName,p.user_id,p.dataSource order by data_generate_time asc) as order_num
            from (
              select distinct appPackageName,user_id,dataSource,data_generate_time from ias_p3.tbl_ex_live_id_list_data_origin_orc where dt='${date}' and user_id is not null and user_id!=''
            ) as p
          ) as g
        ) as h where h.order_num_2>0
        ) as b
        on a.appPackageName=b.appPackageName and a.user_id=b.user_id and a.order_num=b.order_num_2 and a.dataSource=b.dataSource
        where b.appPackageName is null or a.order_num=1 or (b.data_generate_time-a.data_generate_time)>${ingkee_not_online};"


echo "############ 计算主播每次开播的时间#########"
user_online_time="CREATE TEMPORARY TABLE default.tmp_p3_user_online_time AS
        SELECT a.appPackageName, a.dataSource, a.user_id, a.online_time
        FROM (
            SELECT appPackageName, user_id, dataSource
                , if(order_num = 1
                    AND online_num = 1, data_generate_time_2, if(order_num = 1
                    AND online_num = 0, data_generate_time, if(order_num > 1
                    AND online_num = 1, data_generate_time_2, 0))) AS online_time
            FROM DEFAULT.tmp_p3_user_info_online_length
            UNION ALL
            SELECT appPackageName, user_id, dataSource
                , if(order_num = 1
                    AND online_num = 1, data_generate_time, 0) AS online_time
            FROM DEFAULT.tmp_p3_user_info_online_length
        ) a
        WHERE a.online_time > 0;"


echo "############ 存储计算主播每次开播的时间#########"
save_to_hive_user_online_time="insert into live_p3.tbl_ex_live_user_online_time_daily_snapshot_of_id_list partition(dt='${date}')
        SELECT appPackageName, dataSource, user_id, online_time
        FROM default.tmp_p3_user_online_time;"


echo "############ 计算一天的总直播时长和总直播次数#########"
user_online_day_snapshot="CREATE TEMPORARY TABLE default.tmp_p3_user_online_day_snapshot AS
        select k.data_generate_time,k.appPackageName,k.dataSource,k.user_id,(k.online_length-t.not_online_length) as live_online_length_day,t.online_num as live_online_count_day
        from(
          select a.appPackageName,
                  a.user_id,
                  a.dataSource,
                  a.data_generate_time,
                  b.data_generate_time as data_generate_time_2,
                  (a.data_generate_time-b.data_generate_time) as online_length
          from(
            select p.appPackageName,p.user_id,p.dataSource,p.data_generate_time,p.order_num
            from (
              select appPackageName,user_id,dataSource,data_generate_time,row_number() over (partition by appPackageName,user_id,dataSource order by data_generate_time desc) as order_num from default.tmp_p3_user_info_online_length
            ) as p where p.order_num=1
          ) as a
          left join
          (
            select p.appPackageName,p.user_id,p.dataSource,p.data_generate_time,p.order_num
            from (
              select appPackageName,user_id,dataSource,data_generate_time,row_number() over (partition by appPackageName,user_id,dataSource order by data_generate_time asc) as order_num from default.tmp_p3_user_info_online_length
            ) as p where p.order_num=1

          ) as b
          on a.appPackageName=b.appPackageName and a.user_id=b.user_id and a.order_num=b.order_num and a.dataSource=b.dataSource
        ) as k
        left join
        (
          select appPackageName,user_id,dataSource,sum(time_length) as not_online_length,(sum(online_num)+1) as online_num from default.tmp_p3_user_info_online_length group by appPackageName,user_id,dataSource
        ) as t
        on k.appPackageName=t.appPackageName and k.user_id=t.user_id and k.dataSource=t.dataSource;"


echo "############ 计算主播历史累计开播天数和历史累计开播次数#########"
user_online_history_snapshot="CREATE TEMPORARY TABLE default.tmp_p3_user_online_history_snapshot AS
        select if(b.data_generate_time is not null and b.data_generate_time!=-1,b.data_generate_time,a.data_generate_time) as data_generate_time,
               if(b.appPackageName is not null and b.appPackageName!='',b.appPackageName,a.appPackageName) as appPackageName,
               if(b.dataSource is not null and b.dataSource!='',b.dataSource,a.dataSource) as dataSource,
               if(b.user_id is not null and b.user_id!='',b.user_id,a.user_id) as user_id,
               if(b.live_online_count_day >0 and a.live_day_count_history is null,1,
                 if(b.live_online_count_day >0 and a.live_day_count_history >0 ,a.live_day_count_history+1,a.live_day_count_history)
               ) as live_day_count_history,
               if(b.live_online_count_day >0 and a.live_online_count_history is null,b.live_online_count_day,
                 if(b.live_online_count_day >0 and a.live_online_count_history>0,b.live_online_count_day+a.live_online_count_history,a.live_online_count_history)
               ) as live_online_count_history,
               if(b.live_online_length_day>=0 and a.live_online_length_history is null,b.live_online_length_day,
                  if(b.live_online_length_day>=0 and a.live_online_length_history >=0,a.live_online_length_history+b.live_online_length_day,a.live_online_length_history)
               ) as live_online_length_history
        from
        (
        select data_generate_time,appPackageName,dataSource,user_id,live_day_count_history,live_online_count_history,live_online_length_history from live_p3.tbl_ex_live_user_online_daily_snapshot where dt='${yesterday}' and user_id is not null and user_id!=''
        ) as a
        FULL JOIN
        (
        select data_generate_time,appPackageName,dataSource,user_id,live_online_count_day,live_online_length_day from default.tmp_p3_user_online_day_snapshot
        ) as b
        ON a.appPackageName=b.appPackageName and a.dataSource=b.dataSource and a.user_id=b.user_id;"


user_online_snapshot="insert into live_p3.tbl_ex_live_user_online_daily_snapshot_of_id_list partition(dt='${date}')
        select coalesce(a.data_generate_time,b.data_generate_time) as data_generate_time,
               coalesce(a.appPackageName,b.appPackageName) as appPackageName,
               coalesce(a.dataSource,b.dataSource) as dataSource,
               coalesce(a.user_id,b.user_id) as user_id,
               a.live_online_length_day,
               a.live_online_count_day,
               b.live_day_count_history,
               b.live_online_count_history,
               live_online_length_history
        from default.tmp_p3_user_online_day_snapshot as a
        full join
            default.tmp_p3_user_online_history_snapshot as b
        on a.appPackageName=b.appPackageName and a.user_id=b.user_id and a.dataSource=b.dataSource;
        "

delete_hive_partition="
   ALTER TABLE live_p3.tbl_ex_live_user_online_daily_snapshot_of_id_list DROP IF EXISTS PARTITION (dt='${date}');
   ALTER TABLE live_p3.tbl_ex_live_user_online_time_daily_snapshot_of_id_list DROP IF EXISTS PARTITION (dt='${date}');
  "

hdfs dfs -rmr /data/ias_p3/live/snapshot/tbl_ex_live_user_online_time_daily_snapshot_of_id_list/dt=${date}
hdfs dfs -rmr /data/ias_p3/live/snapshot/tbl_ex_live_user_online_daily_snapshot_of_id_list/dt=${date}


executeHiveCommand "${delete_hive_partition}  ${user_info_online_length} ${user_online_time} ${save_to_hive_user_online_time} ${user_online_day_snapshot} ${user_online_history_snapshot} ${user_online_snapshot}"

echo "############ 主播在线时长和直播次数  end #########"




