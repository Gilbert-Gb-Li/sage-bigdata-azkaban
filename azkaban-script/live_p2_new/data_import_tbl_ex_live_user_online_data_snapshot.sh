#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo "############ 每日在线时长和直播次数 快照表  start   #########"

user_info_online_length="CREATE TEMPORARY TABLE default.user_info_online_length AS
        select a.app_package_name,a.user_id,a.order_num,a.data_generate_time,
             if((b.data_generate_time-a.data_generate_time)>${ingkee_not_online},(b.data_generate_time-a.data_generate_time),0) as time_length,
             if((b.data_generate_time-a.data_generate_time)>${ingkee_not_online},1,0) as online_num
        from
        (
        select p.app_package_name,p.user_id,p.data_generate_time,row_number() over (partition by p.app_package_name,p.user_id order by data_generate_time asc) as order_num
        from (
          select distinct app_package_name,user_id,data_generate_time from ias_p2.tbl_ex_live_user_info_data_origin_orc where dt='${date}' and is_live=1
        ) as p
        ) as a
        left join
        (
        select h.app_package_name,h.user_id,h.data_generate_time,h.order_num_2 from (
          select g.app_package_name,g.user_id,g.data_generate_time,(cast(g.order_num as int)-1) as order_num_2 from(
            select *,row_number() over (partition by p.app_package_name,p.user_id order by data_generate_time asc) as order_num
            from (
              select distinct app_package_name,user_id,data_generate_time from ias_p2.tbl_ex_live_user_info_data_origin_orc where dt='${date}' and is_live=1
            ) as p
          ) as g
        ) as h where h.order_num_2>0
        ) as b
        on a.app_package_name=b.app_package_name and a.user_id=b.user_id and a.order_num=b.order_num_2
        where b.app_package_name is null or a.order_num=1 or (b.data_generate_time-a.data_generate_time)>${ingkee_not_online} ; "


tbl_ex_live_user_online_data_snapshot="insert into live_p2.tbl_ex_live_user_online_data_snapshot partition(dt='${date}')
        select k.data_generate_time,k.app_package_name,'${ias_source}' as data_source,k.user_id,(k.online_length-t.not_online_length) as live_online_length,t.online_num as live_online_count
        from(
          select a.app_package_name,
                  a.user_id,
                  a.data_generate_time,
                  b.data_generate_time as data_generate_time_2,
                  (a.data_generate_time-b.data_generate_time) as online_length
          from(
            select p.app_package_name,p.user_id,p.data_generate_time,p.order_num
            from (
              select app_package_name,user_id,data_generate_time,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num from default.user_info_online_length
            ) as p where p.order_num=1
          ) as a
          left join
          (
            select p.app_package_name,p.user_id,p.data_generate_time,p.order_num
            from (
              select app_package_name,user_id,data_generate_time,row_number() over (partition by app_package_name,user_id order by data_generate_time asc) as order_num from default.user_info_online_length
            ) as p where p.order_num=1

          ) as b
          on a.app_package_name=b.app_package_name and a.user_id=b.user_id and a.order_num=b.order_num
        ) as k
        left join
        (
          select app_package_name,user_id,sum(time_length) as not_online_length,(sum(online_num)+1) as online_num from default.user_info_online_length group by app_package_name,user_id
        ) as t
        on k.app_package_name=t.app_package_name and k.user_id=t.user_id ; "


tbl_ex_live_user_online_data_daily_snapshot="insert into live_p2.tbl_ex_live_user_online_data_daily_snapshot partition(dt='${date}')
        select if(b.data_generate_time is not null,b.data_generate_time,a.data_generate_time) as data_generate_time,
               if(b.app_package_name is not null,b.app_package_name,a.app_package_name) as app_package_name,
               if(b.data_source is not null,b.data_source,a.data_source) as data_source,
               if(b.user_id is not null,b.user_id,a.user_id) as user_id,
               if(b.live_online_count >0 and a.live_day_count is null,1,
                 if(b.live_online_count >0 and a.live_day_count >0 ,a.live_day_count+1,a.live_day_count)
               ) as live_day_count,
               if(b.live_online_count >0 and a.live_online_count is null,b.live_online_count,
                 if(b.live_online_count >0 and a.live_online_count>0,b.live_online_count+a.live_online_count,a.live_online_count)
               ) as live_online_count,
               if(b.live_online_length>=0 and a.live_online_length is null,b.live_online_length,
                  if(b.live_online_length>=0 and a.live_online_length >=0,a.live_online_length+b.live_online_length,a.live_online_length)
               ) as live_online_length
        from
        (
        select data_generate_time,app_package_name,data_source,user_id,live_day_count,live_online_count,live_online_length from live_p2.tbl_ex_live_user_online_data_daily_snapshot where dt='${yesterday}'
        ) as a
        FULL JOIN
        (
        select data_generate_time,app_package_name,data_source,user_id,live_online_count,live_online_length from live_p2.tbl_ex_live_user_online_data_snapshot where dt='${date}'
        ) as b
        ON a.app_package_name=b.app_package_name and a.data_source=b.data_source and a.user_id=b.user_id ;"

executeHiveCommand "${user_info_online_length} ${tbl_ex_live_user_online_data_snapshot} ${tbl_ex_live_user_online_data_daily_snapshot}"

echo "############ 每日在线时长和直播次数  end #########"




