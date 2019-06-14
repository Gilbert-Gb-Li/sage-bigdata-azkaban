#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo "############ 主播用户详情全量快照表  start   #########"

hive_sql="insert into live_p2.tbl_ex_live_user_daily_snapshot_new partition(dt='${date}')
SELECT   if(a.record_time is not null,a.record_time,b.record_time) as record_time,
         if(a.app_package_name is not null,a.app_package_name,b.app_package_name) as app_package_name,
         if(a.data_source is not null,a.data_source,b.data_source) as data_source,
         if(a.data_generate_time is not null,a.data_generate_time,b.client_time) as data_generate_time,
         if(a.search_id is not null,a.search_id,b.search_id) as search_id,
         if(a.user_id is not null,a.user_id,b.user_id) as user_id,
         if(a.user_name is not null,a.user_name,b.user_name) as user_name,
         a.age,a.sex,
         if(a.user_level is null and b.user_level is null,-1,
            if(a.user_level is not null and a.user_level>0 and b.user_level is null,a.user_level,
              if(b.user_level is not null and b.user_level>0 and a.user_level is null,b.user_level,
                if(a.user_level is not null and b.user_level is not null and a.user_level>b.user_level,
                  a.user_level,
                  b.user_level
                )
              )
            )
         ) as user_level,
         a.vip_level,a.family,a.sign,a.constellation,a.hometown,a.occupation,a.room_id,
         if(a.live_desc is not null,a.live_desc,b.live_desc) as live_desc,
         a.follow_count,
         a.fans_count,a.income_app_coin,a.cost_app_coin,a.location,a.join_time,
         if(b.user_image is not null,b.user_image,'') as user_image,
         a.guard_num,a.authentication,a.user_label,a.user_hobby
  FROM
  (
    select record_time,app_package_name,data_source,data_generate_time,search_id,user_id,user_name,age,sex,
           user_level,vip_level,family,sign,constellation,hometown,occupation,room_id,live_desc,follow_count,
           fans_count,income_app_coin,cost_app_coin,location,join_time,
           guard_num,authentication,user_label,user_hobby
    from(
        select *,row_number() over (partition by data_source,app_package_name,search_id,user_id order by data_generate_time desc) as order_num
        from (
            select
                  record_time,app_id as app_package_name,'${ias_source}' as data_source,data_generate_time,search_id,
                  user_id,user_name,age,sex,user_level,vip_level,family,sign,constellation,hometown,occupation,room_id,
                  live_desc,follow_count,fans_count,income as income_app_coin,cost as cost_app_coin,location,join_time,
                  guard_num,authentication,user_label,user_hobby
            from ias_p2.tbl_ex_live_user_info_data_origin_orc
            where dt='${date}' and search_id is not null and user_id is not null
            union all
            select record_time,app_package_name,data_source,data_generate_time,search_id,user_id,user_name,age,sex,
                   user_level,vip_level,family,sign,constellation,hometown,occupation,room_id,live_desc,follow_count,
                   fans_count,income_app_coin,cost_app_coin,location,join_time,
                   guard_num,authentication,user_label,user_hobby
            from live_p2.tbl_ex_live_user_daily_snapshot_new
            where dt='${yesterday}'
        )as p
    )as t  where t.order_num =1
  ) a
  FULL JOIN
  (
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
   where t.order_num =1
  ) b
  ON a.search_id=b.search_id and a.user_id=b.user_id and a.app_package_name=b.app_package_name and a.data_source=b.data_source"

executeHiveCommand "${hive_sql}"

echo "############ 主播用户详情全量快照表  end #########"