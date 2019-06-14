#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo "############ 主播用户详情全量快照表  start   #########"

hive_sql="insert into live_p2.tbl_ex_live_user_info_daily_snapshot_new partition(dt='${date}')
  SELECT if(a.data_generate_time is not null,a.data_generate_time,b.data_generate_time) as data_generate_time,
         if(a.app_package_name is not null,a.app_package_name,b.app_package_name) as app_package_name,
         if(a.data_source is not null,a.data_source,b.data_source) as data_source,
         if(a.search_id is not null,a.search_id,b.search_id) as search_id,
         if(a.live_id is not null,a.live_id,b.live_id) as live_id,
         if(a.user_id is not null,a.user_id,b.user_id) as user_id,
         if(a.user_name is not null,a.user_name,b.user_name) as user_name,
         if(a.age is not null,a.age,-1) as age,
         if(a.gender is not null,a.gender,-1) as gender,
         if(a.user_level is not null and a.user_level !='-1',a.user_level,b.user_level) as user_level,
         if(a.vip_level is not null,a.vip_level,null) as vip_level,
         a.family,
         a.sign,
         a.constellation,
         a.hometown,
         a.occupation,
         if(a.room_id is not null,a.room_id,b.room_id) as room_id,
         if(a.live_desc is not null,a.live_desc,b.live_desc) as live_desc,
         a.follow_count,
         a.fans_count,
         a.income_app_coin,
         a.cost_app_coin,
         a.location,
         a.join_time,
         a.start_time,
         a.guardian_count,
         a.authentication,
         a.user_label,
         a.user_hobby,
         a.is_live,
         if(a.user_image is not null,a.user_image,b.user_image) as user_image,
         if(a.share_url is not null,a.share_url,b.share_url) as share_url
  FROM
  (
    select data_generate_time,app_package_name,data_source,search_id,live_id,user_id,user_name,age,gender,
           user_level,vip_level,family,sign,constellation,hometown,occupation,room_id,live_desc,follow_count,
           fans_count,income_app_coin,cost_app_coin,location,join_time,start_time,is_live,
           guardian_count,authentication,user_label,user_hobby,user_image,share_url
    from(
        select *,row_number() over (partition by data_source,app_package_name,user_id order by data_generate_time desc) as order_num
        from (
            select data_generate_time,app_package_name,'${ias_source}' as data_source,search_id,live_id,
                   user_id,user_name,age,sex as gender,cast(user_level as string) as user_level,cast(vip_level as string) as vip_level,
                   family,sign,constellation,hometown,occupation,room_id,live_desc,follow_count,fans_count,income as income_app_coin,
                   cost as cost_app_coin,location,join_time,start_time,cast(is_live as string) as is_live,
                   guard_num as guardian_count,authentication,user_label,user_hobby,null as user_image, null as share_url
            from ias_p2.tbl_ex_live_user_info_data_origin_orc
            where dt='${date}' and search_id is not null and user_id is not null
            union all
            select data_generate_time,app_package_name,data_source,search_id,live_id,user_id,user_name,age,gender,
                   user_level,vip_level,family,sign,constellation,hometown,occupation,room_id,live_desc,follow_count,
                   fans_count,income_app_coin,cost_app_coin,location,join_time,start_time,is_live,
                   guardian_count,authentication,user_label,user_hobby,user_image,share_url
            from live_p2.tbl_ex_live_user_info_daily_snapshot_new
            where dt='${yesterday}'
        )as p
    )as t  where t.order_num =1
  ) a
  FULL JOIN
  (
    select data_generate_time,app_package_name,data_source,search_id,live_id,user_id,
           user_name,live_desc,room_id,user_image,user_level,share_url
    from(
        select *,row_number() over (partition by data_source,app_package_name,user_id order by data_generate_time desc) as order_num
        from (
            select client_time as data_generate_time,app_package_name,'${ias_source}' as data_source,search_id,live_id,
                   user_id,user_name,live_desc,room_id,user_image,cast(user_level as string) as user_level,share_url
            from ias_p2.tbl_ex_live_id_list_data_origin_orc
            where dt='${date}' and search_id is not null and user_id is not null
        )as p
    )as t
    where t.order_num =1
  ) b
  ON a.user_id=b.user_id and a.app_package_name=b.app_package_name and a.data_source=b.data_source
"

executeHiveCommand "${hive_sql}"

echo "############ 主播用户详情全量快照表  end #########"