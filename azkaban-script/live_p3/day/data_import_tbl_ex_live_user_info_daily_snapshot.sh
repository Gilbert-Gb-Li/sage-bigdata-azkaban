#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p3_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

echo "############ 全量主播用户详情快照表  start   #########"

hive_sql="insert into live_p3.tbl_ex_live_user_info_daily_snapshot partition(dt='${date}')
  SELECT if(a.data_generate_time is not null and a.data_generate_time!=-1,a.data_generate_time,b.data_generate_time) as data_generate_time,
         if(a.appPackageName is not null and a.appPackageName!='',a.appPackageName,b.appPackageName) as appPackageName,
         if(a.dataSource is not null and a.dataSource!='',a.dataSource,b.dataSource) as dataSource,
         if(a.room_id is not null and a.room_id!='',a.room_id,b.room_id) as room_id,
         if(a.user_id is not null and a.user_id!='',a.user_id,b.user_id) as user_id,
         if(a.user_name is not null and a.user_name!='',a.user_name,b.user_name) as user_name,
         if(a.user_age is not null,a.user_age,-1) as user_age,
         if(a.user_sex is not null and a.user_sex!='-1',a.user_sex,b.user_sex) as user_sex,
         if(a.user_level is not null and a.user_level !='-1',a.user_level,b.user_level) as user_level,
         if(a.vip_level is not null and a.vip_level!='',a.vip_level,'') as vip_level,
         a.user_family,
         a.user_sign,
         a.user_constellation,
         if(a.user_hometown is not null and a.user_hometown!='',a.user_hometown,b.user_hometown) as user_hometown,
         a.user_profession,
         if(a.live_desc is not null and a.live_desc!='',a.live_desc,b.live_desc) as live_desc,
         a.follow_num,
         a.fans_num,
         a.income,
         a.consume,
         a.location,
         a.join_time,
         a.start_time,
         a.is_live,
         a.guard_num,
         a.authentication,
         a.user_label_list,
         a.user_hobby_list,
         if(a.user_image is not null and a.user_image!='',a.user_image,b.user_image) as user_image,
         if(a.share_url is not null and a.share_url!='',a.share_url,b.share_url) as share_url
  FROM
  (
    select data_generate_time,appPackageName,dataSource,room_id,
           user_id,user_name,user_age,user_sex,user_level,vip_level,
           user_family,user_sign,user_constellation,user_hometown,
           user_profession,live_desc,follow_num,fans_num,income,
           consume,location,join_time,start_time,is_live,
           guard_num,authentication,user_label_list,user_hobby_list,user_image,share_url
    from(
        select *,row_number() over (partition by dataSource,appPackageName,user_id order by data_generate_time desc) as order_num
        from (
            select data_generate_time,appPackageName,dataSource,room_id,
                   user_id,user_name,user_age,user_sex,user_level,vip_level,
                   user_family,user_sign,user_constellation,user_hometown,
                   user_profession,live_desc,follow_num,fans_num,income,
                   consume,location,join_time,start_time,is_live,
                   guard_num,authentication,user_label_list,user_hobby_list,
                   null as user_image, null as share_url
            from ias_p3.tbl_ex_live_user_info_data_origin_orc
            where dt='${date}' and user_id is not null and user_id !=''
            union all
            select data_generate_time,appPackageName,dataSource,room_id,
                   user_id,user_name,user_age,user_sex,user_level,vip_level,
                   user_family,user_sign,user_constellation,user_hometown,
                   user_profession,live_desc,follow_num,fans_num,income,
                   consume,location,join_time,start_time,is_live,
                   guard_num,authentication,user_label_list,user_hobby_list,
                   user_image,share_url
            from live_p3.tbl_ex_live_user_info_daily_snapshot
            where dt='${yesterday}'
        )as p
    )as t  where t.order_num =1 and t.data_generate_time is not null and t.appPackageName is not null and t.dataSource is not null and t.user_id is not null
  ) a
  FULL JOIN
  (
    select data_generate_time,appPackageName,dataSource,room_id,user_id,
           user_name,live_desc,user_image,user_level,share_url,user_sex,
           user_hometown
    from(
        select *,row_number() over (partition by dataSource,appPackageName,user_id order by data_generate_time desc) as order_num
        from (
            select data_generate_time,appPackageName,dataSource,room_id,
                   user_id,user_name,live_desc,user_image,user_level,share_url,
                   user_sex,user_hometown
            from ias_p3.tbl_ex_live_id_list_data_origin_orc
            where dt='${date}' and user_id is not null and user_id !=''
        )as p
    )as t
    where t.order_num =1 and t.data_generate_time is not null and t.appPackageName is not null and t.dataSource is not null and t.user_id is not null
  ) b
  ON a.user_id=b.user_id and a.appPackageName=b.appPackageName and a.dataSource=b.dataSource

  ;
"

delete_hive_partition="
   ALTER TABLE live_p3.tbl_ex_live_user_info_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
  "

hdfs dfs -rmr /data/ias_p3/live/snapshot/tbl_ex_live_user_info_daily_snapshot/dt=${date}

executeHiveCommand "
                   ${delete_hive_partition}
                   ${hive_sql}"

echo "############ 全量主播用户详情快照表  end #########"