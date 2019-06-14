#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
date=$1
yesterday=`date -d "-0 day $date" +%Y-%m-%d`
preyesterday=`date -d "-1 day $date" +%Y-%m-%d`
weekday=`date -d "-6 day $date" +%Y-%m-%d`
month2day=`date -d "-29 day $date" +%Y-%m-%d`
echo ${yesterday}
echo ${preyesterday}
echo ${weekday}
echo ${month2day}
echo "=================================生成每天去重后的主播表==================================="
sql1="insert into table bigdata.copyrightcloud_secphoto_user_list_snapshot PARTITION(dt='${yesterday}')
    select
     b.appVersion ,
     b.appPackageName ,
     b.resourceKey ,
     b.ias_timestamp ,
     b.user_id ,
     b.user_avatar ,
     b.user_nickname,
     b.user_attention_num ,
     b.user_follower_num ,
     b.user_works_num
     from
     (select
     a.appVersion ,
     a.appPackageName ,
     a.resourceKey ,
     a.ias_timestamp ,
     a.user_id ,
     a.user_avatar ,
     a.user_nickname,
     a.user_attention_num ,
     a.user_follower_num ,
     a.user_works_num,
     row_number() over (partition by a.appPackageName,a.user_id order by a.ias_timestamp desc) as order_num
 from  bigdata.copyrightcloud_secphoto_user_list_origin a
where a.dt = '${yesterday}' and a.user_id is not null) b
where b.order_num=1;"
echo "=================================生成每天新增的主播表==================================="
sql2="insert into table bigdata.copyrightcloud_secphoto_new_user_list_snapshot PARTITION(dt='${yesterday}')
        select
     c1.appVersion ,
     c1.appPackageName ,
     c1.resourceKey ,
     c1.ias_timestamp ,
     c1.user_id ,
     c1.user_avatar ,
     c1.user_nickname,
     c1.user_attention_num ,
     c1.user_follower_num ,
     c1.user_works_num
        from
        (select * from bigdata.copyrightcloud_secphoto_user_list_snapshot where dt='${yesterday}'
        ) as c1
        left join (
        select appPackageName,user_id from bigdata.copyrightcloud_secphoto_user_list_snapshot where dt='${preyesterday}'
        ) as c2
        on c1.user_id = c2.user_id and c1.appPackageName = c2.appPackageName
        where  c2.user_id is null;"
echo "=================================生成每天全量主播表==================================="
sql3="insert into table bigdata.copyrightcloud_secphoto_all_user_list_snapshot PARTITION(dt='${yesterday}')
          select
              appVersion ,
			  appPackageName ,
			  resourceKey ,
			  ias_timestamp ,
			  user_id ,
			  user_avatar ,
			  user_nickname,
			  user_attention_num ,
			  user_follower_num ,
			  user_works_num
            from
            bigdata.copyrightcloud_secphoto_new_user_list_snapshot where dt='${yesterday}'
			union all
			select
              appVersion ,
			  appPackageName ,
			  resourceKey ,
			  ias_timestamp ,
			  user_id ,
			  user_avatar ,
			  user_nickname,
			  user_attention_num ,
			  user_follower_num ,
			  user_works_num
            from
            bigdata.copyrightcloud_secphoto_all_user_list_snapshot where dt='${preyesterday}';"
			

echo "=================================生成违规用户表==================================="
sql4="insert into table bigdata.copyrightcloud_secphoto_update_video_data_snapshot PARTITION(dt='${yesterday}')
		select b.appPackageName,b.user_id,b.manual_status,b.time_stamp
		from (
			select appPackageName,user_id,manual_status,time_stamp,row_number() over (partition by appPackageName,user_id order by time_stamp,manual_status desc ) as order_num from bigdata.copyrightcloud_secphoto_update_video_data_origin
			where dt = '${yesterday}') b where b.order_num=1;"

echo "=================================生成去重后的是否白名单表==================================="
sql5="insert into table bigdata.copyrightcloud_secphoto_update_user_data_snapshot PARTITION(dt='${yesterday}')
		select
		b.appPackageName,
		b.app_id,
		b.user_id,
		b.u_id,
		b.user_pure,
		b.time_stamp
     from
     (select
	    a.appPackageName,
		a.app_id,
		a.user_id,
		a.u_id,
		a.user_pure,
		a.time_stamp,
     row_number() over (partition by a.appPackageName,a.user_id order by a.time_stamp desc) as order_num
 from  bigdata.copyrightcloud_secphoto_update_user_data_origin a
where a.dt = '${yesterday}' and a.user_id is not null) b
where b.order_num=1;"

   echo "================================生成每天去重后的活跃主播表==================================="
    executeHiveCommand "${sql1}"
   echo "=================================生成每天新增的主播表==================================="
    executeHiveCommand "${sql2}"
   echo "=================================生成每天全量主播表=========================================="
    executeHiveCommand "${sql3}"
   echo "=================================生成违规用户表==================================="
    executeHiveCommand "${sql4}"
   echo "=================================生成去重后的是否白名单表==================================="
    executeHiveCommand "${sql5}"