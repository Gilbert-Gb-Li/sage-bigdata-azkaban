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
echo "=================================生成每天的主播状态表==================================="
sql1="insert into table bigdata.copyrightcloud_secphoto_user_daily_status_snapshot PARTITION(dt='${yesterday}')
select appPackageName,user_id,sum(new_user),sum(increase_user),sum(user_status),sum(user_pure) from (
select a.appPackageName,a.user_id,'0' as new_user,
case when a.user_follower_num-b.user_follower_num > 10000 then 1 when (a.user_follower_num-b.user_follower_num)/b.user_follower_num > 0.3 then 1 else 0 end as increase_user,
'0' as user_status,'0' as user_pure from 
(select appPackageName,user_id,user_follower_num from bigdata.copyrightcloud_secphoto_all_user_list_snapshot where dt = '${yesterday}') as a 
left join 
(select appPackageName,user_id,user_follower_num from bigdata.copyrightcloud_secphoto_all_user_list_snapshot where dt = '${preyesterday}') as b
on a.appPackageName = b.appPackageName and a.user_id=b.user_id
where a.user_follower_num > 1000
union all
select appPackageName,user_id,'0' as new_user,'0' as increase_user,case manual_status when 2 then 1 else 0 end as user_status,'0' as user_pure from  
bigdata.copyrightcloud_secphoto_update_video_data_snapshot where dt = '${yesterday}'
union all
select appPackageName,user_id ,'1' as new_user,'0' as increase_user,'0' as user_status,'0' as user_pure
from bigdata.copyrightcloud_secphoto_new_user_list_snapshot where dt = '${yesterday}'
union all
select appPackageName,user_id,'0' as new_user,'0' as increase_user,'0' as user_status,CAST(user_pure AS BIGINT) from 
bigdata.copyrightcloud_secphoto_update_user_data_snapshot from dt = '${yesterday}'
) as t group by appPackageName,user_id;"

echo "=================================生成全量主播 详情+状态 表==================================="
sql2="insert into table bigdata.copyrightcloud_secphoto_user_status_detail_snapshot PARTITION(dt='${yesterday}')
        select 
		a.appVersion,
		a.appPackageName,
		a.resourceKey,
		a.user_nickname,
		a.user_id,
		a.user_avatar,
		a.user_attention_num,
		a.user_follower_num,
		a.user_works_num,
		b.new_user,
		b.increase_user ,
		b.user_status ,
		b.user_pure
		from
		(select 
		appVersion,
		appPackageName,
		resourceKey,
		user_id,
		user_avatar,
		user_nickname,
		user_attention_num,
		user_follower_num,
		user_works_num
		from 
		bigdata.copyrightcloud_secphoto_all_user_list_snapshot  where dt='${yesterday}') a 
		left join 
		(select appPackageName,user_id,new_user,increase_user,user_status,user_pure from 
		bigdata.copyrightcloud_secphoto_user_daily_status_snapshot where dt='${yesterday}') b
		on a.user_id = b.user_id and a.appPackageName = b.appPackageName;"

sql3="select * from bigdata.copyrightcloud_secphoto_user_status_detail_snapshot where dt='${yesterday}';"

   echo "================================生成每天的主播状态表==================================="
    executeHiveCommand "${sql1}"
   echo "=================================生成全量主播 详情+状态 表==================================="
    executeHiveCommand "${sql2}"
   echo "================================= load user_detail_status data to mysql ==================================="
	hiveSqlToTakeoutMysql "${sql3}" "${yesterday}" "${mysql_table}" "appVersion,app_package_name,resourceKey,user_name,user_id,user_dp,user_follow_num,user_fans_num,user_video_num,new_user,increase_user,user_status,user_pure" "date"