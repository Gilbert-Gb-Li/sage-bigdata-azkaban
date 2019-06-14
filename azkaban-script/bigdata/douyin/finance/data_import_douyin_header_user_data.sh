#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
tmpDir=/tmp/douyin

hive_sql="insert into bigdata.douyin_header_user_data_orc partition(dt='${date}')
select user_id,concat('com.ss.android.ugc.aweme',' ','USER_INFO',' ','com.ss.android.ugc.aweme.profile.ui.UserProfileActivity',' ',sha1(concat('com.ss.android.ugc.aweme','#_#','USER_INFO','#_#','com.ss.android.ugc.aweme.profile.ui.UserProfileActivity','#_#',user_id))) as resource_key,app_version,app_package_name from bigdata.douyin_user_daily_snapshot 
where dt = '${date}' and follower_count >= '8000' and user_id != '' and user_id is not null and
app_version is not null and app_version != '' and app_package_name is not null and app_package_name != '';"

executeHiveCommand "${hive_sql}"

#导出头部用户的resource_key到hdfs下，供爬虫二次爬取使用
hive_sql2="select '${date}' as stat_date,'T_USER' as type,app_version,app_package_name,resource_key from bigdata.douyin_header_user_data_orc where dt = '${date}'
union all
select '${date}' as stat_date,'T_VIDEO' as type,app_version,app_package_name,concat('com.ss.android.ugc.aweme',' ','USER_VIDEO_LIST',' ','com.ss.android.ugc.aweme.profile.ui.UserProfileActivity',' ',sha1(concat('com.ss.android.ugc.aweme','#_#','USER_VIDEO_LIST','#_#','com.ss.android.ugc.aweme.profile.ui.UserProfileActivity','#_#',user_id))) as resource_key from bigdata.douyin_header_user_data_orc where dt = '${date}'
;"

hive -e "${hive_sql2}" > ${tmpDir}/t.txt

cat ${tmpDir}/t.txt >> ${tmpDir}/douyin_t_r_data.txt

file_md5=`md5sum ${tmpDir}/t.txt|cut -d ' ' -f1`

echo ${file_md5} > ${tmpDir}/t.md5

hadoop fs -rmr /data/export/douyin/${date}
hadoop fs -mkdir /data/export/douyin/${date}
hadoop fs -put ${tmpDir}/t.* /data/export/douyin/${date}/