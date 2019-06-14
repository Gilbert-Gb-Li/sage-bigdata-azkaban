#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

tmpDir=/tmp/douyin

date=$1
today=`date -d "1 day ${date}" +%Y-%m-%d`
day=`date -d "${today}" +%d`

hive_sql1="insert into bigdata.douyin_sampling_user_data_orc partition(dt='${today}')
select user_id,concat('com.ss.android.ugc.aweme',' ','USER_INFO',' ','com.ss.android.ugc.aweme.profile.ui.UserProfileActivity',' ',sha1(concat('com.ss.android.ugc.aweme','#_#','USER_INFO','#_#','com.ss.android.ugc.aweme.profile.ui.UserProfileActivity','#_#',user_id))) as resource_key,app_version,app_package_name
from (
  select user_id,resource_key,app_version,app_package_name,ROW_NUMBER() over(partition by rank order by luck) row_num
  from (
    select user_id,resource_key,app_version,app_package_name,ntile(10) over(order by follower_count desc) rank, rand() luck
    from bigdata.douyin_user_daily_snapshot
    where dt='${date}' and follower_count != -1 and user_id != '' and user_id is not null and app_version is not null and app_version != '' 
    and app_package_name is not null and app_package_name != ''
  ) a
) t
where row_num <= 10000;"

#导出抽样用户的resource_key到hdfs下，供爬虫二次爬取使用
hive_sql2="select '${date}' as stat_date,'R_USER' as type,app_version,app_package_name,resource_key from bigdata.douyin_sampling_user_data_orc where dt = '${today}'
union all
select '${date}' as stat_date,'R_VIDEO' as type,app_version,app_package_name,concat('com.ss.android.ugc.aweme',' ','USER_VIDEO_LIST',' ','com.ss.android.ugc.aweme.profile.ui.UserProfileActivity',' ',sha1(concat('com.ss.android.ugc.aweme','#_#','USER_VIDEO_LIST','#_#','com.ss.android.ugc.aweme.profile.ui.UserProfileActivity','#_#',user_id))) as resource_key from bigdata.douyin_sampling_user_data_orc where dt = '${today}'
;"

if [ ${day} -eq '01' ]
    then
      executeHiveCommand "${hive_sql1}"
      hive -e "${hive_sql2}" > ${tmpDir}/r.txt
      cat ${tmpDir}/r.txt >> ${tmpDir}/douyin_t_r_data.txt
	    file_md5=`md5sum ${tmpDir}/r.txt|cut -d ' ' -f1`
	    echo ${file_md5} > ${tmpDir}/r.md5
	    hadoop fs -put ${tmpDir}/r.* /data/export/douyin/${date}/
    else
      echo "不是自然月的最后一天，不进行计算！"
fi