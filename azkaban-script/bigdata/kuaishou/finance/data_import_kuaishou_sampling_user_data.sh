#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

tmpDir=/tmp/kuaishou

date=$1
today=`date -d "1 day ${date}" +%Y-%m-%d`
day=`date -d "${today}" +%d`


    echo "################ 导出抽样数据 ######################"
    hive_sql1="
    insert into bigdata.kuaishou_sampling_user_data_orc partition(dt='${today}')
    SELECT user_id,
        concat('com.smile.gifmaker',' ','USER_INFO',' ','com.yxcorp.gifshow.profile.activity.UserProfileActivity',' ',sha1(concat('com.smile.gifmaker','#_#','USER_INFO','#_#','com.yxcorp.gifshow.profile.activity.UserProfileActivity','#_#',user_id))) as resource_key,
        app_version,
        app_package_name
    FROM (
        SELECT user_id, resource_key, app_version, app_package_name, ROW_NUMBER() OVER (PARTITION BY rank ORDER BY luck) AS row_num
        FROM (
            SELECT user_id, resource_key, app_version, app_package_name
                , ntile(10) OVER (ORDER BY follower_count DESC) AS rank, rand() AS luck
            FROM bigdata.kuaishou_user_data_daily_snapshot
            WHERE (dt = '${date}'
                AND follower_count != -1
                AND user_id != ''
                AND user_id IS NOT NULL
                AND app_version IS NOT NULL
                AND app_version != ''
                AND app_package_name IS NOT NULL
                AND app_package_name != ''
                AND resource_key IS NOT NULL
                AND resource_key != ''
                AND data_generate_time >= 1554739200000 )
        ) a
    ) t
    WHERE row_num <= 10000;
    "
    echo "${hive_sql1}"
    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_sampling_user_data_orc DROP IF EXISTS PARTITION (dt='${today}');
    "
    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_sampling_user_data_orc/dt=${today}


    echo "#####   导出抽样用户的resource_key到hdfs下，供爬虫二次爬取使用 #####"
    hive_sql2="
    SELECT '${date}' AS stat_date, 'R_USER' AS type, app_version,app_package_name,resource_key
    FROM bigdata.kuaishou_sampling_user_data_orc
    WHERE dt = '${today}'
    union all
    select '${date}' as stat_date, 'R_VIDEO' as type,app_version,app_package_name,
        concat('com.smile.gifmaker',' ','USER_VIDEO_LIST',' ','com.yxcorp.gifshow.profile.activity.UserProfileActivity',' ',sha1(concat('com.smile.gifmaker','#_#','USER_VIDEO_LIST','#_#','com.yxcorp.gifshow.profile.activity.UserProfileActivity','#_#',user_id))) as resource_key
    from bigdata.kuaishou_sampling_user_data_orc where dt = '${today}';
    "
    echo "${hive_sql2}"

if [ ${day} -eq '01' ]
    then
        executeHiveCommand "${delete_hive_partitions} ${hive_sql1}"
        hive -e "${hive_sql2}" > ${tmpDir}/r.txt
        cat ${tmpDir}/r.txt >> ${tmpDir}/kuaishou_t_r_data.txt
#        file_md5=`md5sum ${tmpDir}/r.txt|cut -d ' ' -f1`
#        echo ${file_md5} > ${tmpDir}/r.md5
#        hadoop fs -put ${tmpDir}/r.* /data/export/kuaishou/${date}/
    else
      echo "不是自然月的最后一天，不进行计算！"
fi