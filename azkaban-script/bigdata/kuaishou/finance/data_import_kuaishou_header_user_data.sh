#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
tmpDir=/tmp/kuaishou

    echo "####################### 计算头部用户 #########################"
    hive_sql="
    insert into bigdata.kuaishou_header_user_data_orc partition(dt='${date}')
    SELECT user_id,
        concat('com.smile.gifmaker',' ','USER_INFO',' ','com.yxcorp.gifshow.profile.activity.UserProfileActivity',' ',sha1(concat('com.smile.gifmaker','#_#','USER_INFO','#_#','com.yxcorp.gifshow.profile.activity.UserProfileActivity','#_#',user_id))) as resource_key,
        app_version,
        app_package_name
    FROM bigdata.kuaishou_user_data_daily_snapshot
    WHERE (dt = '${date}'
        AND follower_count >= 8000
        AND user_id != ''
        AND user_id IS NOT NULL
        AND app_version IS NOT NULL
        AND app_version != ''
        AND app_package_name IS NOT NULL
        AND app_package_name != ''
        AND data_generate_time >= 1554739200000 );
    "

    echo "${hive_sql}"
    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_header_user_data_orc DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_header_user_data_orc/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${hive_sql}"



    echo "######  导出头部用户的resource_key到hdfs下，供爬虫二次爬取使用  ###"
    hive_sql2="
    SELECT '${date}' AS stat_date, 'T_USER' AS type, app_version,app_package_name,resource_key
    FROM bigdata.kuaishou_header_user_data_orc
    WHERE dt = '${date}'
    union all
    select '${date}' as stat_date,'T_VIDEO' as type,app_version,app_package_name,
        concat('com.smile.gifmaker',' ','USER_VIDEO_LIST',' ','com.yxcorp.gifshow.profile.activity.UserProfileActivity',' ',sha1(concat('com.smile.gifmaker','#_#','USER_VIDEO_LIST','#_#','com.yxcorp.gifshow.profile.activity.UserProfileActivity','#_#',user_id))) as resource_key
    from bigdata.kuaishou_header_user_data_orc where dt = '${date}';
    "

    echo "${hive_sql2}"
    hive -e "${hive_sql2}" > ${tmpDir}/t.txt

    cat ${tmpDir}/t.txt >> ${tmpDir}/kuaishou_t_r_data.txt

    #file_md5=`md5sum ${tmpDir}/t.txt|cut -d ' ' -f1`

    #echo ${file_md5} > ${tmpDir}/t.md5

    #hadoop fs -rmr /data/export/kuaishou/${date}
    #hadoop fs -mkdir /data/export/kuaishou/${date}
    #hadoop fs -put ${tmpDir}/t.* /data/export/kuaishou/${date}/