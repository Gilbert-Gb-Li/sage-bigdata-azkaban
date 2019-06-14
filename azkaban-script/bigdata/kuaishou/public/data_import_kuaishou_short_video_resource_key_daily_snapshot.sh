#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

    hive_sql="INSERT INTO bigdata.kuaishou_short_video_resource_key_daily_snapshot partition(dt='${date}')
    SELECT data_generate_time, app_package_name, app_version, user_id, video_id
        , resource_key, resource_type
    FROM (
        SELECT *, row_number() OVER (PARTITION BY user_id, video_id, resource_key ORDER BY data_generate_time DESC) AS order_num
        FROM (
            SELECT data_generate_time, user_id, video_id, resource_key, split(resource_key,' ')[1] as resource_type
                , app_version, app_package_name
            FROM (
                SELECT *, row_number() OVER (PARTITION BY user_id, video_id ORDER BY data_generate_time DESC) AS order_num
                FROM (
                    SELECT data_generate_time, user_id, video_id, resource_key, app_version, app_package_name
                    FROM bigdata.kuaishou_short_video_data_origin_orc
                    WHERE dt = '${date}'
                        AND link_uid=user_id
                ) a
            ) s
            WHERE s.order_num = 1
            UNION ALL
            SELECT data_generate_time, user_id, video_id, resource_key,resource_type
                , app_version, app_package_name
            FROM bigdata.kuaishou_short_video_resource_key_daily_snapshot
            WHERE dt = '${yesterday}'
        ) t1
    ) t2
    WHERE t2.order_num = 1;
    "

    echo "${hive_sql}"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_short_video_resource_key_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_short_video_resource_key_daily_snapshot/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${hive_sql}"
    echo "####################### 快手全量用户信息快照 end  #################################"