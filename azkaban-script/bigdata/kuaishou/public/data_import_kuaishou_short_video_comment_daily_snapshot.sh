#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

apk_name="com.smile.gifmaker"

hive_sql="
    insert into bigdata.kuaishou_short_video_comment_data_daily_snapshot partition(dt='${date}')
    SELECT data_generate_time, app_package_name, video_id, video_user_id, comment_id, comment_content
        , comment_time, comment_like_count, comment_user_name, comment_user_id, comment_user_kwaiid
        , comment_recall_count
    FROM (
        SELECT *, row_number() OVER (PARTITION BY comment_id ORDER BY data_generate_time DESC) AS order_num
        FROM (
            SELECT data_generate_time,app_package_name,video_id, video_user_id, comment_id, comment_content
                , comment_time, comment_like_count, comment_user_name, comment_user_id, comment_user_kwaiid
                , comment_recall_count
            FROM bigdata.kuaishou_short_video_comment_data_origin_orc
            WHERE dt = '${date}' and app_package_name='${apk_name}'
                  and comment_id!='' and comment_id is not null
            UNION ALL
            SELECT data_generate_time,app_package_name,video_id, video_user_id, comment_id, comment_content
                , comment_time, comment_like_count, comment_user_name, comment_user_id, comment_user_kwaiid
                , comment_recall_count
            FROM bigdata.kuaishou_short_video_comment_data_daily_snapshot
            WHERE dt = '${yesterday}'
        ) p
    ) t
    WHERE t.order_num = 1;
    "
    echo "${hive_sql}"
    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_short_video_comment_data_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_short_video_comment_data_daily_snapshot/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${hive_sql}
    "