#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

apk_name="com.smile.gifmaker"

hive_sql="
    insert into bigdata.kuaishou_talk_comment_data_daily_snapshot partition(dt='${date}')
    SELECT data_generate_time,app_package_name,talk_tag,talk_id,talk_content,comment_count,like_count,
        user_id,user_name,user_avatar_url,talk_publish_time,comment_user_id,comment_user_name,comment_id,
        comment_content,comment_talk_id,comment_publish_time
    FROM (
        SELECT *, row_number() OVER (PARTITION BY talk_id,comment_id ORDER BY data_generate_time DESC) AS order_num
        FROM (
            SELECT data_generate_time,app_package_name,talk_tag,talk_id,talk_content,comment_count,like_count,
                user_id,user_name,user_avatar_url,talk_publish_time,comment_user_id,comment_user_name,comment_id,
                comment_content,comment_talk_id,comment_publish_time
            FROM bigdata.kuaishou_talk_info_data_origin_orc
            WHERE dt = '${date}' and app_package_name='${apk_name}'
                  and talk_id!='' and talk_id is not null
                  and comment_id!='' and comment_id is not null
            UNION ALL
            SELECT data_generate_time,app_package_name,talk_tag,talk_id,talk_content,comment_count,like_count,
                user_id,user_name,user_avatar_url,talk_publish_time,comment_user_id,comment_user_name,comment_id,
                comment_content,comment_talk_id,comment_publish_time
            FROM bigdata.kuaishou_talk_comment_data_daily_snapshot
            WHERE dt = '${yesterday}'
        ) p
    ) t
    WHERE t.order_num = 1;
    "
    echo "${hive_sql}"
    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_talk_comment_data_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_talk_comment_data_daily_snapshot/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${hive_sql}
    "
