#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

apk_name="com.smile.gifmaker"

hive_sql="
    insert into bigdata.kuaishou_challenge_video_data_daily_snapshot partition(dt='${date}')

    SELECT data_generate_time,app_package_name,video_create_time,video_id,video_caption,user_id,user_name,
        kwai_id,video_comment_count,video_like_count,video_play_count,bg_music_name,user_avatar_url,video_magic_face_id,
        video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,
        bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info,
        challenge_from,join_count
    FROM (
        SELECT *, row_number() OVER (PARTITION BY video_id,challenge_from ORDER BY data_generate_time DESC) AS order_num
        FROM (
            SELECT data_generate_time,app_package_name,video_create_time,video_id,video_caption,user_id,user_name,
                kwai_id,video_comment_count,video_like_count,video_play_count,bg_music_name,user_avatar_url,video_magic_face_id,
                video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,
                bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info,
                challenge_from,join_count
            FROM bigdata.kuaishou_challenge_video_info_data_origin_orc
            WHERE dt = '${date}' and app_package_name='${apk_name}'
                  and video_id!='' and video_id is not null
                  and challenge_from!='' and challenge_from is not null
            UNION ALL
            SELECT data_generate_time,app_package_name,video_create_time,video_id,video_caption,user_id,user_name,
                kwai_id,video_comment_count,video_like_count,video_play_count,bg_music_name,user_avatar_url,video_magic_face_id,
                video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,
                bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info,
                challenge_from,join_count
            FROM bigdata.kuaishou_challenge_video_data_daily_snapshot
            WHERE dt = '${yesterday}'
        ) as p
    ) as t
    WHERE t.order_num = 1;
    "
    echo "${hive_sql}"
    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_challenge_video_data_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_challenge_video_data_daily_snapshot/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${hive_sql}
    "