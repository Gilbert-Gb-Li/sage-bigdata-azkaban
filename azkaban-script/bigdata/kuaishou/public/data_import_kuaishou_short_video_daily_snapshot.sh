#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

apk_name="com.smile.gifmaker"


    echo "####################### 快手全量视频信息快照 start  #################################"

    echo "#####################  每日快手最新视频数据 ########################"
    tmp_kuaishou_video_new_data="
    CREATE TEMPORARY TABLE default.tmp_kuaishou_video_new_data AS
    select *
    from(
        select *,row_number() over (partition by app_package_name,video_id order by data_generate_time desc) as order_num
        from(
            select
                data_generate_time,app_package_name,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,video_comment_count,video_like_count,video_play_count,bg_music_name,user_avatar_url,video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info
            from(
                select *,row_number() over (partition by app_package_name,video_id order by data_generate_time desc) as order_num
                from bigdata.kuaishou_short_video_data_origin_orc
                where dt='${date}' and app_package_name='${apk_name}' and video_id is not null and video_id!=''
            ) as a
            where a.order_num=1

            UNION ALL
            select
                data_generate_time,app_package_name,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,video_comment_count,video_like_count,video_play_count,bg_music_name,user_avatar_url,video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info
            from(
                select *,row_number() over (partition by app_package_name,video_id order by data_generate_time desc) as order_num
                from bigdata.kuaishou_location_video_info_data_origin_orc
                where dt='${date}' and app_package_name='${apk_name}' and video_id is not null and video_id!=''
            ) as a
            where a.order_num=1

            UNION ALL
            select
                data_generate_time,app_package_name,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,video_comment_count,video_like_count,video_play_count,bg_music_name,user_avatar_url,video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info
            from(
                select *,row_number() over (partition by app_package_name,video_id order by data_generate_time desc) as order_num
                from bigdata.kuaishou_challenge_video_info_data_origin_orc
                where dt='${date}' and app_package_name='${apk_name}' and video_id is not null and video_id!=''
            ) as a
            where a.order_num=1
        ) as c
    ) as d
    where d.order_num=1;
    "
    echo "${tmp_kuaishou_video_new_data}"
    echo "###################################################################"


    hive_sql="insert into bigdata.kuaishou_short_video_data_daily_snapshot partition(dt='${date}')
    select d.data_generate_time,d.app_package_name,d.video_create_time,d.video_id,d.video_caption,d.user_id,d.user_name,d.kwai_id,d.video_comment_count,d.video_like_count,
        d.video_play_count,d.bg_music_name,d.user_avatar_url,d.video_magic_face_id,d.video_magic_face_name,d.video_location_city,d.video_location_address,d.video_location_id,
        d.video_shopping,d.video_advert,d.bg_musician_id,d.bg_musician_name,d.bg_musician_kwai_id,d.bg_music_type,d.bg_music_id,d.bg_music_upload_time,d.video_share_info
    from(

        select *,row_number() over (partition by app_package_name,video_id order by data_generate_time desc) as order_num
        from(
            select
                data_generate_time,app_package_name,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,video_comment_count,video_like_count,video_play_count,bg_music_name,user_avatar_url,video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info
            from default.tmp_kuaishou_video_new_data
            UNION ALL
            select
                data_generate_time,app_package_name,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,video_comment_count,video_like_count,video_play_count,bg_music_name,user_avatar_url,video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info
            from bigdata.kuaishou_short_video_data_daily_snapshot
            where dt='${yesterday}'
        ) as c
    ) as d
    where d.order_num=1;
    "
    echo "${hive_sql}"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_short_video_data_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_short_video_data_daily_snapshot/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${tmp_kuaishou_video_new_data}
    ${hive_sql}
    "
    echo "####################### 快手全量视频信息快照 end  #################################"
