#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1

apk_name="com.smile.gifmaker"

    echo "#####################################################"
    kuaishou_user_data_origin_orc="insert into table bigdata.kuaishou_user_data_origin_orc PARTITION(dt='${date}')
    select
        cloud_service_id,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,times_tamp,hour,user_name,user_id,user_share_url,follower_count,following_count,signature,store_or_curriculum,curriculum,sex,constellation,certification,short_video_count,talk_count,music_count,label3,parent_resource_key,task_data_id,flow_id,source
    from bigdata.kuaishou_user_data_origin
    where dt='${date}' and app_package_name='${apk_name}' and schema='kuaishou_user_info' and user_id!='' and user_id is not null and (follower_count!=-1 or following_count!=-1 or short_video_count!=-1 or talk_count!=-1 or music_count!=-1);
    "
    echo "${kuaishou_user_data_origin_orc}"
    echo "#####################################################"

    kuaishou_talk_info_data_origin_orc="insert into bigdata.kuaishou_talk_info_data_origin_orc PARTITION(dt='${date}')
    select
        cloud_service_id,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,times_tamp,hour,talk_tag,talk_id,talk_content,comment_count,like_count,user_id,user_name,user_avatar_url,talk_publish_time,comment_user_id,comment_user_name,comment_id,comment_content,comment_talk_id,comment_publish_time,parent_resource_key,task_data_id,flow_id,source
    from bigdata.kuaishou_talk_info_data_origin
    where dt='${date}' and app_package_name='${apk_name}' and schema='short_talk_info' and talk_id!='' and talk_id is not null and comment_id!='' and comment_id is not null ;
    "
    echo "${kuaishou_talk_info_data_origin_orc}"
    echo "#####################################################"

    kuaishou_short_video_data_origin_orc="insert into bigdata.kuaishou_short_video_data_origin_orc PARTITION(dt='${date}')
    select
        cloud_service_id,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,times_tamp,hour,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,video_comment_count,video_like_count,video_play_count,bg_music_name,user_avatar_url,video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info,link_uid,parent_resource_key,task_data_id,flow_id,source
    from bigdata.kuaishou_short_video_data_origin
    where dt='${date}' and app_package_name='${apk_name}' and (schema='short_video_info' or schema='home_video_info') and video_id!='' and video_id is not null and (video_comment_count!=-1 or video_like_count!=-1 or video_play_count!=-1);
    "
    echo "${kuaishou_short_video_data_origin_orc}"
    echo "#####################################################"

    kuaishou_short_video_comment_data_origin_orc="insert into bigdata.kuaishou_short_video_comment_data_origin_orc PARTITION(dt='${date}')
    select
        cloud_service_id,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,times_tamp,hour,video_id,video_user_id,comment_id,comment_content,comment_time,comment_like_count,comment_user_name,comment_user_id,comment_user_kwaiid,comment_recall_count,parent_resource_key,task_data_id,flow_id,source
    from bigdata.kuaishou_short_video_comment_data_origin
    where dt='${date}' and app_package_name='${apk_name}' and schema='video_comment_list' and comment_id!='' and comment_id is not null and comment_content!='' and comment_content is not null ;
    "
    echo "${kuaishou_short_video_comment_data_origin_orc}"
    echo "#####################################################"

    kuaishou_music_data_origin_orc="insert into bigdata.kuaishou_music_data_origin_orc PARTITION(dt='${date}')
    select
        cloud_service_id,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,times_tamp,hour,music_id,music_name,music_use_count,musician_name,musician_id,musician_kwai_id,music_type,music_upload_time,link_uid,parent_resource_key,task_data_id,flow_id,source
    from bigdata.kuaishou_music_data_origin
    where dt='${date}' and app_package_name='${apk_name}' and schema='music_info' and music_id!='' and music_id is not null ;
    "
    echo "${kuaishou_music_data_origin_orc}"
    echo "#####################################################"

    kuaishou_location_video_info_data_origin_orc="insert into bigdata.kuaishou_location_video_info_data_origin_orc PARTITION(dt='${date}')
    select
        cloud_service_id,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,times_tamp,hour,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,video_comment_count,video_like_count,video_play_count,bg_music_name,user_avatar_url,video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info,address_name,address_count,location_id,parent_resource_key,task_data_id,flow_id,source
    from bigdata.kuaishou_location_video_info_data_origin
    where dt='${date}' and app_package_name='${apk_name}' and schema='location_video_info' and location_id!='' and location_id is not null and video_id!='' and video_id is not null and (video_comment_count!=-1 or video_like_count!=-1 or video_play_count!=-1);
    "
    echo "${kuaishou_location_video_info_data_origin_orc}"
    echo "#####################################################"

    kuaishou_live_gift_data_origin_orc="insert into bigdata.kuaishou_live_gift_data_origin_orc PARTITION(dt='${date}')
    select
        cloud_service_id,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,times_tamp,hour,room_id,user_id,user_name,gift_id,gift_name,gift_currency,gift_status,live_share_url,gift_unit_val,parent_resource_key,task_data_id,flow_id,source
    from bigdata.kuaishou_live_gift_data_origin
    where dt='${date}' and app_package_name='${apk_name}' and schema='live_gift_list' and user_id!='' and user_id is not null and gift_id!='' and gift_id is not null;
    "
    echo "${kuaishou_live_gift_data_origin_orc}"
    echo "#####################################################"

    kuaishou_live_end_data_origin_orc="insert into bigdata.kuaishou_live_end_data_origin_orc PARTITION(dt='${date}')
    select
        cloud_service_id,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,times_tamp,hour,room_id,user_id,user_name,audience_count,like_count,live_duration,parent_resource_key,task_data_id,flow_id,source
    from bigdata.kuaishou_live_end_data_origin
    where dt='${date}' and app_package_name='${apk_name}' and schema='live_end_info' and user_id!='' and user_id is not null and (audience_count!=-1 or like_count!=-1 or live_duration!=-1);
    "
    echo "${kuaishou_live_end_data_origin_orc}"
    echo "#####################################################"

    kuaishou_live_danmu_data_origin_orc="insert into bigdata.kuaishou_live_danmu_data_origin_orc PARTITION(dt='${date}')
    select
        cloud_service_id,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,times_tamp,hour,room_id,user_id,user_name,audience_id,audience_name,danmu_content,danmu_gift_id,danmu_gift_count,parent_resource_key,task_data_id,flow_id,source
    from bigdata.kuaishou_live_danmu_data_origin
    where dt='${date}' and app_package_name='${apk_name}' and schema='kuaishou_live_danmu' and user_id!='' and user_id is not null and ((audience_id!='' and audience_id is not null) or (audience_name!='' and audience_name is not null));
    "
    echo "${kuaishou_live_danmu_data_origin_orc}"
    echo "#####################################################"

    kuaishou_challenge_video_info_data_origin_orc="insert into bigdata.kuaishou_challenge_video_info_data_origin_orc PARTITION(dt='${date}')
    select
        cloud_service_id,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,times_tamp,hour,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,video_comment_count,video_like_count,video_play_count,bg_music_name,user_avatar_url,video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info,challenge_from,join_count,parent_resource_key,task_data_id,flow_id,source
    from bigdata.kuaishou_challenge_video_info_data_origin
    where dt='${date}' and app_package_name='${apk_name}' and schema='challenge_video_info' and challenge_from!='' and challenge_from is not null and video_id!='' and video_id is not null and (video_comment_count!=-1 or video_like_count!=-1 or video_play_count!=-1);
    "
    echo "${kuaishou_challenge_video_info_data_origin_orc}"
    echo "#####################################################"

    kuaishou_user_commodity_info_data_origin_orc="insert into bigdata.kuaishou_user_commodity_info_data_origin_orc PARTITION(dt='${date}')
    select
        cloud_service_id,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,times_tamp,meta_table_name,hour,user_id,commodity_name,commodity_price,commodity_source,commodity_sell_num,parent_resource_key,task_data_id,flow_id,source
    from bigdata.kuaishou_user_commodity_info_data_origin
    where dt='${date}' and app_package_name='${apk_name}' and meta_table_name='commodity_info' and user_id!='' and user_id is not null and commodity_name!='' and commodity_name is not null and commodity_price!=-1;
    "
    echo "${kuaishou_user_commodity_info_data_origin_orc}"
    echo "#####################################################"

    kuaishou_live_user_info_data_origin_orc="insert into bigdata.kuaishou_live_user_info_data_origin_orc PARTITION(dt='${date}')
    select
        cloud_service_id,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,times_tamp,hour,room_id,user_id,user_name,parent_resource_key,task_data_id,flow_id,source
    from bigdata.kuaishou_live_user_info_data_origin
    where dt='${date}' and app_package_name='${apk_name}' and schema='live_user_info' and room_id!='' and room_id is not null and user_id!='' and user_id is not null ;
    "
    echo "${kuaishou_live_user_info_data_origin_orc}"
    echo "#####################################################"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_user_data_origin_orc DROP IF EXISTS PARTITION (dt='${date}');
    ALTER TABLE bigdata.kuaishou_talk_info_data_origin_orc DROP IF EXISTS PARTITION (dt='${date}');
    ALTER TABLE bigdata.kuaishou_short_video_data_origin_orc DROP IF EXISTS PARTITION (dt='${date}');
    ALTER TABLE bigdata.kuaishou_short_video_comment_data_origin_orc DROP IF EXISTS PARTITION (dt='${date}');
    ALTER TABLE bigdata.kuaishou_music_data_origin_orc DROP IF EXISTS PARTITION (dt='${date}');
    ALTER TABLE bigdata.kuaishou_location_video_info_data_origin_orc DROP IF EXISTS PARTITION (dt='${date}');
    ALTER TABLE bigdata.kuaishou_live_gift_data_origin_orc DROP IF EXISTS PARTITION (dt='${date}');
    ALTER TABLE bigdata.kuaishou_live_end_data_origin_orc DROP IF EXISTS PARTITION (dt='${date}');
    ALTER TABLE bigdata.kuaishou_live_danmu_data_origin_orc DROP IF EXISTS PARTITION (dt='${date}');
    ALTER TABLE bigdata.kuaishou_challenge_video_info_data_origin_orc DROP IF EXISTS PARTITION (dt='${date}');
    ALTER TABLE bigdata.kuaishou_user_commodity_info_data_origin_orc DROP IF EXISTS PARTITION (dt='${date}');
    ALTER TABLE bigdata.kuaishou_live_user_info_data_origin_orc DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/orc/kuaishou_user_info_orc/dt=${date}
    hdfs dfs -rm -r /data/kuaishou/orc/short_talk_info_orc/dt=${date}
    hdfs dfs -rm -r /data/kuaishou/orc/short_video_info_orc/dt=${date}
    hdfs dfs -rm -r /data/kuaishou/orc/video_comment_list_orc/dt=${date}
    hdfs dfs -rm -r /data/kuaishou/orc/music_info_orc/dt=${date}
    hdfs dfs -rm -r /data/kuaishou/orc/location_video_info_orc/dt=${date}
    hdfs dfs -rm -r /data/kuaishou/orc/live_gift_list_orc/dt=${date}
    hdfs dfs -rm -r /data/kuaishou/orc/live_end_info_orc/dt=${date}
    hdfs dfs -rm -r /data/kuaishou/orc/kuaishou_live_danmu_orc/dt=${date}
    hdfs dfs -rm -r /data/kuaishou/orc/challenge_video_info_orc/dt=${date}
    hdfs dfs -rm -r /data/kuaishou/orc/commodity_info_orc/dt=${date}
    hdfs dfs -rm -r /data/kuaishou/orc/live_user_info_orc/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${kuaishou_user_data_origin_orc}
    ${kuaishou_talk_info_data_origin_orc}
    ${kuaishou_short_video_data_origin_orc}
    ${kuaishou_short_video_comment_data_origin_orc}
    ${kuaishou_music_data_origin_orc}
    ${kuaishou_location_video_info_data_origin_orc}
    ${kuaishou_live_gift_data_origin_orc}
    ${kuaishou_live_end_data_origin_orc}
    ${kuaishou_live_danmu_data_origin_orc}
    ${kuaishou_challenge_video_info_data_origin_orc}
    ${kuaishou_user_commodity_info_data_origin_orc}
    ${kuaishou_live_user_info_data_origin_orc}
    "
    echo "######################## origin 数据转存 orc 格式 end #############################"

