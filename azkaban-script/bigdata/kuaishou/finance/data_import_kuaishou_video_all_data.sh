#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
stat_date=`date -d "$date" +%Y%m%d`
year=`date -d "$date" +%Y`
month=`date -d "$date" +%m`
echo "date:${date}"
echo "yesterday:${yesterday}"
echo "stat_date:${stat_date}"
echo "year:${year}"
echo "month:${month}"

apk_name="com.smile.gifmaker"


    echo "################### 获取短视频的话题信息 start ###################"
    tmp_finance_kuaishou_short_video_challenge_from="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_short_video_challenge_from AS
    select app_package_name,video_id,concat_ws('|', collect_set(regexp_replace(challenge_from,'\\|',''))) as challenge_from
    from bigdata.kuaishou_challenge_video_data_daily_snapshot
    where dt='${date}' and app_package_name='${apk_name}'
        and challenge_from is not null and challenge_from!=''
        and video_id is not null and video_id!=''
    group by app_package_name,video_id;
    "
    echo "${tmp_finance_kuaishou_short_video_challenge_from}"
    echo "#################################################################"

    echo "################### 全量视频数据整合 start ###################"
    tmp_finance_kuaishou_short_video_all_data="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_short_video_all_data AS
    select d.app_package_name,d.data_generate_time,d.video_create_time,d.video_id,d.video_caption,d.user_id,d.user_name,d.kwai_id,
        d.user_avatar_url,d.video_magic_face_id,d.video_magic_face_name,d.video_location_city,d.video_location_address,d.video_location_id,d.video_shopping,
        d.video_advert,d.bg_musician_id,d.bg_musician_name,d.bg_musician_kwai_id,d.bg_music_name,d.bg_music_type,d.bg_music_id,d.bg_music_upload_time,d.video_share_info,
        d.video_comment_count,d.new_video_comment_count,d.video_like_count,d.new_video_like_count,d.video_play_count,d.new_video_play_count,
        if(e.challenge_from is not null,e.challenge_from,'') as challenge_from
    from(
        select b.app_package_name,b.data_generate_time,b.video_create_time,b.video_id,b.video_caption,b.user_id,b.user_name,b.kwai_id,
            b.user_avatar_url,b.video_magic_face_id,b.video_magic_face_name,b.video_location_city,b.video_location_address,b.video_location_id,b.video_shopping,
            b.video_advert,b.bg_musician_id,b.bg_musician_name,b.bg_musician_kwai_id,b.bg_music_name,b.bg_music_type,b.bg_music_id,b.bg_music_upload_time,b.video_share_info,
            b.video_comment_count,
            if(c.video_comment_count is not null and c.video_comment_count>0,b.video_comment_count-c.video_comment_count,b.video_comment_count) as new_video_comment_count,
            b.video_like_count,
            if(c.video_like_count is not null and c.video_like_count>0,b.video_like_count-c.video_like_count,b.video_like_count) as new_video_like_count,
            b.video_play_count,
            if(c.video_play_count is not null and c.video_play_count>0,b.video_play_count-c.video_play_count,b.video_play_count) as new_video_play_count
        from(
            select app_package_name,data_generate_time,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,
                user_avatar_url,video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,
                video_advert,bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_name,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info,
                if(video_comment_count<0,0,video_comment_count) as video_comment_count,
                if(video_like_count<0,0,video_like_count) as video_like_count,
                if(video_play_count<0,0,video_play_count) as video_play_count
            from bigdata.kuaishou_short_video_data_daily_snapshot
            where dt='${date}' and app_package_name='${apk_name}'
                and video_id is not null and video_id!=''
        ) as b
        left join(
            select app_package_name,video_id,
                if(video_comment_count<0,0,video_comment_count) as video_comment_count,
                if(video_like_count<0,0,video_like_count) as video_like_count,
                if(video_play_count<0,0,video_play_count) as video_play_count
            from bigdata.kuaishou_short_video_data_daily_snapshot
            where dt='${year}-${month}-01' and app_package_name='${apk_name}'
                and video_id is not null and video_id!=''
        ) as c
        on b.app_package_name=c.app_package_name and b.video_id=c.video_id
    ) as d
    left join(
        select app_package_name,video_id,challenge_from from default.tmp_finance_kuaishou_short_video_challenge_from
    ) as e
    on d.app_package_name=e.app_package_name and d.video_id=e.video_id;
    "

    echo "${tmp_finance_kuaishou_short_video_all_data}"
    echo "#################################################################"

    echo "######################### 全量短视频导入hive ################################"
    save_finance_kuaishou_short_video_all_data_to_hive="
    insert into bigdata.kuaishou_short_video_all_data partition(dt='${date}')
    select 'kauishou' as  meta_app_name, 'kuaishou_short_video_all_data' as meta_table_name,'' as resource_key,'' as app_version,
        app_package_name,data_generate_time,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,bg_music_name,user_avatar_url,
        video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,
        bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info,video_comment_count,
        new_video_comment_count,video_like_count,new_video_like_count,video_play_count,new_video_play_count,challenge_from
    from default.tmp_finance_kuaishou_short_video_all_data;
    "
    echo "${save_finance_kuaishou_short_video_all_data_to_hive}"
    echo "########################################################################"


    echo "##################### 全量短视频导入ES #############################"
    save_finance_kuaishou_short_video_all_data_to_es="
    add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
    add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
    insert into table bigdata.kuaishou_short_video_all_es_data
    select  concat('VIDEO',video_id) as key_word, '${year}${month}' as stat_month,dt,
        meta_app_name,meta_table_name,resource_key,app_version,
        app_package_name,data_generate_time,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,bg_music_name,user_avatar_url,
        video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,
        bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info,video_comment_count,
        new_video_comment_count,video_like_count,new_video_like_count,video_play_count,new_video_play_count,challenge_from
    from bigdata.kuaishou_short_video_all_data
    where dt='${date}';
    "
    echo "${save_finance_kuaishou_short_video_all_data_to_es}"
    echo "#####################################################################"


    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_short_video_all_data DROP IF EXISTS PARTITION (dt='${date}');
    "
    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_short_video_all_data/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${tmp_finance_kuaishou_short_video_challenge_from}
    ${tmp_finance_kuaishou_short_video_all_data}
    ${save_finance_kuaishou_short_video_all_data_to_hive}
    ${save_finance_kuaishou_short_video_all_data_to_es}
    "
