#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
stat_date=`date -d "$date" +%Y%m%d`
month1=`date -d "${date}" +%Y-%m`
month1_01=`date -d "${month1}-01" +%Y-%m-%d`
month1_01_1=`date -d "${month1}-01" +%Y%m%d`
month1_01_yesterday_1=`date -d "-1 day $month1_01" +%Y-%m-%d`
month2=`date -d "${month1_01_yesterday_1}" +%Y-%m`
month2_01=`date -d "${month2}-01" +%Y-%m-%d`
month2_01_1=`date -d "${month2}-01" +%Y%m%d`
year=`date -d "$date" +%Y`
month=`date -d "$date" +%m`
date_add_1=`date -d "+1 day $date" +%Y-%m-%d`
week=`date -d "${date_add_1}" +%w`
day=`date -d "${date_add_1}" +%d`

echo "date:${date}"
echo "yesterday:${yesterday}"
echo "stat_date:${stat_date}"
echo "month1:${month1}"
echo "month1_01:${month1_01}"
echo "month1_01_1:${month1_01_1}"
echo "month1_01_yesterday_1:${month1_01_yesterday_1}"
echo "month2:${month2}"
echo "month2_01:${month2_01}"
echo "month2_01_1:${month2_01_1}"
echo "year:${year}"
echo "month:${month}"

apk_name="com.smile.gifmaker"


    echo '################快手原始数据统计存储es####################'
    kuaishou_statistics_data_save_es="
    insert into table bigdata.kuaishou_statistics_es_data
    SELECT '${year}${month}' AS stat_month, '${date}' AS dt, key_word, meta_app_name, meta_table_name
        , app_package_name, origin_data_count, valid_origin_data_count, user_data_count, valid_user_data_count
        , user_commodity_data_count, valid_user_commodity_data_count, user_talk_data_count, valid_user_talk_data_count, video_data_count, valid_video_data_count
        , video_comment_data_count, valid_video_comment_data_count, music_data_count, valid_music_data_count, location_video_data_count
        , valid_location_video_data_count, live_user_data_count, valid_live_user_data_count, live_gift_data_count, valid_live_gift_data_count
        , live_end_data_count, valid_live_end_data_count, live_danmu_data_count, valid_live_danmu_data_count, valid_live_gift_danmu_data_count
        , challenge_video_data_count, valid_challenge_video_data_count
    FROM bigdata.kuaishou_statistics_data
    WHERE dt = '${date}';
    "
    echo "${kuaishou_statistics_data_save_es}"


    echo "##################### 导出头部集用户数据到es #############################"
    save_finance_kuaishou_user_t_data_to_es="
    insert into table bigdata.kuaishou_user_r_t_es_data
    select  set_type,concat('${stat_date}','_T_USER_',user_id) as key_word, '${year}${month}' as stat_month,dt,
        meta_app_name,meta_table_name,resource_key,app_version,app_package_name,data_generate_time,
        user_name,user_id,kwai_id,user_share_url,signature,store_or_curriculum,hometown,sex,constellation,certification,follower_count,new_follower_count,
        following_count,new_following_count,short_video_count,new_short_video_count,talk_count,new_talk_count,music_count,new_music_count,like_count,new_like_count,
        video_comment_count,new_video_comment_count,challenge_video_count,new_challenge_video_count,user_comment_count,new_user_comment_count,
        talk_comment_count,new_talk_comment_count,commodity_count,extract_date
    from bigdata.kuaishou_user_r_t_data
    where dt='${date}' and set_type='T_USER' and extract_date='${stat_date}' ;
    "
    echo "${save_finance_kuaishou_user_t_data_to_es}"
    echo "######################################################################"


    echo "##################### 导出当月抽样集用户数据到es #############################"
    save_finance_kuaishou_user_r_now_data_to_es="
    insert into table bigdata.kuaishou_user_r_t_es_data
    select  set_type,concat('${stat_date}','_R_USER_','${month1_01_1}_',user_id) as key_word, '${year}${month}' as stat_month,dt,
        meta_app_name,meta_table_name,resource_key,app_version,app_package_name,data_generate_time,
        user_name,user_id,kwai_id,user_share_url,signature,store_or_curriculum,hometown,sex,constellation,certification,follower_count,new_follower_count,
        following_count,new_following_count,short_video_count,new_short_video_count,talk_count,new_talk_count,music_count,new_music_count,like_count,new_like_count,
        video_comment_count,new_video_comment_count,challenge_video_count,new_challenge_video_count,user_comment_count,new_user_comment_count,
        talk_comment_count,new_talk_comment_count,commodity_count,extract_date
    from bigdata.kuaishou_user_r_t_data
    where dt='${date}' and set_type='R_USER' and extract_date='${month1_01_1}';
    "
    echo "${save_finance_kuaishou_user_r_now_data_to_es}"
    echo "######################################################################"


    echo "##################### 导出上月抽样集用户数据到es #############################"
    save_finance_kuaishou_user_r_old_data_to_es="
    insert into table bigdata.kuaishou_user_r_t_es_data
    select set_type,concat('${stat_date}','_R_USER_','${month2_01_1}_',user_id) as key_word, '${year}${month}' as stat_month,dt,
        meta_app_name, meta_table_name,resource_key,app_version,app_package_name,data_generate_time,
        user_name,user_id,kwai_id,user_share_url,signature,store_or_curriculum,hometown,sex,constellation,certification,follower_count,new_follower_count,
        following_count,new_following_count,short_video_count,new_short_video_count,talk_count,new_talk_count,music_count,new_music_count,like_count,new_like_count,
        video_comment_count,new_video_comment_count,challenge_video_count,new_challenge_video_count,user_comment_count,new_user_comment_count,
        talk_comment_count,new_talk_comment_count,commodity_count,extract_date
    from bigdata.kuaishou_user_r_t_data
    where dt='${date}' and set_type='R_USER' and extract_date='${month2_01_1}';
    "
    echo "${save_finance_kuaishou_user_r_old_data_to_es}"
    echo "######################################################################"


    echo "##################### 头部集视频数据导入ES #############################"
    save_finance_kuaishou_short_video_t_data_to_es="
    insert into table bigdata.kuaishou_short_video_r_t_es_data
    select set_type, concat('${stat_date}','_T_VIDEO_',video_id) as key_word, '${year}${month}' as stat_month,dt,
        meta_app_name, meta_table_name,resource_key,app_version,
        app_package_name,data_generate_time,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,bg_music_name,user_avatar_url,
        video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,
        bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info,video_comment_count,
        new_video_comment_count,video_like_count,new_video_like_count,video_play_count,new_video_play_count,challenge_from,extract_date
    from bigdata.kuaishou_short_video_r_t_data
    where dt='${date}' and set_type='T_VIDEO' and extract_date='${stat_date}';
    "
    echo "${save_finance_kuaishou_short_video_t_data_to_es}"
    echo "#####################################################################"


    echo "##################### 头部集视频数据导入ES #############################"
    save_finance_kuaishou_short_video_r_data_to_es="
    insert into table bigdata.kuaishou_short_video_r_t_es_data
    select set_type, concat('${stat_date}','_R_VIDEO_','${month1_01_1}_',video_id) as key_word, '${year}${month}' as stat_month,dt,
        meta_app_name,meta_table_name,resource_key,app_version,
        app_package_name,data_generate_time,video_create_time,video_id,video_caption,user_id,user_name,kwai_id,bg_music_name,user_avatar_url,
        video_magic_face_id,video_magic_face_name,video_location_city,video_location_address,video_location_id,video_shopping,video_advert,
        bg_musician_id,bg_musician_name,bg_musician_kwai_id,bg_music_type,bg_music_id,bg_music_upload_time,video_share_info,video_comment_count,
        new_video_comment_count,video_like_count,new_video_like_count,video_play_count,new_video_play_count,challenge_from,extract_date
    from bigdata.kuaishou_short_video_r_t_data
    where dt='${date}' and set_type='R_VIDEO' and extract_date='${month1_01_1}';
    "
    echo "${save_finance_kuaishou_short_video_r_data_to_es}"
    echo "#####################################################################"

    echo "##################### 头部集抽样集用户日留存导入ES #############################"
    tmp_kuaishou_r_t_user_day_remain_to_es="
    insert into bigdata.kuaishou_remain_r_t_es_data
    SELECT  concat('${stat_date}_',set_type,'_',remain_type,'_',extract_date) as key_word,
        '${date}' as dt, meta_app_name,meta_table_name,stat_month,remain_type,set_type,extract_date,
        comment_count_remain,comment_count_origin,video_count_remain,video_count_origin
    FROM bigdata.kuaishou_remain_r_t_data
    WHERE dt = '${date}'
        AND remain_type ='day';
    "
    echo "${tmp_kuaishou_r_t_user_day_remain_to_es}"
    echo "#####################################################################"

    echo "##################### 头部集抽样集用户周留存导入ES #############################"
    tmp_kuaishou_r_t_user_week_remain_to_es="
    insert into bigdata.kuaishou_remain_r_t_es_data
    SELECT  concat('${stat_date}_',set_type,'_',remain_type,'_',extract_date) as key_word,
        '${date}' as dt, meta_app_name,meta_table_name,stat_month,remain_type,set_type,extract_date,
        comment_count_remain,comment_count_origin,video_count_remain,video_count_origin
    FROM bigdata.kuaishou_remain_r_t_data
    WHERE dt = '${date}'
        AND remain_type ='week';
    "
    echo "#####################################################################"


    echo "##################### 头部集抽样集用户月留存导入ES #############################"
    tmp_kuaishou_r_t_user_month_remain_to_es="
    insert into bigdata.kuaishou_remain_r_t_es_data
    SELECT  concat('${stat_date}_',set_type,'_',remain_type,'_',extract_date) as key_word,
        '${date}' as dt, meta_app_name,meta_table_name,stat_month,remain_type,set_type,extract_date,
        comment_count_remain,comment_count_origin,video_count_remain,video_count_origin
    FROM bigdata.kuaishou_remain_r_t_data
    WHERE dt = '${date}'
        AND remain_type ='month';
    "
    echo "#####################################################################"


    echo "################### 直播用户信息存ES #############################"
    tmp_finance_kuaishou_like_user_all_data_to_es="
    insert into table bigdata.kuaishou_live_user_all_es_data
    select 'LIVE_USER' as set_type, concat('${stat_date}','_LIVE_USER_',user_id) as key_word,'${year}${month}' as stat_month,dt,
        meta_app_name,meta_table_name,
        app_package_name,user_name,user_id,kwai_id,user_share_url,signature,store_or_curriculum,hometown,sex,constellation,certification,
        follower_count,following_count,short_video_count,talk_count,music_count,
        gift_val,user_cost_gift_count,online_count_max,online_count_sum
    from  bigdata.kuaishou_live_user_all_data
    where dt='${date}';
    "
    echo "${tmp_finance_kuaishou_like_user_all_data_to_es}"
    echo "##########################################################"

    echo "################### 付费用户信息存ES #############################"
    tmp_finance_kuaishou_like_audience_all_data_to_es="
    insert into table bigdata.kuaishou_live_audience_all_es_data
    select 'LIVE_AUDIENCE' as set_type, concat('${stat_date}','_LIVE_AUDIENCE_',user_id,'_',receive_gift_user_id) as key_word, '${year}${month}' as stat_month,dt,
        meta_app_name, meta_table_name,
        app_package_name,user_id,kwai_id,receive_gift_user_id,receive_gift_kwai_id,receive_gift_val
    from bigdata.kuaishou_live_audience_all_data
    where dt='${date}';
    "
    echo "${tmp_finance_kuaishou_like_audience_all_data_to_es}"
    echo "##########################################################"


    echo '################快手平台指标数据存储es####################'
    kuaishou_platform_data_save_es="
    insert into table bigdata.kuaishou_platform_es_data
    SELECT '${year}${month}' AS stat_month, '${date}' AS dt, key_word, meta_app_name, meta_table_name
        , app_package_name, user_total_count, new_user_count, video_total_count, new_video_count
        , video_comment_total_count,new_video_comment_total_count, talk_total_count, talk_comment_total_count, music_total_count, challenge_total_count
        , live_active_user_count, live_gift_total_money, live_pay_user_count,t_user_count,new_t_user_count
    FROM bigdata.kuaishou_platform_data
    WHERE dt = '${date}';
    "

    echo "${kuaishou_platform_data_save_es}"
    echo "##########################################################"

    echo '################快手r t 统计数据存储es####################'
    kuaishou_user_r_t_statistics_es_data="
    insert into table bigdata.kuaishou_user_r_t_statistics_es_data
    SELECT set_type, concat(key_word,extract_date) as key_word,stat_month, '${date}' AS dt, meta_app_name, meta_table_name
        , app_package_name, extract_date,user_count,user_new_short_video_count,user_new_challenge_video_count,user_store_count,user_curriculum_count
        , user_commodity_count,user_new_talk_count,user_new_music_count,user_new_like_count,user_new_video_comment_count,user_new_user_comment_count
        , user_new_talk_comment_count,user_new_follower_count,user_new_following_count
        , proportion_new_short_video_count,proportion_new_challenge_video_count,proportion_store_count,proportion_curriculum_count,proportion_commodity_count
        , proportion_new_talk_count,proportion_new_music_count,proportion_new_like_count,proportion_new_video_comment_count,proportion_new_user_comment_count
        , proportion_new_talk_comment_count,proportion_new_follower_count,proportion_new_following_count
    FROM bigdata.kuaishou_user_r_t_statistics_data
    WHERE dt = '${date}';
    "
    echo "${kuaishou_user_r_t_statistics_es_data}"
    echo "##########################################################"

    executeHiveCommand "
    add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
    add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
    ${kuaishou_statistics_data_save_es}
    ${save_finance_kuaishou_user_t_data_to_es}
    ${save_finance_kuaishou_user_r_now_data_to_es}
    ${save_finance_kuaishou_user_r_old_data_to_es}
    ${save_finance_kuaishou_short_video_t_data_to_es}
    ${save_finance_kuaishou_short_video_r_data_to_es}
    ${tmp_kuaishou_r_t_user_day_remain_to_es}
    ${tmp_finance_kuaishou_like_user_all_data_to_es}
    ${tmp_finance_kuaishou_like_audience_all_data_to_es}
    ${kuaishou_platform_data_save_es}
    ${kuaishou_user_r_t_statistics_es_data}
    "

    if [ ${week} -eq '1' ]
        then
            echo "${tmp_kuaishou_r_t_user_week_remain_to_es}"
            executeHiveCommand "
            add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
            add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
            ${tmp_kuaishou_r_t_user_week_remain_to_es}
            "

        else
            echo "不是自然周的最后一天，不进行计算！"
    fi

    if [ ${day} -eq '01' ]
        then
            echo "${tmp_kuaishou_r_t_user_month_remain_to_es}"
            executeHiveCommand "
            add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
            add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
            ${tmp_kuaishou_r_t_user_month_remain_to_es}
            "

        else
            echo "不是自然月的最后一天，不进行计算！"
    fi