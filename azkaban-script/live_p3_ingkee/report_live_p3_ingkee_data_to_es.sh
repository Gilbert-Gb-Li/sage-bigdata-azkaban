#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p3_util.sh

day=$1
yesterday=`date -d "-1 day $day" +%Y-%m-%d`
stat_date=`date -d "$day" +%Y%m%d`
week=`date -d "-6 day $day" +%Y-%m-%d`
year=`date -d "$day" +%Y`
month=`date -d "$day" +%m`


tmp_dir=/tmp/ingkee


    echo '################映客原始数据统计存储es####################'
    ingkee_statistics_data_save_es="insert into table live_p3_finance.tbl_ex_ingkee_statistics_es_data
    SELECT '${year}${month}' AS stat_month, '${day}' AS dt, keyWord, meta_app_name, meta_table_name
        , appPackageName, origin_data_count, valid_origin_data_count, id_list_data_count, valid_id_list_data_count
        , live_danmu_data_count, valid_live_danmu_data_count, valid_gift_danmu_data_count, gift_contributor_data_count, valid_gift_contributor_data_count
        , gift_info_data_count, valid_gift_info_data_count, guard_data_count, valid_guard_data_count, stream_url_data_count
        , valid_stream_url_data_count, user_info_data_count, valid_user_info_data_count, live_viewer_data_count, valid_live_viewer_data_count
        , weibo_url_data_count, valid_weibo_url_data_count
    FROM live_p3_finance.tbl_ex_ingkee_statistics_data
    WHERE dt = '${day}';
    "
    echo "${ingkee_statistics_data_save_es}"

    echo '################主播统计信息合并存储es####################'
    ingkee_live_user_save_es="insert into table live_p3_finance.tbl_ex_live_user_es_data
    SELECT
         '${year}${month}' AS stat_month,dt,
         keyWord,
         'ingkee' as meta_app_name,
         'tbl_ex_live_user_h' as meta_table_name,
         appPackageName, user_id, follow_num, fans_num
        , guard_num, danmu_num_day, audience_interact_num_day, gift_income_money_day, audience_gift_num_day
        , danmu_gift_num_day, guard_num_day_new, guard_money_day, income_money_day, live_online_length_day
        , live_day_count_history, live_online_count_history, money_week, audience_cost_money_num_week, arpu_week
        , money_month, audience_cost_money_num_month, arpu_month, money_history, audience_cost_money_num_history
        , arpu_history
    FROM live_p3_finance.tbl_ex_live_user_h
    WHERE dt = '${day}';
    "
    echo ${ingkee_live_user_save_es}

    echo '################观众统计信息合并存储es####################'
    ingkee_audience_save_es="insert into table live_p3_finance.tbl_ex_live_audience_es_data
    SELECT
         '${year}${month}' AS stat_month,dt,
         keyWord
        ,'ingkee' as meta_app_name
        ,'tbl_ex_live_audience_h' as meta_table_name
        , appPackageName, user_id, gift_cost_money_day, user_income_money_num_daily
        , money_week, user_income_money_num_week, arpu_week, money_month, user_income_money_num_month
        , arpu_month, money_history, user_income_money_num_history, arpu_history
    FROM live_p3_finance.tbl_ex_live_audience_h
    WHERE dt = '${day}';
    "
    echo ${ingkee_audience_save_es}

    echo '################平台统计信息to es ####################'
    ingkee_platform_save_to_es="insert into table live_p3_finance.tbl_ex_live_platform_es_data
    SELECT '${year}${month}' AS stat_month,dt,
        keyWord
        ,'ingkee' as meta_app_name
        ,'tbl_ex_live_platform_h' as meta_table_name
        , appPackageName, active_user_num_day, active_user_num_week, active_user_num_month
        , gift_money_day, gift_money_week, gift_money_month, guard_money_day, guard_money_week
        , guard_money_month, user_cost_gift_num_day, user_cost_gift_num_week, user_cost_gift_num_month, user_cost_guarder_num_day
        , user_cost_guarder_num_week, user_cost_guarder_num_month, guard_count_day, guard_count_week, guard_count_month
        , user_cost_money_num_day, user_cost_money_num_week, user_cost_money_num_month, arpu_day, arpu_week
        , arpu_month, user_num_all, active_user_num_day_new, live_count_day, audience_cost_money_num_day_new
    FROM live_p3_finance.tbl_ex_live_platform_h
    WHERE dt = '${day}';
    "
    echo ${ingkee_platform_save_to_es}
    echo '################直播时间信息to es ####################'
    ingkee_interval_save_to_es="insert into table live_p3_finance.tbl_ex_live_user_interval_percent_es_data
    SELECT  stat_month ,dt ,keyWord ,meta_app_name ,meta_table_name ,appPackageName ,category ,num ,live_length_percent
    FROM live_p3_finance.tbl_ex_live_user_interval_percent_data
    WHERE dt = '${day}';
    "
    echo ${ingkee_interval_save_to_es}

    echo '################性别信息to es ####################'
    ingkee_gender_save_to_es="insert into table live_p3_finance.tbl_ex_live_user_gender_percent_es_data
    SELECT   set_type ,
        stat_month ,
        dt,
        keyWord ,
        meta_app_name ,
        meta_table_name ,
        appPackageName ,
        gender ,
        num ,
        gender_percent
    FROM live_p3_finance.tbl_ex_live_user_gender_percent_data
    WHERE dt = '${day}';
    "
    echo ${ingkee_gender_save_to_es}

    echo '################星座信息to es ####################'
    ingkee_constellation_save_to_es="insert into table live_p3_finance.tbl_ex_live_user_constellation_percent_es_data
    SELECT   set_type ,
        stat_month ,
        dt,
        keyWord ,
        meta_app_name ,
        meta_table_name ,
        appPackageName ,
        constellation ,
        num ,
        constellation_percent
    FROM live_p3_finance.tbl_ex_live_user_constellation_percent_data
    WHERE dt = '${day}';
    "
    echo ${ingkee_constellation_save_to_es}

    echo '################位置信息to es ####################'
    ingkee_hometown_save_to_es="insert into table live_p3_finance.tbl_ex_live_user_hometown_percent_es_data
    SELECT  set_type ,
        stat_month ,
        dt,
        keyWord ,
        meta_app_name ,
        meta_table_name ,
        appPackageName ,
        hometown ,
        num ,
        hometown_percent
    FROM live_p3_finance.tbl_ex_live_user_hometown_percent_data
    WHERE dt = '${day}';
    "
    echo ${ingkee_hometown_save_to_es}

   executeHiveCommand "
    add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
    add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
    ${ingkee_statistics_data_save_es}
    ${ingkee_platform_save_to_es}
    ${ingkee_interval_save_to_es}
    ${ingkee_gender_save_to_es}
    ${ingkee_constellation_save_to_es}
    ${ingkee_hometown_save_to_es}
    ${ingkee_live_user_save_es}
    ${ingkee_audience_save_es}
    "


