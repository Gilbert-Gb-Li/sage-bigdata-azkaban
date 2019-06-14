#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p3_util.sh

day=$1

echo ${day}

echo "##############   导入 orc 表 开始   ##################"

    tbl_ex_live_id_list_data_origin_orc="INSERT INTO TABLE ias_p3.tbl_ex_live_id_list_data_origin_orc PARTITION(dt='${day}')
    SELECT dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id, user_id
        , user_name, live_desc, online_num, current_page, location
        , user_image, user_level, share_url, user_sex, user_hometown,hour
        , parent_resource_key,task_data_id,flow_id,source
    FROM ias_p3.tbl_ex_live_id_list_data_origin
    WHERE dt='${day}';"

    echo "${tbl_ex_live_id_list_data_origin_orc}"
    echo "#####################################"

    tbl_ex_live_danmu_data_origin_orc="INSERT INTO TABLE ias_p3.tbl_ex_live_danmu_data_origin_orc PARTITION(dt='${day}')
    SELECT dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id, user_id
        , audience_id, audience_name, gift_id, gift_type, gift_name
        , gift_image_url, gift_num, content, gift_unit_price, type
        , gift_type_id, gift_unit_val, gift_val,hour
        , parent_resource_key,task_data_id,flow_id,source
    FROM ias_p3.tbl_ex_live_danmu_data_origin
    WHERE dt='${day}';"

    echo "${tbl_ex_live_danmu_data_origin_orc}"
    echo "#####################################"

    tbl_ex_live_user_info_data_origin_orc="INSERT INTO TABLE ias_p3.tbl_ex_live_user_info_data_origin_orc PARTITION(dt='${day}')
    SELECT dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id, user_id
        , user_name, user_age, user_sex, user_level, vip_level
        , user_family, user_sign, user_constellation, user_hometown, user_profession
        , live_desc, online_num, follow_num, fans_num, income
        , consume, location, start_time, join_time, is_live
        , guard_num, authentication, user_label_list, user_hobby_list,hour
        , parent_resource_key,task_data_id,flow_id,source
    FROM ias_p3.tbl_ex_live_user_info_data_origin
    WHERE dt='${day}';"

    echo "${tbl_ex_live_user_info_data_origin_orc}"
    echo "#####################################"

    tbl_ex_live_stream_url_data_origin_orc="INSERT INTO TABLE ias_p3.tbl_ex_live_stream_url_data_origin_orc PARTITION(dt='${day}')
    SELECT dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id, user_id
        , stream_url,hour
        , parent_resource_key,task_data_id,flow_id,source
    FROM ias_p3.tbl_ex_live_stream_url_data_origin
    WHERE dt='${day}';"

    echo "${tbl_ex_live_stream_url_data_origin_orc}"
    echo "#####################################"

    tbl_ex_live_record_data_origin_orc="INSERT INTO TABLE ias_p3.tbl_ex_live_record_data_origin_orc PARTITION(dt='${day}')
    SELECT dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id, user_id
        , order_id, video_url, video_length, start_time, end_time
        , result_code,hour
        , parent_resource_key,task_data_id,flow_id,source
    FROM ias_p3.tbl_ex_live_record_data_origin
    WHERE dt='${day}';"

    echo "${tbl_ex_live_record_data_origin_orc}"
    echo "#####################################"

    tbl_ex_live_record_audience_count_data_origin_orc="INSERT INTO TABLE ias_p3.tbl_ex_live_record_audience_count_data_origin_orc PARTITION(dt='${day}')
    SELECT dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id, user_id
        , order_id, online_num, is_live, income,hour
        , parent_resource_key,task_data_id,flow_id,source
    FROM ias_p3.tbl_ex_live_record_audience_count_data_origin
    WHERE dt='${day}';"

    echo "${tbl_ex_live_record_audience_count_data_origin_orc}"
    echo "#####################################"

    tbl_ex_live_viewer_list_data_origin_orc="INSERT INTO TABLE ias_p3.tbl_ex_live_viewer_list_data_origin_orc PARTITION(dt='${day}')
    SELECT dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id, user_id
        , audience_id, audience_name, audience_sex, audience_hometown, online_num
        , location, current_page,hour
        , parent_resource_key,task_data_id,flow_id,source
    FROM ias_p3.tbl_ex_live_viewer_list_data_origin
    WHERE dt='${day}';"

    echo "${tbl_ex_live_viewer_list_data_origin_orc}"
    echo "#####################################"

    tbl_ex_live_weibo_url_data_origin_orc="INSERT INTO TABLE ias_p3.tbl_ex_live_weibo_url_data_origin_orc PARTITION(dt='${day}')
    SELECT dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id, user_id
        , weibo_url,hour
        , parent_resource_key,task_data_id,flow_id,source
    FROM ias_p3.tbl_ex_live_weibo_url_data_origin
    WHERE dt='${day}';"

    echo "${tbl_ex_live_weibo_url_data_origin_orc}"
    echo "#####################################"

    tbl_ex_live_guard_list_data_origin_orc="INSERT INTO TABLE ias_p3.tbl_ex_live_guard_list_data_origin_orc PARTITION(dt='${day}')
    SELECT dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id, user_id
        , is_live, guarder_id, guarder_name, guarder_contribute,hour
        , parent_resource_key,task_data_id,flow_id,source
    FROM ias_p3.tbl_ex_live_guard_list_data_origin
    WHERE dt='${day}';"

    echo "${tbl_ex_live_guard_list_data_origin_orc}"
    echo "#####################################"

    tbl_ex_live_gift_contributor_list_data_origin_orc="INSERT INTO TABLE ias_p3.tbl_ex_live_gift_contributor_list_data_origin_orc PARTITION(dt='${day}')
    SELECT dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id, user_id
        , contributor_user_id, contributor_user_name, contributor_user_sex, contributor_user_level, contributor_gift_num
        , contributor_location, is_live, contributor_current_page,hour
        , parent_resource_key,task_data_id,flow_id,source
    FROM ias_p3.tbl_ex_live_gift_contributor_list_data_origin
    WHERE dt='${day}';"

    echo "${tbl_ex_live_gift_contributor_list_data_origin_orc}"
    echo "#####################################"

    tbl_ex_live_gift_info_data_origin_orc="INSERT INTO TABLE ias_p3.tbl_ex_live_gift_info_data_origin_orc PARTITION(dt='${day}')
    SELECT dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id, user_id
        , gift_id, gift_name, gift_image, gift_gold_type, gift_gold
        , gift_unit_val,hour
        , parent_resource_key,task_data_id,flow_id,source
    FROM ias_p3.tbl_ex_live_gift_info_data_origin
    WHERE dt='${day}';"

    echo "${tbl_ex_live_gift_info_data_origin_orc}"
    echo "#####################################"

    delete_hive_partition="
    ALTER TABLE ias_p3.tbl_ex_live_danmu_data_origin_orc DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE ias_p3.tbl_ex_live_gift_contributor_list_data_origin_orc DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE ias_p3.tbl_ex_live_gift_info_data_origin_orc DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE ias_p3.tbl_ex_live_guard_list_data_origin_orc DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE ias_p3.tbl_ex_live_id_list_data_origin_orc DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE ias_p3.tbl_ex_live_record_audience_count_data_origin_orc DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE ias_p3.tbl_ex_live_record_data_origin_orc DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE ias_p3.tbl_ex_live_stream_url_data_origin_orc DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE ias_p3.tbl_ex_live_user_info_data_origin_orc DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE ias_p3.tbl_ex_live_viewer_list_data_origin_orc DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE ias_p3.tbl_ex_live_weibo_url_data_origin_orc DROP IF EXISTS PARTITION (dt='${day}');
    "

    hdfs dfs -rm -r /data/ias_p3/live/orc/live_danmu/dt=${day}
    hdfs dfs -rm -r /data/ias_p3/live/orc/live_gift_contributor_list/dt=${day}
    hdfs dfs -rm -r /data/ias_p3/live/orc/live_gift_info/dt=${day}
    hdfs dfs -rm -r /data/ias_p3/live/orc/live_guard_list/dt=${day}
    hdfs dfs -rm -r /data/ias_p3/live/orc/live_id_list/dt=${day}
    hdfs dfs -rm -r /data/ias_p3/live/orc/live_record_audience_count/dt=${day}
    hdfs dfs -rm -r /data/ias_p3/live/orc/live_record/dt=${day}
    hdfs dfs -rm -r /data/ias_p3/live/orc/live_stream_url/dt=${day}
    hdfs dfs -rm -r /data/ias_p3/live/orc/live_user_info/dt=${day}
    hdfs dfs -rm -r /data/ias_p3/live/orc/live_viewer_list/dt=${day}
    hdfs dfs -rm -r /data/ias_p3/live/orc/live_weibo_url/dt=${day}

    executeHiveCommand "
    ${delete_hive_partition}
    ${tbl_ex_live_id_list_data_origin_orc}
    ${tbl_ex_live_danmu_data_origin_orc}
    ${tbl_ex_live_user_info_data_origin_orc}
    ${tbl_ex_live_stream_url_data_origin_orc}
    ${tbl_ex_live_record_data_origin_orc}
    ${tbl_ex_live_record_audience_count_data_origin_orc}
    ${tbl_ex_live_viewer_list_data_origin_orc}
    ${tbl_ex_live_weibo_url_data_origin_orc}
    ${tbl_ex_live_guard_list_data_origin_orc}
    ${tbl_ex_live_gift_contributor_list_data_origin_orc}
    ${tbl_ex_live_gift_info_data_origin_orc}
    "

echo "##############   导入 orc表结束   ##################"

