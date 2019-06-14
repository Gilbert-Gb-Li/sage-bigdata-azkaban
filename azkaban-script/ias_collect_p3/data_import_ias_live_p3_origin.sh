#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p3_util.sh

value1=$1
value2=$2
if [ $value2 == 23 ]; then
    day=`date -d "1 day $value1" +%Y-%m-%d`
    hour=`date -d "1 hour $value2" +"%H"`
else
    day=$value1
    hour=`date -d "1 hour $value2" +"%H"`
fi

echo ${day} ${hour}

echo "##############  hive表加载数据 start ##################"

    tbl_ex_live_id_list_data_origin=" alter table ias_p3.tbl_ex_live_id_list_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_id_list/$day/$hour'; "

    echo "##############添加分区   ${tbl_ex_live_id_list_data_origin}    ###########"

    tbl_ex_live_danmu_data_origin=" alter table ias_p3.tbl_ex_live_danmu_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_danmu/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_danmu_data_origin}   ###########"

    tbl_ex_live_user_info_data_origin=" alter table ias_p3.tbl_ex_live_user_info_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_user_info/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_user_info_data_origin}   ###########"

    tbl_ex_live_stream_url_data_origin=" alter table ias_p3.tbl_ex_live_stream_url_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_stream_url/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_stream_url_data_origin}   ###########"

    tbl_ex_live_record_data_origin=" alter table ias_p3.tbl_ex_live_record_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_record/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_record_data_origin}   ###########"

    tbl_ex_live_record_audience_count_data_origin=" alter table ias_p3.tbl_ex_live_record_audience_count_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_record_audience_count/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_record_audience_count_data_origin}   ###########"

    tbl_ex_live_viewer_list_data_origin=" alter table ias_p3.tbl_ex_live_viewer_list_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_viewer_list/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_viewer_list_data_origin}   ###########"

    tbl_ex_live_weibo_url_data_origin=" alter table ias_p3.tbl_ex_live_weibo_url_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_weibo_url/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_weibo_url_data_origin}   ###########"

    tbl_ex_live_guard_list_data_origin=" alter table ias_p3.tbl_ex_live_guard_list_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_guard_list/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_guard_list_data_origin}   ###########"

    tbl_ex_live_gift_contributor_list_data_origin=" alter table ias_p3.tbl_ex_live_gift_contributor_list_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_gift_contributor_list/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_gift_contributor_list_data_origin}   ###########"

    tbl_ex_live_gift_info_data_origin=" alter table ias_p3.tbl_ex_live_gift_info_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_gift_info/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_gift_info_data_origin}   ###########"

    executeHiveCommand "${tbl_ex_live_id_list_data_origin} ${tbl_ex_live_danmu_data_origin} ${tbl_ex_live_user_info_data_origin} ${tbl_ex_live_stream_url_data_origin} ${tbl_ex_live_record_data_origin} ${tbl_ex_live_record_audience_count_data_origin} ${tbl_ex_live_viewer_list_data_origin} ${tbl_ex_live_weibo_url_data_origin} ${tbl_ex_live_guard_list_data_origin} ${tbl_ex_live_gift_contributor_list_data_origin} ${tbl_ex_live_gift_info_data_origin}"

echo "##############   hive表加载数据 end  ##################"


