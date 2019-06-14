#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

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

echo "##############   导入${all_live_app_list} origin 表 start ##################"

for app in ${all_live_app_list};
  do
    echo "##############   导入${app} origin表 start   ##################"

    tbl_ex_live_id_list_data_origin=" alter table ias_p2.tbl_ex_live_id_list_data_origin add if not exists partition (dt='$day',hour='$hour',app_id='$app') location '${p2_location_origin}/live_id_list/$day/$hour/$app'; "
    echo "##############添加分区   ${tbl_ex_live_id_list_data_origin}    ###########"

    tbl_ex_live_danmu_data_origin=" alter table ias_p2.tbl_ex_live_danmu_data_origin add if not exists partition (dt='$day',hour='$hour',app_id='$app') location '${p2_location_origin}/live_danmu/$day/$hour/$app'; "
    echo "##############添加分区   ${tbl_ex_live_danmu_data_origin}   ###########"

    tbl_ex_live_user_info_data_origin=" alter table ias_p2.tbl_ex_live_user_info_data_origin add if not exists partition (dt='$day',hour='$hour',app_id='$app') location '${p2_location_origin}/live_user_info/$day/$hour/$app'; "
    echo "##############添加分区   ${tbl_ex_live_user_info_data_origin}   ###########"

    tbl_ex_live_stream_url_data_origin=" alter table ias_p2.tbl_ex_live_stream_url_data_origin add if not exists partition (dt='$day',hour='$hour',app_id='$app') location '${p2_location_origin}/live_stream_url/$day/$hour/$app'; "
    echo "##############添加分区   ${tbl_ex_live_stream_url_data_origin}   ###########"

###暂时没有上报数据
#    tbl_ex_live_ip_list_data_origin=" alter table ias_p2.tbl_ex_live_ip_list_data_origin add if not exists partition (dt='$day',hour='$hour',app_id='$app') location '${p2_location_origin}/live_ip_list/$day/$hour/$app'; "
#    echo "##############添加分区   ${tbl_ex_live_ip_list_data_origin}   ###########"

###暂时没有上报数据
#    tbl_ex_live_share_link_data_origin=" alter table ias_p2.tbl_ex_live_share_link_data_origin add if not exists partition (dt='$day',hour='$hour',app_id='$app') location '${p2_location_origin}/live_share_link/$day/$hour/$app'; "
#    echo "##############添加分区   ${tbl_ex_live_share_link_data_origin}   ###########"


    tbl_ex_live_record_data_origin=" alter table ias_p2.tbl_ex_live_record_data_origin add if not exists partition (dt='$day',hour='$hour',app_id='$app') location '${p2_location_origin}/live_record/$day/$hour/$app'; "
    echo "##############添加分区   ${tbl_ex_live_record_data_origin}   ###########"

    tbl_ex_live_record_audience_count_data_origin=" alter table ias_p2.tbl_ex_live_record_audience_count_data_origin add if not exists partition (dt='$day',hour='$hour',app_id='$app') location '${p2_location_origin}/live_record_audience_count/$day/$hour/$app'; "
    echo "##############添加分区   ${tbl_ex_live_record_audience_count_data_origin}   ###########"

    tbl_ex_live_viewer_list_data_origin=" alter table ias_p2.tbl_ex_live_viewer_list_data_origin add if not exists partition (dt='$day',hour='$hour',app_id='$app') location '${p2_location_origin}/live_viewer_list/$day/$hour/$app'; "
    echo "##############添加分区   ${tbl_ex_live_viewer_list_data_origin}   ###########"

    tbl_ex_live_weibo_url_data_origin=" alter table ias_p2.tbl_ex_live_weibo_url_data_origin add if not exists partition (dt='$day',hour='$hour',app_id='$app') location '${p2_location_origin}/live_weibo_url/$day/$hour/$app'; "
    echo "##############添加分区   ${tbl_ex_live_weibo_url_data_origin}   ###########"

    tbl_ex_live_guard_list_data_origin=" alter table ias_p2.tbl_ex_live_guard_list_data_origin add if not exists partition (dt='$day',hour='$hour',app_id='$app') location '${p2_location_origin}/live_guard_list/$day/$hour/$app'; "
    echo "##############添加分区   ${tbl_ex_live_guard_list_data_origin}   ###########"

    tbl_ex_live_gift_contributor_list_data_origin=" alter table ias_p2.tbl_ex_live_gift_contributor_list_data_origin add if not exists partition (dt='$day',hour='$hour',app_id='$app') location '${p2_location_origin}/live_gift_contributor_list/$day/$hour/$app'; "
    echo "##############添加分区   ${tbl_ex_live_gift_contributor_list_data_origin}   ###########"


    executeHiveCommand "${tbl_ex_live_id_list_data_origin} ${tbl_ex_live_danmu_data_origin} ${tbl_ex_live_user_info_data_origin} ${tbl_ex_live_stream_url_data_origin} ${tbl_ex_live_record_data_origin} ${tbl_ex_live_record_audience_count_data_origin} ${tbl_ex_live_viewer_list_data_origin} ${tbl_ex_live_weibo_url_data_origin} ${tbl_ex_live_guard_list_data_origin} ${tbl_ex_live_gift_contributor_list_data_origin}"

    echo "##############   导入${app} origin表 end   ##################"

done

echo "##############   导入${all_live_app_list} origin 表 end ##################"

echo "##############   导入live_gift_info 表 start ##################"

   tbl_ex_live_gift_info_data_origin=" alter table ias_p2.tbl_ex_live_gift_info_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p2_location_origin}/live_gift_info/$day/$hour'; "
   echo "##############添加分区   ${tbl_ex_live_gift_info_data_origin}   ###########"

   executeHiveCommand "${tbl_ex_live_gift_info_data_origin}"

echo "##############   导入live_gift_info 表 end ##################"