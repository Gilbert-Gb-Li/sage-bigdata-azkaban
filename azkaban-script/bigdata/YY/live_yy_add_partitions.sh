#!/bin/sh

source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh


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

p3_location_yy=/data/yy
p3_location_origin_yy=${p3_location_yy}/origin
p3_location_snapshot_yy=${p3_location_yy}/snapshot

echo "##############  hive原始表添加分区 start ##################"
    tbl_ex_live_id_list_data_origin=" alter table bigdata.YY_live_id_list_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin_yy}/live_id_list/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_id_list_data_origin}    ###########"
    tbl_ex_live_danmu_data_origin=" alter table bigdata.YY_live_danmu_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin_yy}/live_danmu/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_danmu_data_origin}   ###########"
    tbl_ex_live_user_info_data_origin=" alter table bigdata.YY_live_user_info_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin_yy}/live_user_info/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_user_info_data_origin}   ###########"
    tbl_ex_live_gift_info_data_origin=" alter table bigdata.YY_live_gift_info_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin_yy}/live_gift_info/$day/$hour'; "
    echo "##############添加分区   ${tbl_ex_live_gift_info_data_origin}   ###########"
    executeHiveCommand "${tbl_ex_live_id_list_data_origin} ${tbl_ex_live_danmu_data_origin} ${tbl_ex_live_user_info_data_origin} ${tbl_ex_live_gift_info_data_origin}"
echo "##############   hive原始表添加分区 end  ##################"

