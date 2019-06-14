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

p3_location=/data/bili
p3_location_origin=${p3_location}/origin
p3_location_snapshot=${p3_location}/snapshot

echo "##############  hive原始表添加分区 start ##################"
    user_info_origin=" alter table bigdata.bili_live_user_info_data_origin add if not exists partition (dt='$day') location '${p3_location_origin}/live_info/$day'; "
    echo "##############添加分区   ${user_info_origin}    ###########"
    danmu_origin=" alter table bigdata.bili_live_danmu_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_danmu_info/$day/$hour'; "
    echo "##############添加分区   ${danmu_origin}   ###########"
    gift_info_origin=" alter table bigdata.bili_live_gift_info_data_origin add if not exists partition (dt='$day') location '${p3_location_origin}/live_gift_info/$day'; "
    echo "##############添加分区   ${gift_info_origin}   ###########"
    guard_list_origin=" alter table bigdata.bili_live_guard_list_data_origin add if not exists partition (dt='$day') location '${p3_location_origin}/live_guard_list/$day'; "
    echo "##############添加分区   ${guard_list_origin}   ###########"
    contribution_origin=" alter table bigdata.bili_live_contribution_rank_data_origin add if not exists partition (dt='$day') location '${p3_location_origin}/live_contribution_rank/$day'; "
    echo "##############添加分区   ${contribution_origin}   ###########"
    live_id_list=" alter table bigdata.bili_live_id_list_data_origin add if not exists partition (dt='$day') location '${p3_location_origin}/live_id_list/$day'; "
    echo "##############添加分区   ${live_id_list}   ###########"
    guard_num_origin="alter table bigdata.bili_live_guard_num_data_origin add if not exists partition (dt='$day') location '${p3_location_origin}/live_guard_num/$day'; "
    echo "##############添加分区   ${guard_num_origin}   ###########"
    executeHiveCommand "${danmu_origin} ${user_info_origin} ${gift_info_origin} ${guard_list_origin} ${contribution_origin} ${live_id_list} ${guard_num_origin}"
echo "##############   hive原始表添加分区 end  ##################"

