#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

data_arr="ias-live1,ias.tbl_ex_live_online_anchor_data_origin ias-live2,ias.tbl_ex_live_room_data_origin ias-live3,ias.tbl_ex_live_user_info_data_origin"
project=ias

export_hdfs_data "$data_arr" $1 $project $3
