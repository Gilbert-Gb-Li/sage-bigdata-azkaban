#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
hour=$2

sh -x ${GOBBLIN_HOME}/start-mr.sh ias-live.pull ${GOBBLIN_HOME}

createPartitionDiffHour ias.tbl_ex_live_online_anchor_data_origin /data/ias/origin/ias-live1 $date $hour
createPartitionDiffHour ias.tbl_ex_live_room_data_origin /data/ias/origin/ias-live2 $date $hour
createPartitionDiffHour ias.tbl_ex_live_user_info_data_origin /data/ias/origin/ias-live3 $date $hour
