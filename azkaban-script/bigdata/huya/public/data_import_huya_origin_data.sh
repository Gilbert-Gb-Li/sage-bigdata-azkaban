#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
hour=$2

########################################
#######完成HIVE表和HDFS文件分区映射#######
########################################

createPartitionDiffHour bigdata.huya_live_id_list_origin /data/huya/origin/live_id_list $date $hour
createPartitionDiffHour bigdata.huya_live_user_info_origin /data/huya/origin/live_user_info $date $hour
createPartitionDiffHour bigdata.huya_live_danmu_origin /data/huya/origin/live_danmu $date $hour
createPartitionDiffHour bigdata.huya_live_gift_origin /data/huya/origin/live_gift $date $hour
createPartitionDiffHour bigdata.huya_live_week_rank_list_origin /data/huya/origin/live_weekRank_list $date $hour