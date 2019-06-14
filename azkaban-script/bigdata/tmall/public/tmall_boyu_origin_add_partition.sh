#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1

########################################
#######完成HIVE表和HDFS文件分区映射#######
########################################

createPartitionDiff bigdata.tmall_boyu_goods_origin /data/tmall/origin/tmall_boyu_goods_origin/ $date