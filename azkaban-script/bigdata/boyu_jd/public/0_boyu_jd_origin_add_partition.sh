#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
date=$1
yesterday=`date -d "-0 day $date" +%Y-%m-%d`
weekday=`date -d "-6 day $date" +%Y-%m-%d`
month2day=`date -d "-29 day $date" +%Y-%m-%d`
echo ${yesterday}
echo ${weekday}
echo ${month2day}
echo "=================================添加原始表分区 start==================================="

sql1="ALTER TABLE bigdata.boyu_jd_goods_origin add if not exists  partition (dt='${date}')
 LOCATION '/data/boyu_jd/origin/goods/${date}';"

executeHiveCommand "${sql1}"
