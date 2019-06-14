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
sql1="ALTER TABLE bigdata.meituan_financial_shop_menu_list_origin add partition (dt='${yesterday}')
      LOCATION '/data/meituan/origin/shop_menu_list/${yesterday}';"
sql2="ALTER TABLE bigdata.meituan_financial_shop_list_origin add partition (dt='${yesterday}')
      LOCATION '/data/meituan/origin/shop_list/${yesterday}';"
executeHiveCommand "${sql1}"
executeHiveCommand "${sql2}"