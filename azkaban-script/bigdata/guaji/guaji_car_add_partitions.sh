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

p3_location=/data/guaji
p3_location_origin=${p3_location}/origin
p3_location_snapshot=${p3_location}/snapshot

echo "##############  hive原始表添加分区 start ##################"
    guaji_car_info_origin=" alter table bigdata.guaji_car_info_daily_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/car_info/$day/$hour'; "
    echo "##############添加分区   ${guaji_car_info_origin}    ###########"
    guaji_car_price_origin=" alter table bigdata.guaji_car_price_daily_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/car_price/$day/$hour'; "
    echo "##############添加分区   ${guaji_car_price_origin}   ###########"
    guaji_car_params_origin=" alter table bigdata.guaji_car_params_daily_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/car_params/$day/$hour'; "
    echo "##############添加分区   ${guaji_car_params_origin}   ###########"
    executeHiveCommand "${guaji_car_info_origin} ${guaji_car_price_origin} ${guaji_car_params_origin}"
echo "##############   hive原始表添加分区 end  ##################"

