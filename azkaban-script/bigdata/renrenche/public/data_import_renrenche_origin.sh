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

p3_location=/data/renrenche
p3_location_origin=${p3_location}/origin
p3_location_snapshot=${p3_location}/snapshot

echo "##############  hive原始表添加分区 start ##################"
    renrenche_car_info_origin=" alter table bigdata.renrenche_car_info_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/renrenche_car_info_origin/$day/$hour'; "
    echo "##############添加分区   ${renrenche_car_info_origin}    ###########"
    renrenche_car_installment_origin=" alter table bigdata.renrenche_car_installment_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/renrenche_car_installment_origin/$day/$hour'; "
    echo "##############添加分区   ${renrenche_car_installment_origin}   ###########"
    renrenche_car_params_origin=" alter table bigdata.renrenche_car_params_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/renrenche_car_params_origin/$day/$hour'; "
    echo "##############添加分区   ${renrenche_car_params_origin}   ###########"
    executeHiveCommand "${renrenche_car_info_origin} ${renrenche_car_installment_origin} ${renrenche_car_params_origin}"
echo "##############   hive原始表添加分区 end  ##################"

