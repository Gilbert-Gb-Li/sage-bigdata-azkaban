#!bin/bash

p3_location_origin='/data/renren/origin'
day=$1

echo "##############  hive表加载数据 start ##################"

for hour in $(seq 0 23)
do
if [ $hour -lt 10 ] ;then
hour="0${hour}"
fi
    renren_car_info_origin=" alter table bigdata.renren_car_info_daily_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/car_info/$day/$hour'; "
    echo "##############添加分区   ${renren_car_info_origin}    ###########"
    /usr/bin/hive -e "${renren_car_info_origin}"
    renren_car_price_origin=" alter table bigdata.renren_car_price_daily_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/car_price/$day/$hour'; "
    echo "##############添加分区   ${renren_car_price_origin}   ###########"
    /usr/bin/hive -e "${renren_car_price_origin}"
    renren_car_params_origin=" alter table bigdata.renren_car_params_daily_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/car_params/$day/$hour'; "
    echo "##############添加分区   ${renren_car_params_origin}   ###########"
    /usr/bin/hive -e "${renren_car_params_origin}"
done

echo "##############   hive表加载数据 end  ##################"

