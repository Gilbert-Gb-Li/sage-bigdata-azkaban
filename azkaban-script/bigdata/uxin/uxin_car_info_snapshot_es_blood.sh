#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
esIndex=`date -d " $date" +%Y%m%d`

hive_sql="add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;insert into bigdata.uxin_car_info_snapshot_es select unix_timestamp('$date', 'yyyy-MM-dd')*1000, unix_timestamp(substr(occur_timestamp, 2, 19), 'yyyy-MM-dd HH:mm:ss')*1000, car_id,price,car_name,brand,mode,year,gear_box,version,down_payment,month_payment,nationwide_purchase, special_offer,return_car_3_days,certificate,warehouse, unix_timestamp(substr(registration_date, 2, 19), 'yyyy-MM-dd HH:mm:ss')*1000, apparent_mileage,emission_standards,car_color, engine_intake,lift_time_min,lift_time_max,engine_exhaust,check_people, unix_timestamp(substr(check_date, 2, 19), 'yyyy-MM-dd HH:mm:ss'), unix_timestamp(substr(last_maintenance_date, 2, 19), 'yyyy-MM-dd HH:mm:ss')*1000,fix_times,maintenance_times, accident_times,share_link, unix_timestamp(substr(param_timestamp, 2, 19), 'yyyy-MM-dd HH:mm:ss')*1000 , car_door_count, car_seat_count, car_type, car_sub_type, '$esIndex', 'car_info', 'uxin' from bigdata.uxin_car_info_snapshot where dt = '$date'; "

executeHiveCommand "${hive_sql}"