#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="INSERT INTO bigdata.uxin_car_parameter_snapshot partition(dt='$date') select occur_timestamp,car_id,car_door_count,car_seat_count,car_type,car_sub_type from( select *,row_number() over (partition by car_id order by occur_timestamp desc) as order_num from ( select occur_timestamp,car_id,car_door_count,car_seat_count,car_type,car_sub_type from bigdata.uxin_car_parameter_origin where dt='$date' and (car_door_count is not null and car_seat_count is not null and car_type is not null) union all select occur_timestamp,car_id,car_door_count,car_seat_count,car_type,car_sub_type from bigdata.uxin_car_parameter_snapshot where (dt='$yesterday')) d1 ) dd2 where dd2.order_num=1;"

executeHiveCommand "${hive_sql}"