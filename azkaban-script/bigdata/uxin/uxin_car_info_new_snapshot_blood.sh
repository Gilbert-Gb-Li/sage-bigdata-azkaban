#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql=`cat << EOF
use bigdata;
insert into bigdata.uxin_car_info_new_snapshot partition(dt='${date}')
select
  b1.occur_timestamp,b1.car_id,b1.price,b1.car_name,b1.brand,b1.mode,b1.year,b1.gear_box,b1.version,b1.down_payment,b1.month_payment,b1.nationwide_purchase,b1.special_offer,b1.return_car_3_days,b1.certificate,b1.warehouse,b1.registration_date,b1.apparent_mileage,b1.emission_standards,b1.car_color,b1.engine_intake,b1.lift_time_min,b1.lift_time_max,b1.engine_exhaust,b1.check_people,b1.check_date,b1.last_maintenance_date,b1.fix_times,b1.maintenance_times,b1.accident_times,b1.share_link
 ,b2.occur_timestamp as param_timestamp,b2.car_door_count,b2.car_seat_count,b2.car_type,b2.car_sub_type
 ,b3.occur_timestamp as analysis_tiemstamp,b3.lower_avg_price,b3.same_items_count
 ,b4.occur_timestamp as payment_timestamp, b4.down_payment_1, b4.down_payment_3, b4.down_payment_5, b4.month_payment_1_48, b4.month_payment_1_36, b4.month_payment_3_36, b4.month_payment_3_24, b4.month_payment_5_36, b4.month_payment_5_24
,0, null, 0, null
from (
  select * from bigdata.uxin_car_info_origin a1 where dt='${date}'
  and a1.car_id not in (select car_id from bigdata.uxin_car_info_all_snapshot where dt='${yesterday}')
) b1
left join bigdata.uxin_car_parameter_snapshot b2 on b1.car_id = b2.car_id and b2.dt='${date}'
left join bigdata.uxin_car_payment_snapshot b4  on b1.car_id = b4.car_id and b4.dt='${date}'
left join bigdata.uxin_car_price_analysis_snapshot b3 on b1.car_id = b3.car_id and b3.dt='${date}'
;
EOF`

executeHiveCommand "${hive_sql}"