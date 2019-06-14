#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql=`cat << EOF
insert into bigdata.uxin_car_info_all_snapshot partition(dt='${date}')


select
 a1.occur_timestamp,a1.car_id,a1.price,a1.car_name,a1.brand,a1.mode,a1.year,a1.gear_box,a1.version,a1.down_payment,a1.month_payment,a1.nationwide_purchase,a1.special_offer,a1.return_car_3_days,a1.certificate,a1.warehouse,a1.registration_date,a1.apparent_mileage,a1.emission_standards,a1.car_color,a1.engine_intake,a1.lift_time_min,a1.lift_time_max,a1.engine_exhaust,a1.check_people,a1.check_date,a1.last_maintenance_date,a1.fix_times,a1.maintenance_times,a1.accident_times,a1.share_link
 ,a1.param_timestamp,a1.car_door_count,a1.car_seat_count,a1.car_type,a1.car_sub_type
 ,a1.analysis_timestamp,a1.lower_avg_price,a1.same_items_count
 ,a1.payment_timestamp, a1.down_payment_1, a1.down_payment_3, a1.down_payment_5, a1.month_payment_1_48, a1.month_payment_1_36, a1.month_payment_3_36, a1.month_payment_3_24, a1.month_payment_5_36, a1.month_payment_5_24
 , case when a2.car_id is not null then 1 else 0 end as off_sale_status, a2.occur_timestamp as off_sale_timestamp
 , case when a3.car_id is not null then 1 else 0 end as had_sale_status, a3.occur_timestamp as had_sale_timestamp
 , 0, 1
from (
  select * from bigdata.uxin_car_info_all_snapshot
  where dt='${yesterday}' and off_sale_status != 1 and had_sale_status != 1
) a1
left join (
  select * from bigdata.uxin_car_off_sale_snapshot where  dt='${date}'
) a2 on  a1.car_id=a2.car_id
left join (
  select * from bigdata.uxin_car_had_sale_snapshot where  dt='${date}'
) a3 on  a1.car_id=a3.car_id

union all

select
 b2.occur_timestamp,b2.car_id,b2.price,b2.car_name,b2.brand,b2.mode,b2.year,b2.gear_box,b2.version,b2.down_payment,b2.month_payment,b2.nationwide_purchase,b2.special_offer,b2.return_car_3_days,b2.certificate,b2.warehouse,b2.registration_date,b2.apparent_mileage,b2.emission_standards,b2.car_color,b2.engine_intake,b2.lift_time_min,b2.lift_time_max,b2.engine_exhaust,b2.check_people,b2.check_date,b2.last_maintenance_date,b2.fix_times,b2.maintenance_times,b2.accident_times,b2.share_link
 ,b2.param_timestamp,b2.car_door_count,b2.car_seat_count,b2.car_type,b2.car_sub_type
 ,b2.analysis_timestamp,b2.lower_avg_price,b2.same_items_count
 ,b2.payment_timestamp, b2.down_payment_1, b2.down_payment_3, b2.down_payment_5, b2.month_payment_1_48, b2.month_payment_1_36, b2.month_payment_3_36, b2.month_payment_3_24, b2.month_payment_5_36, b2.month_payment_5_24
 ,0,null,0,null,1,1
from bigdata.uxin_car_info_new_snapshot b2 where dt='${date}'

union all
select
 b3.occur_timestamp,b3.car_id,b3.price,b3.car_name,b3.brand,b3.mode,b3.year,b3.gear_box,b3.version,b3.down_payment,b3.month_payment,b3.nationwide_purchase,b3.special_offer,b3.return_car_3_days,b3.certificate,b3.warehouse,b3.registration_date,b3.apparent_mileage,b3.emission_standards,b3.car_color,b3.engine_intake,b3.lift_time_min,b3.lift_time_max,b3.engine_exhaust,b3.check_people,b3.check_date,b3.last_maintenance_date,b3.fix_times,b3.maintenance_times,b3.accident_times,b3.share_link
 ,b3.param_timestamp,b3.car_door_count,b3.car_seat_count,b3.car_type,b3.car_sub_type
 ,b3.analysis_timestamp,b3.lower_avg_price,b3.same_items_count
 ,b3.payment_timestamp, b3.down_payment_1, b3.down_payment_3, b3.down_payment_5, b3.month_payment_1_48, b3.month_payment_1_36, b3.month_payment_3_36, b3.month_payment_3_24, b3.month_payment_5_36, b3.month_payment_5_24
 ,b3.off_sale_status, b3.off_sale_timestamp, b3.had_sale_status, b3.had_sale_timestamp
 , 0, 0
from bigdata.uxin_car_info_all_snapshot b3
where dt='${yesterday}' and (off_sale_status = 1 or had_sale_status = 1)
EOF`

executeHiveCommand "${hive_sql}"