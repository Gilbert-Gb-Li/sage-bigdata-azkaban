#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
esIndex=`date -d " $date" +%Y%m%d`

hive_sql=`cat << EOF
add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;

INSERT INTO bigdata.uxin_car_info_all_snapshot_es
SELECT
  unix_timestamp('${date}', 'yyyy-MM-dd') * 1000,
  unix_timestamp(substr(occur_timestamp, 2, 19), 'yyyy-MM-dd HH:mm:ss') * 1000,
  car_id,
  price,
  car_name,
  brand,
  MODE,
  YEAR,
  gear_box,
  version,
  down_payment,
  month_payment,
  nationwide_purchase,
  special_offer,
  return_car_3_days,
  certificate,
  warehouse,
  unix_timestamp(substr(registration_date, 2, 19), 'yyyy-MM-dd HH:mm:ss') * 1000,
  apparent_mileage,
  emission_standards,
  car_color,
  engine_intake,
  lift_time_min,
  lift_time_max,
  engine_exhaust,
  check_people,
  unix_timestamp(substr(check_date, 2, 19), 'yyyy-MM-dd HH:mm:ss'),
  unix_timestamp(substr(last_maintenance_date, 2, 19), 'yyyy-MM-dd HH:mm:ss') * 1000,
  fix_times,
  maintenance_times,
  accident_times,
  share_link,
  unix_timestamp(substr(param_timestamp, 2, 19), 'yyyy-MM-dd HH:mm:ss') * 1000,
  car_door_count,
  car_seat_count,
  car_type,
  car_sub_type,
  unix_timestamp(substr(analysis_timestamp, 2, 19), 'yyyy-MM-dd HH:mm:ss') * 1000,
  lower_avg_price,
  same_items_count,
  unix_timestamp(substr(payment_timestamp, 2, 19), 'yyyy-MM-dd HH:mm:ss') * 1000,
  down_payment_1,
  down_payment_3,
  down_payment_5,
  month_payment_1_48,
  month_payment_1_36,
  month_payment_3_36,
  month_payment_3_24,
  month_payment_5_36,
  month_payment_5_24,
  off_sale_status,
  unix_timestamp(substr(off_sale_timestamp, 2, 19), 'yyyy-MM-dd HH:mm:ss') * 1000,
  had_sale_status,
  unix_timestamp(substr(had_sale_timestamp, 2, 19), 'yyyy-MM-dd HH:mm:ss') * 1000,
  new_sale_status,
  current_status,
  '${esIndex}',
  'car_info',
  'uxin'
FROM
  bigdata.uxin_car_info_all_snapshot
WHERE
  dt = '${date}' and car_id is not null and car_id != '';
EOF`

executeHiveCommand "${hive_sql}"