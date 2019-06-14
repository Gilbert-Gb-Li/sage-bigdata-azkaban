#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql=`cat << EOF
INSERT INTO bigdata.uxin_car_payment_snapshot partition(dt='${date}')
select occur_timestamp, car_id, down_payment_1, down_payment_3, down_payment_5, month_payment_1_48, month_payment_1_36, month_payment_3_36, month_payment_3_24, month_payment_5_36, month_payment_5_24
from (
select  *,row_number() over (partition by car_id order by occur_timestamp desc) as order_num from  (
  select occur_timestamp, car_id, down_payment_1, down_payment_3, down_payment_5, month_payment_1_48, month_payment_1_36, month_payment_3_36, month_payment_3_24, month_payment_5_36, month_payment_5_24
    from bigdata.uxin_car_payment_origin
    where dt='${date}' and car_id is not null and (
      (down_payment_1 is not null and month_payment_1_48 is not null and month_payment_1_36 is not null)
      or
      (down_payment_3 is not null and month_payment_3_36 is not null and month_payment_3_24 is not null)
      or
      (down_payment_5 is not null and month_payment_5_36 is not null and month_payment_5_24 is not null)
    )
  union all
  select occur_timestamp, car_id, down_payment_1, down_payment_3, down_payment_5, month_payment_1_48, month_payment_1_36, month_payment_3_36, month_payment_3_24, month_payment_5_36, month_payment_5_24
    from  bigdata.uxin_car_payment_snapshot where (dt='${yesterday}')
) d1
) dd2 where dd2.order_num=1;
EOF`

executeHiveCommand "${hive_sql}"