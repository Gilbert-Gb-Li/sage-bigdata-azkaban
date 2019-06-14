#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql=`cat << EOF
INSERT INTO bigdata.uxin_car_off_sale_snapshot partition(dt='${date}')

select occur_timestamp,car_id
from (
  select  *,row_number() over (partition by car_id order by occur_timestamp desc) as order_num from  (
    select occur_timestamp,car_id
      from bigdata.uxin_car_off_sale_origin
      where dt='${date}' and car_id is not null and car_id != ''
  ) d1
) dd2 where dd2.order_num=1;
EOF`

executeHiveCommand "${hive_sql}"