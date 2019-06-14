#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
apk='com.renrenche.carapp'

hive_sql="
INSERT INTO bigdata.renrenche_car_installment_daily_snapshot partition(dt='${date}')
select t1.data_generate_time,t1.appPackageName,t1.car_uid,t1.car_name,t1.car_price,t1.car_down_payment,t1.car_month_payment,t1.car_month_period
from  (
        select a.data_generate_time,a.appPackageName,a.car_uid,a.car_name,a.car_price,a.car_down_payment,a.car_month_payment,a.car_month_period,
            row_number() over (partition by a.car_uid order by a.data_generate_time desc) as row_num
        from(
            SELECT data_generate_time,appPackageName,split(car_uid,'_')[0] as car_uid,car_name,car_price,car_down_payment,car_month_payment,car_month_period
            FROM bigdata.renrenche_car_installment_origin
            WHERE (dt = '${date}'
                AND appPackageName='${apk}'
                AND car_uid IS NOT NULL AND car_uid != ''
                AND car_name IS NOT NULL AND car_name != '')
            UNION
            SELECT data_generate_time,appPackageName,car_uid,car_name,car_price,car_down_payment,car_month_payment,car_month_period
            FROM bigdata.renrenche_car_installment_daily_snapshot
            WHERE (dt = '${yesterday}'
                AND car_uid IS NOT NULL
                AND car_uid != '')
        ) as a
      ) t1
where t1.row_num =1;
"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.renrenche_car_installment_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/renrenche/snapshot/renrenche_car_installment_daily_snapshot/dt=${date}

executeHiveCommand "
${delete_hive_partitions}
${hive_sql}
"
