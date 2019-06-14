#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
apk='com.renrenche.carapp'

echo "########### 每日新增在售数据 #################"
hive_sql_0="
INSERT INTO bigdata.renrenche_car_info_new_snapshot partition(dt='${date}')
select t1.data_generate_time,t1.first_data_generate_time,t1.appPackageName,t1.car_uid,car_name,t1.car_source_id,t1.car_sold_out,t1.car_price,
    t1.service_charge,t1.new_car_price_tax,t1.service_percentage,t1.car_is_strict_selection,t1.car_second_hand,t1.car_state
from(
    select a.data_generate_time,a.first_data_generate_time,a.appPackageName,a.car_uid,a.car_name,a.car_source_id,a.car_sold_out,a.car_price,
        a.service_charge,a.new_car_price_tax,a.service_percentage,a.car_is_strict_selection,a.car_second_hand,a.car_state,
        b.car_uid as b_car_uid
    from (
        SELECT data_generate_time,first_data_generate_time,appPackageName,car_uid,car_name,car_source_id,car_sold_out,car_price,
            service_charge,new_car_price_tax,service_percentage,car_is_strict_selection,car_second_hand,car_state
        FROM bigdata.renrenche_car_info_daily_snapshot
        WHERE (dt = '${date}'
            AND car_uid IS NOT NULL
            AND car_uid != ''
            AND car_state= 0)
    ) as a
    left join(
        SELECT data_generate_time,first_data_generate_time,appPackageName,car_uid,car_name,car_source_id,car_sold_out,car_price,
            service_charge,new_car_price_tax,service_percentage,car_is_strict_selection,car_second_hand,car_state
        FROM bigdata.renrenche_car_info_daily_snapshot
        WHERE (dt = '${yesterday}'
            AND car_uid IS NOT NULL
            AND car_uid != ''
            AND car_state= 0)
    ) as b
    on a.car_uid=b.car_uid
) as t1
where t1.b_car_uid is null;
"

echo "########### 每日新增下架数据 #################"
hive_sql_1="
INSERT INTO bigdata.renrenche_car_info_new_snapshot partition(dt='${date}')
select t1.data_generate_time,t1.first_data_generate_time,t1.appPackageName,t1.car_uid,car_name,t1.car_source_id,t1.car_sold_out,t1.car_price,
    t1.service_charge,t1.new_car_price_tax,t1.service_percentage,t1.car_is_strict_selection,t1.car_second_hand,t1.car_state
from(
    select a.data_generate_time,a.first_data_generate_time,a.appPackageName,a.car_uid,a.car_name,a.car_source_id,a.car_sold_out,a.car_price,
        a.service_charge,a.new_car_price_tax,a.service_percentage,a.car_is_strict_selection,a.car_second_hand,a.car_state,
        b.car_uid as b_car_uid
    from (
        SELECT data_generate_time,first_data_generate_time,appPackageName,car_uid,car_name,car_source_id,car_sold_out,car_price,
            service_charge,new_car_price_tax,service_percentage,car_is_strict_selection,car_second_hand,car_state
        FROM bigdata.renrenche_car_info_daily_snapshot
        WHERE (dt = '${date}'
            AND car_uid IS NOT NULL
            AND car_uid != ''
            AND car_state= 1)
    ) as a
    left join(
        SELECT data_generate_time,first_data_generate_time,appPackageName,car_uid,car_name,car_source_id,car_sold_out,car_price,
            service_charge,new_car_price_tax,service_percentage,car_is_strict_selection,car_second_hand,car_state
        FROM bigdata.renrenche_car_info_daily_snapshot
        WHERE (dt = '${yesterday}'
            AND car_uid IS NOT NULL
            AND car_uid != ''
            AND car_state= 1)
    ) as b
    on a.car_uid=b.car_uid
) as t1
where t1.b_car_uid is null;
"

echo "########### 每日新增已售数据 #################"
hive_sql_2="
INSERT INTO bigdata.renrenche_car_info_new_snapshot partition(dt='${date}')
select t1.data_generate_time,t1.first_data_generate_time,t1.appPackageName,t1.car_uid,car_name,t1.car_source_id,t1.car_sold_out,t1.car_price,
    t1.service_charge,t1.new_car_price_tax,t1.service_percentage,t1.car_is_strict_selection,t1.car_second_hand,t1.car_state
from(
    select a.data_generate_time,a.first_data_generate_time,a.appPackageName,a.car_uid,a.car_name,a.car_source_id,a.car_sold_out,a.car_price,
        a.service_charge,a.new_car_price_tax,a.service_percentage,a.car_is_strict_selection,a.car_second_hand,a.car_state,
        b.car_uid as b_car_uid
    from (
        SELECT data_generate_time,first_data_generate_time,appPackageName,car_uid,car_name,car_source_id,car_sold_out,car_price,
            service_charge,new_car_price_tax,service_percentage,car_is_strict_selection,car_second_hand,car_state
        FROM bigdata.renrenche_car_info_daily_snapshot
        WHERE (dt = '${date}'
            AND car_uid IS NOT NULL
            AND car_uid != ''
            AND car_state= 2)
    ) as a
    left join(
        SELECT data_generate_time,first_data_generate_time,appPackageName,car_uid,car_name,car_source_id,car_sold_out,car_price,
            service_charge,new_car_price_tax,service_percentage,car_is_strict_selection,car_second_hand,car_state
        FROM bigdata.renrenche_car_info_daily_snapshot
        WHERE (dt = '${yesterday}'
            AND car_uid IS NOT NULL
            AND car_uid != ''
            AND car_state= 2)
    ) as b
    on a.car_uid=b.car_uid
) as t1
where t1.b_car_uid is null;
"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.renrenche_car_info_new_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/renrenche/snapshot/renrenche_car_info_new_snapshot/dt=${date}

executeHiveCommand "
${delete_hive_partitions}
${hive_sql_0}
${hive_sql_1}
${hive_sql_2}
"

