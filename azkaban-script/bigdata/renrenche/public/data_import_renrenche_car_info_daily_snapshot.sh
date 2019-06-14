#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
apk='com.renrenche.carapp'

echo "########### 在售全量数据 #################"
hive_sql_0="
INSERT INTO bigdata.renrenche_car_info_daily_snapshot partition(dt='${date}')
select t1.data_generate_time,t1.first_data_generate_time,t1.appPackageName,t1.car_uid,t1.car_name,t1.car_source_id,t1.car_sold_out,t1.car_price,
    t1.service_charge,t1.new_car_price_tax,t1.service_percentage,t1.car_is_strict_selection,t1.car_second_hand,t1.car_state
from  (
        select a.data_generate_time,a.first_data_generate_time,a.appPackageName,a.car_uid,a.car_name,a.car_source_id,a.car_sold_out,a.car_price,
            a.service_charge,a.new_car_price_tax,a.service_percentage,a.car_is_strict_selection,a.car_second_hand,a.car_state,
            row_number() over (partition by a.car_uid order by a.data_generate_time desc) as row_num
        from(
            SELECT a1.data_generate_time,
                if(a2.first_data_generate_time is null,a1.data_generate_time,a2.first_data_generate_time) as first_data_generate_time,
                a1.appPackageName,split(a1.car_uid,'_')[0] as car_uid ,a1.car_name,a1.car_source_id,a1.car_sold_out,a1.car_price,
                a1.service_charge,a1.new_car_price_tax,a1.service_percentage,a1.car_is_strict_selection,a1.car_second_hand,a1.car_state
            from (
                SELECT * FROM bigdata.renrenche_car_info_origin
                WHERE (dt = '${date}' AND appPackageName='${apk}'
                    AND car_uid IS NOT NULL AND car_uid != ''
                    AND car_source_id IS NOT NULL AND car_source_id != ''
                    AND car_name IS NOT NULL AND car_name != ''
                    AND car_price IS NOT NULL AND car_price > 0
                    AND service_charge IS NOT NULL AND service_charge > 0
                    AND car_state= 0)
            ) as a1
            left join(
                SELECT first_data_generate_time,car_uid
                FROM bigdata.renrenche_car_info_daily_snapshot
                WHERE (dt = '${yesterday}'
                    AND car_uid IS NOT NULL
                    AND car_uid != ''
                    AND car_state= 0)
            ) as a2
            on split(a1.car_uid,'_')[0]=a2.car_uid
            UNION
            SELECT data_generate_time,first_data_generate_time,appPackageName,car_uid,car_name,car_source_id,car_sold_out,car_price,
                service_charge,new_car_price_tax,service_percentage,car_is_strict_selection,car_second_hand,car_state
            FROM bigdata.renrenche_car_info_daily_snapshot
            WHERE (dt = '${yesterday}'
                AND car_uid IS NOT NULL
                AND car_uid != ''
                AND car_state= 0)
        ) as a
      ) t1
where t1.row_num =1;
"

echo "########### 下架全量数据 #################"
hive_sql_1="
INSERT INTO bigdata.renrenche_car_info_daily_snapshot partition(dt='${date}')
select t1.data_generate_time,t1.first_data_generate_time,t1.appPackageName,t1.car_uid,t1.car_name,t1.car_source_id,t1.car_sold_out,t1.car_price,
    t1.service_charge,t1.new_car_price_tax,t1.service_percentage,t1.car_is_strict_selection,t1.car_second_hand,t1.car_state
from  (
        select a.data_generate_time,a.first_data_generate_time,a.appPackageName,a.car_uid,a.car_name,a.car_source_id,a.car_sold_out,a.car_price,
            a.service_charge,a.new_car_price_tax,a.service_percentage,a.car_is_strict_selection,a.car_second_hand,a.car_state,
            row_number() over (partition by a.car_uid order by a.data_generate_time) as row_num
        from(
            SELECT a1.data_generate_time,a1.data_generate_time as first_data_generate_time,a1.appPackageName,
                split(a1.car_uid,'_')[0] as car_uid,
                if(a1.car_name is not null and a1.car_name!='',a1.car_name,a2.car_name) as car_name,
                if(a1.car_source_id is not null and a1.car_source_id!='',a1.car_source_id,a2.car_source_id) as car_source_id,
                a1.car_sold_out,
                if(a1.car_price is not null and a1.car_price>0,a1.car_price,a2.car_price) as car_price,
                if(a1.service_charge is not null and a1.service_charge>0,a1.car_price,a2.service_charge) as service_charge,
                if(a1.new_car_price_tax is not null and a1.new_car_price_tax>0,a1.car_price,a2.new_car_price_tax) as new_car_price_tax,
                if(a1.service_percentage is not null and a1.service_percentage>0,a1.car_price,a2.service_percentage) as service_percentage,
                a1.car_is_strict_selection,
                a1.car_second_hand,
                a1.car_state
            from (
                SELECT * FROM bigdata.renrenche_car_info_origin
                WHERE (dt = '${date}'
                    AND appPackageName='${apk}'
                    AND car_uid IS NOT NULL
                    AND car_uid != ''
                    AND car_state= 1)
            ) as a1
            left join(
                SELECT car_uid,car_name,car_source_id,car_price,
                    service_charge,new_car_price_tax,service_percentage,car_is_strict_selection,car_second_hand
                FROM bigdata.renrenche_car_info_daily_snapshot
                WHERE (dt = '${yesterday}'
                    AND car_uid IS NOT NULL
                    AND car_uid != ''
                    AND car_state= 0)
            ) as a2
            on split(a1.car_uid,'_')[0]=a2.car_uid
            UNION
            SELECT data_generate_time,first_data_generate_time,appPackageName,car_uid,car_name,car_source_id,car_sold_out,car_price,
                service_charge,new_car_price_tax,service_percentage,car_is_strict_selection,car_second_hand,car_state
            FROM bigdata.renrenche_car_info_daily_snapshot
            WHERE (dt = '${yesterday}'
                AND car_uid IS NOT NULL
                AND car_uid != ''
                AND car_state= 1)
        ) as a
      ) t1
where t1.row_num =1;
"

echo "########### 已售全量数据 #################"
hive_sql_2="
INSERT INTO bigdata.renrenche_car_info_daily_snapshot partition(dt='${date}')
select t1.data_generate_time,t1.first_data_generate_time,t1.appPackageName,t1.car_uid,t1.car_name,t1.car_source_id,t1.car_sold_out,t1.car_price,
    t1.service_charge,t1.new_car_price_tax,t1.service_percentage,t1.car_is_strict_selection,t1.car_second_hand,t1.car_state
from  (
        select a.data_generate_time,a.first_data_generate_time,a.appPackageName,a.car_uid,a.car_name,a.car_source_id,a.car_sold_out,a.car_price,
            a.service_charge,a.new_car_price_tax,a.service_percentage,a.car_is_strict_selection,a.car_second_hand,a.car_state,
            row_number() over (partition by a.car_uid order by a.data_generate_time) as row_num
        from(
            SELECT a1.data_generate_time,a1.data_generate_time as first_data_generate_time,a1.appPackageName,
                split(a1.car_uid,'_')[0] as car_uid,
                if(a1.car_name is not null and a1.car_name!='',a1.car_name,a2.car_name) as car_name,
                if(a1.car_source_id is not null and a1.car_source_id!='',a1.car_source_id,a2.car_source_id) as car_source_id,
                a1.car_sold_out,
                if(a1.car_price is not null and a1.car_price>0,a1.car_price,a2.car_price) as car_price,
                if(a1.service_charge is not null and a1.service_charge>0,a1.car_price,a2.service_charge) as service_charge,
                if(a1.new_car_price_tax is not null and a1.new_car_price_tax>0,a1.car_price,a2.new_car_price_tax) as new_car_price_tax,
                if(a1.service_percentage is not null and a1.service_percentage>0,a1.car_price,a2.service_percentage) as service_percentage,
                a1.car_is_strict_selection,
                a1.car_second_hand,
                a1.car_state
            from (
                SELECT * FROM bigdata.renrenche_car_info_origin
                WHERE (dt = '${date}'
                    AND appPackageName='${apk}'
                    AND car_uid IS NOT NULL
                    AND car_uid != ''
                    AND car_state= 2)
            ) as a1
            left join(
                SELECT car_uid,car_name,car_source_id,car_price,
                    service_charge,new_car_price_tax,service_percentage,car_is_strict_selection,car_second_hand
                FROM bigdata.renrenche_car_info_daily_snapshot
                WHERE (dt = '${yesterday}'
                    AND car_uid IS NOT NULL
                    AND car_uid != ''
                    AND car_state= 0)
            ) as a2
            on split(a1.car_uid,'_')[0]=a2.car_uid
            UNION
            SELECT data_generate_time,first_data_generate_time,appPackageName,car_uid,car_name,car_source_id,car_sold_out,car_price,
                service_charge,new_car_price_tax,service_percentage,car_is_strict_selection,car_second_hand,car_state
            FROM bigdata.renrenche_car_info_daily_snapshot
            WHERE (dt = '${yesterday}'
                AND car_uid IS NOT NULL
                AND car_uid != ''
                AND car_state= 2)
        ) as a
      ) t1
where t1.row_num =1;
"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.renrenche_car_info_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/renrenche/snapshot/renrenche_car_info_daily_snapshot/dt=${date}

executeHiveCommand "
${delete_hive_partitions}
${hive_sql_0}
${hive_sql_1}
${hive_sql_2}
"

