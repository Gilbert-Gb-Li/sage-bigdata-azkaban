#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
apk='com.renrenche.carapp'

echo "########### 在售数据 #################"
hive_sql_0="
INSERT INTO bigdata.renrenche_car_info_finance_snapshot partition(dt='${date}')
select t1.data_generate_time,t1.first_data_generate_time,t1.appPackageName,
      t1.car_uid,t1.car_source_id,
      t1.car_name,t1.car_brand,t1.car_series,
      t1.car_type,t1.emission_standard,t1.car_price,t1.service_charge,t1.period,t1.price_range,t1.car_state
from  (
            SELECT a1.data_generate_time,a1.first_data_generate_time,
                a1.appPackageName,a1.car_uid,a1.car_source_id,a1.car_name,
                trim(split(split(a1.car_name,' ')[0],'-')[0]) as car_brand,
                trim(split(split(a1.car_name,' ')[0],'-')[1]) as car_series,
                if(a3.basic_motorcycle_type is not null and a3.basic_motorcycle_type!='',a3.basic_motorcycle_type,'无') as car_type,
                if(a3.eparam_emission_standard is not null and a3.eparam_emission_standard!='',a3.eparam_emission_standard,'无') as emission_standard,
                a1.car_price,a1.service_charge,
                datediff(dt,from_unixtime(cast(substr(first_data_generate_time,0,10) as bigint),'yyyy-MM-dd')) as period,
                CASE WHEN round(a1.car_price) < 50000 THEN '5万以下'
                     WHEN round(a1.car_price) >= 50000  AND round(a1.car_price) < 100000 THEN '5~10万'
                     WHEN round(a1.car_price) >= 100000 AND round(a1.car_price) < 150000 THEN '10~15万'
                     WHEN round(a1.car_price) >= 150000 AND round(a1.car_price) < 200000 THEN '15~20万'
                     WHEN round(a1.car_price) >= 200000 AND round(a1.car_price) < 300000 THEN '20~30万'
                     WHEN round(a1.car_price) >= 300000 AND round(a1.car_price) < 500000 THEN '30~50万'
                     WHEN round(a1.car_price) >= 500000 THEN '50万以上'
                ELSE '无价格' END price_range,
                a1.car_state,
                a2.car_uid as b_car_uid
            from (
                SELECT * FROM bigdata.renrenche_car_info_daily_snapshot
                WHERE (dt = '${date}' AND appPackageName='${apk}'
                    AND car_uid IS NOT NULL AND car_uid != ''
                    AND car_source_id IS NOT NULL AND car_source_id != ''
                    AND car_name IS NOT NULL AND car_name != ''
                    AND car_price IS NOT NULL AND car_price > 0
                    AND service_charge IS NOT NULL AND service_charge > 0
                    AND car_state= 0)
            ) as a1
            left join(
                SELECT car_uid FROM bigdata.renrenche_car_info_daily_snapshot
                WHERE (dt = '${date}'
                    AND car_uid IS NOT NULL
                    AND car_uid != ''
                    AND car_state!= 0)
            ) as a2
            on a1.car_uid=a2.car_uid
            left join(
                SELECT car_uid,basic_motorcycle_type,eparam_emission_standard
                FROM bigdata.renrenche_car_params_daily_snapshot
                WHERE (dt = '${date}'
                    AND car_uid IS NOT NULL AND car_uid != ''
                    AND car_name IS NOT NULL AND car_name != '')
            ) as a3
            on a1.car_uid=a3.car_uid
      ) t1
where t1.b_car_uid is null;
"



echo "########### 下架全量数据 #################"
hive_sql_1="
INSERT INTO bigdata.renrenche_car_info_finance_snapshot partition(dt='${date}')
select t1.data_generate_time,t1.first_data_generate_time_0,t1.appPackageName,
       t1.car_uid,t1.car_source_id,t1.car_name,
       if(t1.car_name is not null,trim(split(split(t1.car_name,' ')[0],'-')[0]),'') as car_brand,
       if(t1.car_name is not null,trim(split(split(t1.car_name,' ')[0],'-')[1]),'') as car_series,
       t1.car_type,t1.emission_standard,t1.car_price,t1.service_charge,
       datediff(from_unixtime(cast(substr(t1.first_data_generate_time_1,0,10) as bigint),'yyyy-MM-dd'),from_unixtime(cast(substr(t1.first_data_generate_time_0,0,10) as bigint),'yyyy-MM-dd')) as period,
       CASE WHEN round(t1.car_price) < 50000 THEN '5万以下'
             WHEN round(t1.car_price) >= 50000  AND round(t1.car_price) < 100000 THEN '5~10万'
             WHEN round(t1.car_price) >= 100000 AND round(t1.car_price) < 150000 THEN '10~15万'
             WHEN round(t1.car_price) >= 150000 AND round(t1.car_price) < 200000 THEN '15~20万'
             WHEN round(t1.car_price) >= 200000 AND round(t1.car_price) < 300000 THEN '20~30万'
             WHEN round(t1.car_price) >= 300000 AND round(t1.car_price) < 500000 THEN '30~50万'
             WHEN round(t1.car_price) >= 500000 THEN '50万以上'
       ELSE '无价格' END price_range,
       t1.car_state
from  (
            SELECT a1.data_generate_time,
                if(a2.first_data_generate_time is not null and a2.first_data_generate_time>0,a2.first_data_generate_time,a1.first_data_generate_time) as first_data_generate_time_0,
                a1.first_data_generate_time as first_data_generate_time_1,
                a1.appPackageName,a1.car_uid,
                if(a1.car_source_id is not null and a1.car_source_id!='',a1.car_source_id,a2.car_source_id) as car_source_id,
                if(a1.car_name is not null and a1.car_name!='',a1.car_name,a2.car_name) as car_name,
                if(a3.basic_motorcycle_type is not null and a3.basic_motorcycle_type!='',a3.basic_motorcycle_type,'无') as car_type,
                if(a3.eparam_emission_standard is not null and a3.eparam_emission_standard!='',a3.eparam_emission_standard,'无') as emission_standard,
                if(a1.car_price is not null and a1.car_price>0,a1.car_price,a2.car_price) as car_price,
                if(a1.service_charge is not null and a1.service_charge>0,a1.service_charge,a2.service_charge) as service_charge,
                a1.car_state
            from (
                SELECT * FROM bigdata.renrenche_car_info_daily_snapshot
                WHERE (dt = '${date}' AND appPackageName='${apk}'
                    AND car_uid IS NOT NULL AND car_uid != ''
                    AND car_state= 1)
            ) as a1
            left join(
                SELECT first_data_generate_time,car_uid,car_source_id,car_name,car_price,service_charge
                FROM bigdata.renrenche_car_info_daily_snapshot
                WHERE (dt = '${date}'
                    AND car_uid IS NOT NULL
                    AND car_uid != ''
                    AND car_state = 0)
            ) as a2
            on a1.car_uid=a2.car_uid
            left join(
                SELECT car_uid,basic_motorcycle_type,eparam_emission_standard
                FROM bigdata.renrenche_car_params_daily_snapshot
                WHERE (dt = '${date}'
                    AND car_uid IS NOT NULL AND car_uid != ''
                    AND car_name IS NOT NULL AND car_name != '')
            ) as a3
            on a1.car_uid=a3.car_uid
      ) t1
;
"

echo "########### 已售全量数据 #################"
hive_sql_2="
INSERT INTO bigdata.renrenche_car_info_finance_snapshot partition(dt='${date}')
select t1.data_generate_time,t1.first_data_generate_time_0,t1.appPackageName,
       t1.car_uid,t1.car_source_id,t1.car_name,
       if(t1.car_name is not null,trim(split(split(t1.car_name,' ')[0],'-')[0]),'') as car_brand,
       if(t1.car_name is not null,trim(split(split(t1.car_name,' ')[0],'-')[1]),'') as car_series,
       t1.car_type,t1.emission_standard,t1.car_price,t1.service_charge,
       datediff(from_unixtime(cast(substr(t1.first_data_generate_time_2,0,10) as bigint),'yyyy-MM-dd'),from_unixtime(cast(substr(t1.first_data_generate_time_0,0,10) as bigint),'yyyy-MM-dd')) as period,
       CASE WHEN round(t1.car_price) < 50000 THEN '5万以下'
             WHEN round(t1.car_price) >= 50000  AND round(t1.car_price) < 100000 THEN '5~10万'
             WHEN round(t1.car_price) >= 100000 AND round(t1.car_price) < 150000 THEN '10~15万'
             WHEN round(t1.car_price) >= 150000 AND round(t1.car_price) < 200000 THEN '15~20万'
             WHEN round(t1.car_price) >= 200000 AND round(t1.car_price) < 300000 THEN '20~30万'
             WHEN round(t1.car_price) >= 300000 AND round(t1.car_price) < 500000 THEN '30~50万'
             WHEN round(t1.car_price) >= 500000 THEN 7
       ELSE '无价格' END price_range,
       t1.car_state
from  (
            SELECT a1.data_generate_time,
                if(a2.first_data_generate_time is not null and a2.first_data_generate_time>0,a2.first_data_generate_time,a1.first_data_generate_time) as first_data_generate_time_0,
                a1.first_data_generate_time as first_data_generate_time_2,
                a1.appPackageName,a1.car_uid,
                if(a1.car_source_id is not null and a1.car_source_id!='',a1.car_source_id,a2.car_source_id) as car_source_id,
                if(a1.car_name is not null and a1.car_name!='',a1.car_name,a2.car_name) as car_name,
                if(a3.basic_motorcycle_type is not null and a3.basic_motorcycle_type!='',a3.basic_motorcycle_type,'无') as car_type,
                if(a3.eparam_emission_standard is not null and a3.eparam_emission_standard!='',a3.eparam_emission_standard,'无') as emission_standard,
                if(a1.car_price is not null and a1.car_price>0,a1.car_price,a2.car_price) as car_price,
                if(a1.service_charge is not null and a1.service_charge>0,a1.service_charge,a2.service_charge) as service_charge,
                a1.car_state
            from (
                SELECT * FROM bigdata.renrenche_car_info_daily_snapshot
                WHERE (dt = '${date}' AND appPackageName='${apk}'
                    AND car_uid IS NOT NULL AND car_uid != ''
                    AND car_state= 2)
            ) as a1
            left join(
                SELECT first_data_generate_time,car_uid,car_source_id,car_name,car_price,service_charge
                FROM bigdata.renrenche_car_info_daily_snapshot
                WHERE (dt = '${date}'
                    AND car_uid IS NOT NULL
                    AND car_uid != ''
                    AND car_state = 0)
            ) as a2
            on a1.car_uid=a2.car_uid
            left join(
                SELECT car_uid,basic_motorcycle_type,eparam_emission_standard
                FROM bigdata.renrenche_car_params_daily_snapshot
                WHERE (dt = '${date}'
                    AND car_uid IS NOT NULL AND car_uid != ''
                    AND car_name IS NOT NULL AND car_name != '')
            ) as a3
            on a1.car_uid=a3.car_uid
      ) t1
;
"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.renrenche_car_info_finance_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/renrenche/snapshot/renrenche_car_info_finance_snapshot/dt=${date}

executeHiveCommand "
${delete_hive_partitions}
${hive_sql_0}
${hive_sql_1}
${hive_sql_2}
"

