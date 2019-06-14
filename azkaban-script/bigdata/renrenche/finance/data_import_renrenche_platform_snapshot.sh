#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
apk='com.renrenche.carapp'

echo "########### 人人车平台统计数据数据 #################"
hive_sql="
INSERT INTO bigdata.renrenche_platform_snapshot partition(dt='${date}')
select t1.appPackageName,
       max(t1.on_sale_car_count) as on_sale_car_count,
       max(t1.on_sale_car_count_origin) as on_sale_car_count_origin,
       max(t1.sale_out_car_count) as sale_out_car_count,
       max(t1.new_sale_out_car_count) as new_sale_out_car_count,
       max(t1.sold_out_car_count) as sold_out_car_count,
       max(t1.new_sold_out_car_count) as new_sold_out_car_count,
       max(t1.car_all_count_today)-max(t1.car_all_count_yesterday) as new_car_count,
       max(t1.sale_out_car_all_price) as sale_out_car_all_price,
       max(t1.sale_out_car_all_service_charge) as sale_out_car_all_service_charge,
       (max(t1.sale_out_car_all_price)/max(t1.car_count)) as sale_out_car_avg_price,
       round(max(t1.sale_out_period)/max(t1.car_count)) as sale_out_car_avg_period,
       round(max(t1.on_sale_period)/max(t1.on_sale_car_count)) as on_sale_car_avg_period,
       max(t1.car_all_count_today)
from  (
                SELECT appPackageName,
                    count(distinct car_uid) as on_sale_car_count,
                    sum(period) as on_sale_period,
                    0 as on_sale_car_count_origin,
                    0 as sale_out_car_count,
                    0 as sold_out_car_count,
                    0 as car_count,
                    0 as sale_out_car_all_price,
                    0 as sale_out_car_all_service_charge,
                    0 as sale_out_period,
                    0 as car_all_count_today,
                    0 as car_all_count_yesterday,
                    0 as new_sale_out_car_count,
                    0 as new_sold_out_car_count
                FROM bigdata.renrenche_car_info_finance_snapshot
                WHERE (dt = '${date}' AND appPackageName='${apk}'
                    AND car_uid IS NOT NULL
                    AND car_uid != ''
                    AND car_state = 0)
                group by appPackageName
                UNION ALL
                SELECT  appPackageName,
                    0 as on_sale_car_count,
                    0 as on_sale_period,
                    count(distinct car_uid) as on_sale_car_count_origin,
                    0 as sale_out_car_count,
                    0 as sold_out_car_count,
                    0 as car_count,
                    0 as sale_out_car_all_price,
                    0 as sale_out_car_all_service_charge,
                    0 as sale_out_period,
                    0 as car_all_count_today,
                    0 as car_all_count_yesterday,
                    0 as new_sale_out_car_count,
                    0 as new_sold_out_car_count
                FROM bigdata.renrenche_car_info_origin
                WHERE (dt = '${date}' AND appPackageName='${apk}'
                    AND car_uid IS NOT NULL AND car_uid != ''
                    AND car_source_id IS NOT NULL AND car_source_id != ''
                    AND car_name IS NOT NULL AND car_name != ''
                    AND car_price IS NOT NULL AND car_price > 0
                    AND service_charge IS NOT NULL AND service_charge > 0
                    AND car_state= 0)
                group by appPackageName
                UNION ALL
                SELECT appPackageName,
                    0 as on_sale_car_count,
                    0 as on_sale_period,
                    0 as on_sale_car_count_origin,
                    count(distinct car_uid) as sale_out_car_count,
                    0 as sold_out_car_count,
                    0 as car_count,
                    0 as sale_out_car_all_price,
                    0 as sale_out_car_all_service_charge,
                    0 as sale_out_period,
                    0 as car_all_count_today,
                    0 as car_all_count_yesterday,
                    0 as new_sale_out_car_count,
                    0 as new_sold_out_car_count
                FROM bigdata.renrenche_car_info_finance_snapshot
                WHERE (dt = '${date}' AND appPackageName='${apk}'
                    AND car_uid IS NOT NULL
                    AND car_uid != ''
                    AND car_state = 2)
                group by appPackageName
                UNION ALL
                SELECT appPackageName,
                    0 as on_sale_car_count,
                    0 as on_sale_period,
                    0 as on_sale_car_count_origin,
                    0 as sale_out_car_count,
                    count(distinct car_uid) as sold_out_car_count,
                    0 as car_count,
                    0 as sale_out_car_all_price,
                    0 as sale_out_car_all_service_charge,
                    0 as sale_out_period,
                    0 as car_all_count_today,
                    0 as car_all_count_yesterday,
                    0 as new_sale_out_car_count,
                    0 as new_sold_out_car_count
                FROM bigdata.renrenche_car_info_finance_snapshot
                WHERE (dt = '${date}' AND appPackageName='${apk}'
                    AND car_uid IS NOT NULL
                    AND car_uid != ''
                    AND car_state = 1)
                group by appPackageName
                UNION ALL
                select a1.appPackageName,
                    0 as on_sale_car_count,
                    0 as on_sale_period,
                    0 as on_sale_car_count_origin,
                    0 as sale_out_car_count,
                    0 as sold_out_car_count,
                    count(distinct a1.car_uid) as car_count,
                    sum(a1.car_price) as sale_out_car_all_price,
                    sum(a1.service_charge) as sale_out_car_all_service_charge,
                    sum(a1.period) as sale_out_period,
                    0 as car_all_count_today,
                    0 as car_all_count_yesterday,
                    0 as new_sale_out_car_count,
                    0 as new_sold_out_car_count
                from (
                    SELECT appPackageName,car_uid,car_price,service_charge,period
                    FROM bigdata.renrenche_car_info_finance_snapshot
                    WHERE (dt = '${date}' AND appPackageName='${apk}'
                        AND car_uid IS NOT NULL
                        AND car_uid != ''
                        AND car_price >0
                        AND service_charge>0
                        AND car_state = 2)
                ) as a1
                left join(
                    SELECT appPackageName,car_uid
                    FROM bigdata.renrenche_car_info_finance_snapshot
                    WHERE (dt = '${yesterday}' AND appPackageName='${apk}'
                        AND car_uid IS NOT NULL
                        AND car_uid != ''
                        AND car_state = 2)
                ) as a2
                on a1.car_uid=a2.car_uid and a1.appPackageName=a2.appPackageName
                where a2.car_uid is null
                group by a1.appPackageName
                UNION ALL
                SELECT appPackageName,
                    0 as on_sale_car_count,
                    0 as on_sale_period,
                    0 as on_sale_car_count_origin,
                    0 as sale_out_car_count,
                    0 as sold_out_car_count,
                    0 as car_count,
                    0 as sale_out_car_all_price,
                    0 as sale_out_car_all_service_charge,
                    0 as sale_out_period,
                    count(distinct car_uid) as car_all_count_today,
                    0 as car_all_count_yesterday,
                    0 as new_sale_out_car_count,
                    0 as new_sold_out_car_count
                FROM bigdata.renrenche_car_info_daily_snapshot
                WHERE (dt = '${date}' AND appPackageName='${apk}'
                    AND car_uid IS NOT NULL AND car_uid != '')
                group by appPackageName
                UNION ALL
                SELECT appPackageName,
                    0 as on_sale_car_count,
                    0 as on_sale_period,
                    0 as on_sale_car_count_origin,
                    0 as sale_out_car_count,
                    0 as sold_out_car_count,
                    0 as car_count,
                    0 as sale_out_car_all_price,
                    0 as sale_out_car_all_service_charge,
                    0 as sale_out_period,
                    0 as car_all_count_today,
                    count(distinct car_uid) as car_all_count_yesterday,
                    0 as new_sale_out_car_count,
                    0 as new_sold_out_car_count
                FROM bigdata.renrenche_car_info_daily_snapshot
                WHERE (dt = '${yesterday}' AND appPackageName='${apk}'
                    AND car_uid IS NOT NULL AND car_uid != '')
                group by appPackageName
                UNION ALL
                select a11.appPackageName,
                    0 as on_sale_car_count,
                    0 as on_sale_period,
                    0 as on_sale_car_count_origin,
                    0 as sale_out_car_count,
                    0 as sold_out_car_count,
                    0 as car_count,
                    0 as sale_out_car_all_price,
                    0 as sale_out_car_all_service_charge,
                    0 as sale_out_period,
                    0 as car_all_count_today,
                    0 as car_all_count_yesterday,
                    count(distinct a11.car_uid) as new_sale_out_car_count,
                    0 as new_sold_out_car_count
                from (
                    SELECT appPackageName,car_uid
                    FROM bigdata.renrenche_car_info_finance_snapshot
                    WHERE (dt = '${date}' AND appPackageName='${apk}'
                        AND car_uid IS NOT NULL
                        AND car_uid != ''
                        AND car_state = 2)
                ) as a11
                left join(
                    SELECT appPackageName,car_uid
                    FROM bigdata.renrenche_car_info_finance_snapshot
                    WHERE (dt = '${yesterday}' AND appPackageName='${apk}'
                        AND car_uid IS NOT NULL
                        AND car_uid != ''
                        AND car_state = 2)
                ) as a21
                on a11.car_uid=a21.car_uid and a11.appPackageName=a21.appPackageName
                where a21.car_uid is null
                group by a11.appPackageName
                UNION ALL
                select a12.appPackageName,
                    0 as on_sale_car_count,
                    0 as on_sale_period,
                    0 as on_sale_car_count_origin,
                    0 as sale_out_car_count,
                    0 as sold_out_car_count,
                    0 as car_count,
                    0 as sale_out_car_all_price,
                    0 as sale_out_car_all_service_charge,
                    0 as sale_out_period,
                    0 as car_all_count_today,
                    0 as car_all_count_yesterday,
                    0 as new_sale_out_car_count,
                    count(distinct a12.car_uid) as new_sold_out_car_count
                from (
                    SELECT appPackageName,car_uid
                    FROM bigdata.renrenche_car_info_finance_snapshot
                    WHERE (dt = '${date}' AND appPackageName='${apk}'
                        AND car_uid IS NOT NULL
                        AND car_uid != ''
                        AND car_state = 1)
                ) as a12
                left join(
                    SELECT appPackageName,car_uid
                    FROM bigdata.renrenche_car_info_finance_snapshot
                    WHERE (dt = '${yesterday}' AND appPackageName='${apk}'
                        AND car_uid IS NOT NULL
                        AND car_uid != ''
                        AND car_state = 1)
                ) as a22
                on a12.car_uid=a22.car_uid and a12.appPackageName=a22.appPackageName
                where a22.car_uid is null
                group by a12.appPackageName
      ) t1
group by t1.appPackageName;
"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.renrenche_platform_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/renrenche/snapshot/renrenche_platform_snapshot/dt=${date}

executeHiveCommand "
${delete_hive_partitions}
${hive_sql}
"

