#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

apk_name="com.smile.gifmaker"

hive_sql="
    insert into bigdata.kuaishou_user_commodity_info_daily_snapshot partition(dt='${date}')
    SELECT data_generate_time,app_package_name,user_id,commodity_name,commodity_price,commodity_source,commodity_sell_num
    FROM (
        SELECT *, row_number() OVER (PARTITION BY user_id,commodity_name ORDER BY data_generate_time DESC) AS order_num
        FROM (
            SELECT data_generate_time,app_package_name,user_id,commodity_name,commodity_price,commodity_source,commodity_sell_num
            FROM bigdata.kuaishou_user_commodity_info_data_origin_orc
            WHERE dt = '${date}' and app_package_name='${apk_name}'
                  and user_id!='' and user_id is not null
                  and commodity_name!='' and commodity_name is not null
            UNION ALL
            SELECT data_generate_time,app_package_name,user_id,commodity_name,commodity_price,commodity_source,commodity_sell_num
            FROM bigdata.kuaishou_user_commodity_info_daily_snapshot
            WHERE dt = '${yesterday}'
        ) p
    ) t
    WHERE t.order_num = 1;
    "
    echo "${hive_sql}"
    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_user_commodity_info_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_user_commodity_info_daily_snapshot/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${hive_sql}
    "
