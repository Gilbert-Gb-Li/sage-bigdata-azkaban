#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

apk_name="com.smile.gifmaker"

    hive_sql="
    insert into bigdata.kuaishou_live_danmu_gift_data partition(dt='${date}')
    SELECT a.data_generate_time, a.app_package_name, a.user_id, a.user_name, a.audience_id
        , a.audience_name, a.danmu_gift_id,null as danmu_gift_name, a.danmu_gift_count,
        if(b.gift_unit_val is not null,b.gift_unit_val,-1) AS gift_val
    FROM (
        SELECT data_generate_time, app_package_name, user_id, user_name, audience_id
            , audience_name, danmu_gift_id, danmu_gift_count
        FROM bigdata.kuaishou_live_danmu_data_origin_orc
        WHERE (dt = '${date}'
            AND app_package_name = '${apk_name}'
            AND user_id != ''
            AND user_id IS NOT NULL
            AND audience_id != ''
            AND audience_id IS NOT NULL
            AND danmu_gift_id != ''
            AND danmu_gift_id IS NOT NULL
            AND danmu_gift_count >= 1)
    ) a
        LEFT JOIN (
            SELECT r.app_package_name, r.gift_id, AVG(r.gift_unit_val) AS gift_unit_val
            FROM (
                SELECT *, row_number() OVER (PARTITION BY gift_id ORDER BY data_generate_time DESC) AS order_num
                FROM bigdata.kuaishou_live_gift_data_daily_snapshot
                WHERE dt = '${date}'
            ) r
            WHERE r.order_num = 1
            GROUP BY r.app_package_name, r.gift_id
        ) b
        ON a.app_package_name = b.app_package_name
            AND a.danmu_gift_id = b.gift_id
    ;
    "
    echo "${hive_sql}"
    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_live_danmu_gift_data DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_live_danmu_gift_data/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${hive_sql}
    "