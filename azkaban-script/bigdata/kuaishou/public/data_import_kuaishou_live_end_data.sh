#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

apk_name="com.smile.gifmaker"

    hive_sql="
    insert into bigdata.kuaishou_live_end_data partition(dt='${date}')
    SELECT a.data_generate_time, a.app_package_name,a.room_id,a.user_id,a.user_name,a.audience_count,a.like_count,
                a.live_duration
    FROM(
        select a4.data_generate_time, a4.app_package_name,a4.room_id,a4.user_id,a4.user_name,a4.audience_count,a4.like_count,
            a4.live_duration,a4.order_num,if(a4.time_length>0 and a4.time_length< 120000,-1,1) as tag
        from (
            select a1.data_generate_time, a1.app_package_name,a1.room_id,a1.user_id,a1.user_name,a1.audience_count,a1.like_count,
                a1.live_duration,a1.order_num,if(a3.data_generate_time is null,0,a3.data_generate_time)-a1.data_generate_time as time_length
            from (
                SELECT *, row_number() OVER (PARTITION BY user_id ORDER BY data_generate_time) AS order_num
                FROM bigdata.kuaishou_live_end_data_origin
                WHERE dt = '${date}' AND app_package_name='${apk_name}'
                     and user_id!='' and user_id is not null
            ) as a1
            left join (
                select a2.data_generate_time, a2.app_package_name,a2.room_id,a2.user_id,a2.user_name,a2.audience_count,a2.like_count,
                    a2.live_duration,(a2.order_num-1) as order_num
                from (
                    SELECT *, row_number() OVER (PARTITION BY user_id ORDER BY data_generate_time) AS order_num
                    FROM bigdata.kuaishou_live_end_data_origin
                    WHERE dt = '${date}' AND app_package_name='${apk_name}'
                         and user_id!='' and user_id is not null
                ) as a2
            ) as a3
            on a1.app_package_name=a3.app_package_name and a1.user_id=a3.user_id and a1.order_num=a3.order_num
        ) as a4
    ) as a
    WHERE a.tag=1
    "
    echo "${hive_sql}"
    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_live_end_data DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_live_end_data/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${hive_sql}
    "