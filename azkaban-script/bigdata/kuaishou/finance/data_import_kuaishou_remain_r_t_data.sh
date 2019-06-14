#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
stat_date=`date -d "$date" +%Y%m%d`
stat_month=`date -d "$date" +%Y%m`
date_add_1=`date -d "+1 day $date" +%Y-%m-%d`
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
date_reduce_1_1=`date -d "-1 day $date" +%Y%m%d`
date_reduce_6=`date -d "-6 day $date" +%Y-%m-%d`
date_reduce_7=`date -d "-7 day $date" +%Y-%m-%d`
date_reduce_7_1=`date -d "-7 day $date" +%Y%m%d`
date_reduce_13=`date -d "-13 day $date" +%Y-%m-%d`
date_reduce_13_1=`date -d "-13 day $date" +%Y%m%d`
week=`date -d "${date_add_1}" +%w`
echo "周：${week}"
month=`date -d "${date}" +%Y%m`
echo "月格式1：${month}"
month1=`date -d "${date}" +%Y-%m`
echo "月格式2：${month1}"
month1_01=`date -d "${month1}-01" +%Y-%m-%d`
month1_01_1=`date -d "${month1}-01" +%Y%m%d`
echo "月第一天：${month1_01}"
month1_01_reduce_1=`date -d "-1 day $month1_01" +%Y-%m-%d`
month1_01_reduce_1_1=`date -d "-1 day $month1_01" +%Y%m%d`
echo "上月最后一天：${month1_01_reduce_1}"
month2=`date -d "${month1_01_reduce_1}" +%Y-%m`
echo "上月格式2：${month2}"
month2_01=`date -d "${month2}-01" +%Y-%m-%d`
month2_01_1=`date -d "${month2}-01" +%Y%m%d`
echo "上月第一天：${month2_01}"
day=`date -d "${date_add_1}" +%d`


    echo "################# 头部集数据日留存  ########################"
    tmp_kuaishou_t_user_day_remain="
    insert into bigdata.kuaishou_remain_r_t_data partition(dt='${date}')
    SELECT 'kuaishou' AS meta_app_name, 'kuaishou_remain_r_t_data' AS meta_table_name, '${stat_month}' as stat_month, 'day' AS remain_type, 'T_USER' AS set_type
        , '${date_reduce_1_1}' AS extract_date, s1.comment_count_remain, s1.comment_count_origin, s2.video_count_remain, s2.video_count_origin
    FROM (
        SELECT t1.remain_count AS comment_count_remain, t2.remain_count AS comment_count_origin
        FROM (
            SELECT COUNT(DISTINCT a.user_id) AS remain_count
            FROM (
                SELECT user_id
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt = '${date}'
                    AND new_user_comment_count > 0
                    AND set_type = 'T_USER')
            ) a
                JOIN (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt = '${yesterday}'
                        AND new_user_comment_count > 0
                        AND set_type = 'T_USER')
                ) b
                ON a.user_id = b.user_id
        ) t1
            LEFT JOIN (
                SELECT COUNT(DISTINCT user_id) AS remain_count
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt = '${yesterday}'
                    AND new_user_comment_count > 0
                    AND set_type = 'T_USER')
            ) t2
    ) s1
        LEFT JOIN (
            SELECT t1.remain_count AS video_count_remain, t2.remain_count AS video_count_origin
            FROM (
                SELECT COUNT(DISTINCT a.user_id) AS remain_count
                FROM (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt = '${date}'
                        AND new_short_video_count > 0
                        AND set_type = 'T_USER')
                ) a
                    JOIN (
                        SELECT user_id
                        FROM bigdata.kuaishou_user_r_t_data
                        WHERE (dt = '${yesterday}'
                            AND new_short_video_count > 0
                            AND set_type = 'T_USER')
                    ) b
                    ON a.user_id = b.user_id
            ) t1
                LEFT JOIN (
                    SELECT COUNT(DISTINCT user_id) AS remain_count
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt = '${yesterday}'
                        AND new_short_video_count > 0
                        AND set_type = 'T_USER')
                ) t2
        ) s2;
    "

    echo "################# 抽样集数据日留存  ########################"
    tmp_kuaishou_r_user_day_remain="
    insert into bigdata.kuaishou_remain_r_t_data partition(dt='${date}')
    SELECT 'kuaishou' AS meta_app_name, 'kuaishou_remain_r_t_data' AS meta_table_name, '${stat_month}' as stat_month,'day' AS remain_type,'R_USER' AS set_type
        ,'${month1_01_1}' AS extract_date, s1.comment_count_remain, s1.comment_count_origin, s2.video_count_remain, s2.video_count_origin
    FROM (
        SELECT t1.remain_count AS comment_count_remain, t2.remain_count AS comment_count_origin
        FROM (
            SELECT COUNT(DISTINCT a.user_id) AS remain_count
            FROM (
                SELECT user_id
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt = '${date}'
                    AND new_user_comment_count > 0
                    AND set_type = 'R_USER'
                    AND extract_date = '${month1_01_1}')
            ) a
                JOIN (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt = '${yesterday}'
                        AND new_user_comment_count > 0
                        AND set_type = 'R_USER'
                        AND extract_date = '${month1_01_1}')
                ) b
                ON a.user_id = b.user_id
        ) t1
            LEFT JOIN (
                SELECT COUNT(DISTINCT user_id) AS remain_count
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt = '${yesterday}'
                    AND new_user_comment_count > 0
                    AND set_type = 'R_USER'
                    AND extract_date = '${month1_01_1}')
            ) t2
    ) s1
        LEFT JOIN (
            SELECT t1.remain_count AS video_count_remain, t2.remain_count AS video_count_origin
            FROM (
                SELECT COUNT(DISTINCT a.user_id) AS remain_count
                FROM (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt = '${date}'
                        AND new_short_video_count > 0
                        AND set_type = 'R_USER'
                        AND extract_date = '${month1_01_1}')
                ) a
                    JOIN (
                        SELECT user_id
                        FROM bigdata.kuaishou_user_r_t_data
                        WHERE (dt = '${yesterday}'
                            AND new_short_video_count > 0
                            AND set_type = 'R_USER'
                            AND extract_date = '${month1_01_1}')
                    ) b
                    ON a.user_id = b.user_id
            ) t1
                LEFT JOIN (
                    SELECT COUNT(DISTINCT user_id) AS remain_count
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt = '${yesterday}'
                        AND new_short_video_count > 0
                        AND set_type = 'R_USER'
                        AND extract_date = '${month1_01_1}')
                ) t2
        ) s2;
    "

    echo "################# 头部集数据周留存  ########################"
    tmp_kuaishou_t_user_week_remain="
    insert into bigdata.kuaishou_remain_r_t_data partition(dt='${date}')
    SELECT 'kuaishou' AS meta_app_name, 'kuaishou_remain_r_t_data' AS meta_table_name, '${stat_month}' as stat_month, 'week' AS remain_type, 'T_USER' AS set_type
        ,concat('${date_reduce_13_1}','-','${date_reduce_7_1}') AS extract_date,
        s1.comment_count_remain, s1.comment_count_origin, s2.video_count_remain, s2.video_count_origin
    FROM (
        SELECT t1.remain_count AS comment_count_remain, t2.remain_count AS comment_count_origin
        FROM (
            SELECT COUNT(DISTINCT a.user_id) AS remain_count
            FROM (
                SELECT user_id
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt >= '${date_reduce_6}'
                    AND dt <= '${date}'
                    AND new_user_comment_count > 0
                    AND set_type = 'T_USER')
            ) a
                JOIN (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${date_reduce_13}'
                        AND dt <= '${date_reduce_7}'
                        AND new_user_comment_count > 0
                        AND set_type = 'T_USER')
                ) b
                ON a.user_id = b.user_id
        ) t1
            LEFT JOIN (
                SELECT COUNT(DISTINCT user_id) AS remain_count
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt >= '${date_reduce_13}'
                    AND dt <= '${date_reduce_7}'
                    AND new_user_comment_count > 0
                    AND set_type = 'T_USER')
            ) t2
    ) s1
        LEFT JOIN (
            SELECT t1.remain_count AS video_count_remain, t2.remain_count AS video_count_origin
            FROM (
                SELECT COUNT(DISTINCT a.user_id) AS remain_count
                FROM (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${date_reduce_6}'
                        AND dt <= '${date}'
                        AND new_short_video_count > 0
                        AND set_type = 'T_USER')
                ) a
                    JOIN (
                        SELECT user_id
                        FROM bigdata.kuaishou_user_r_t_data
                        WHERE (dt >= '${date_reduce_13}'
                            AND dt <= '${date_reduce_7}'
                            AND new_short_video_count > 0
                            AND set_type = 'T_USER')
                    ) b
                    ON a.user_id = b.user_id
            ) t1
                LEFT JOIN (
                    SELECT COUNT(DISTINCT user_id) AS remain_count
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${date_reduce_13}'
                        AND dt <= '${date_reduce_7}'
                        AND new_short_video_count > 0
                        AND set_type = 'T_USER')
                ) t2
        ) s2;
    "

    echo "################# 抽样集数据周留存  ########################"
    tmp_kuaishou_r_user_week_remain="
    insert into bigdata.kuaishou_remain_r_t_data partition(dt='${date}')
    SELECT 'kuaishou' AS meta_app_name, 'kuaishou_remain_r_t_data' AS meta_table_name, '${stat_month}' as stat_month, 'week' AS remain_type, 'R_USER' AS set_type
        , '${month1_01_1}' AS extract_date, s1.comment_count_remain, s1.comment_count_origin, s2.video_count_remain, s2.video_count_origin
    FROM (
        SELECT t1.remain_count AS comment_count_remain, t2.remain_count AS comment_count_origin
        FROM (
            SELECT COUNT(DISTINCT a.user_id) AS remain_count
            FROM (
                SELECT user_id
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt >= '${date_reduce_6}'
                    AND dt <= '${date}'
                    AND new_user_comment_count > 0
                    AND set_type = 'R_USER'
                    AND extract_date = '${month1_01_1}')
            ) a
                JOIN (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${date_reduce_13}'
                        AND dt <= '${date_reduce_7}'
                        AND new_user_comment_count > 0
                        AND set_type = 'R_USER'
                        AND extract_date = '${month1_01_1}')
                ) b
                ON a.user_id = b.user_id
        ) t1
            LEFT JOIN (
                SELECT COUNT(DISTINCT user_id) AS remain_count
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt >= '${date_reduce_13}'
                    AND dt <= '${date_reduce_7}'
                    AND new_user_comment_count > 0
                    AND set_type = 'R_USER'
                    AND extract_date = '${month1_01_1}')
            ) t2
    ) s1
        LEFT JOIN (
            SELECT t1.remain_count AS video_count_remain, t2.remain_count AS video_count_origin
            FROM (
                SELECT COUNT(DISTINCT a.user_id) AS remain_count
                FROM (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${date_reduce_6}'
                        AND dt <= '${date}'
                        AND new_short_video_count > 0
                        AND set_type = 'R_USER'
                        AND extract_date = '${month1_01_1}')
                ) a
                    JOIN (
                        SELECT user_id
                        FROM bigdata.kuaishou_user_r_t_data
                        WHERE (dt >= '${date_reduce_13}'
                            AND dt <= '${date_reduce_7}'
                            AND new_short_video_count > 0
                            AND set_type = 'R_USER'
                            AND extract_date = '${month1_01_1}')
                    ) b
                    ON a.user_id = b.user_id
            ) t1
                LEFT JOIN (
                    SELECT COUNT(DISTINCT user_id) AS remain_count
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${date_reduce_13}'
                        AND dt <= '${date_reduce_7}'
                        AND new_short_video_count > 0
                        AND set_type = 'R_USER'
                        AND extract_date = '${month1_01_1}')
                ) t2
        ) s2;
    "

    echo "######################计算上月抽样集用户的七日留存#############################"
    tmp_kuaishou_r_user_week_remain_old="
    insert into bigdata.kuaishou_remain_r_t_data partition(dt='${date}')
    SELECT 'kuaishou' AS meta_app_name, 'kuaishou_remain_r_t_data' AS meta_table_name, '${stat_month}' as stat_month, 'week' AS remain_type, 'R_USER' AS set_type
        , '${month2_01_1}' AS extract_date, s1.comment_count_remain, s1.comment_count_origin, s2.video_count_remain, s2.video_count_origin
    FROM (
        SELECT t1.remain_count AS comment_count_remain, t2.remain_count AS comment_count_origin
        FROM (
            SELECT COUNT(DISTINCT a.user_id) AS remain_count
            FROM (
                SELECT user_id
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt >= '${date_reduce_6}'
                    AND dt <= '${date}'
                    AND new_user_comment_count > 0
                    AND set_type = 'R_USER'
                    AND extract_date = '${month2_01_1}')
            ) a
                JOIN (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${date_reduce_13}'
                        AND dt <= '${date_reduce_7}'
                        AND new_user_comment_count > 0
                        AND set_type = 'R_USER'
                        AND extract_date = '${month2_01_1}')
                ) b
                ON a.user_id = b.user_id
        ) t1
            LEFT JOIN (
                SELECT COUNT(DISTINCT user_id) AS remain_count
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt >= '${date_reduce_13}'
                    AND dt <= '${date_reduce_7}'
                    AND new_user_comment_count > 0
                    AND set_type = 'R_USER'
                    AND extract_date = '${month2_01_1}')
            ) t2
    ) s1
        LEFT JOIN (
            SELECT t1.remain_count AS video_count_remain, t2.remain_count AS video_count_origin
            FROM (
                SELECT COUNT(DISTINCT a.user_id) AS remain_count
                FROM (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${date_reduce_6}'
                        AND dt <= '${date}'
                        AND new_short_video_count > 0
                        AND set_type = 'R_USER'
                        AND extract_date = '${month2_01_1}')
                ) a
                    JOIN (
                        SELECT user_id
                        FROM bigdata.kuaishou_user_r_t_data
                        WHERE (dt >= '${date_reduce_13}'
                            AND dt <= '${date_reduce_7}'
                            AND new_short_video_count > 0
                            AND set_type = 'R_USER'
                            AND extract_date = '${month2_01_1}')
                    ) b
                    ON a.user_id = b.user_id
            ) t1
                LEFT JOIN (
                    SELECT COUNT(DISTINCT user_id) AS remain_count
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${date_reduce_13}'
                        AND dt <= '${date_reduce_7}'
                        AND new_short_video_count > 0
                        AND set_type = 'R_USER'
                        AND extract_date = '${month2_01_1}')
                ) t2
        ) s2;
    "

    echo "######################头部集数据的月留存#############################"
    tmp_kuaishou_t_user_month_remain="
    insert into bigdata.kuaishou_remain_r_t_data partition(dt='${date}')
    SELECT 'kuaishou' AS meta_app_name, 'kuaishou_remain_r_t_data' AS meta_table_name, '${stat_month}' as stat_month, 'month' AS remain_type, 'T_USER' AS set_type
        , concat('${month2_01_1}','-','${month1_01_reduce_1_1}') AS extract_date
        , s1.comment_count_remain, s1.comment_count_origin, s2.video_count_remain, s2.video_count_origin
    FROM (
        SELECT t1.remain_count AS comment_count_remain, t2.remain_count AS comment_count_origin
        FROM (
            SELECT COUNT(DISTINCT a.user_id) AS remain_count
            FROM (
                SELECT user_id
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt >= '${month1_01}'
                    AND dt <= '${date}'
                    AND new_user_comment_count > 0
                    AND set_type = 'T_USER')
            ) a
                JOIN (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${month2_01}'
                        AND dt <= '${month1_01_reduce_1}'
                        AND new_user_comment_count > 0
                        AND set_type = 'T_USER')
                ) b
                ON a.user_id = b.user_id
        ) t1
            LEFT JOIN (
                SELECT COUNT(DISTINCT user_id) AS remain_count
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt >= '${month2_01}'
                    AND dt <= '${month1_01_reduce_1}'
                    AND new_user_comment_count > 0
                    AND set_type = 'T_USER')
            ) t2
    ) s1
        LEFT JOIN (
            SELECT t1.remain_count AS video_count_remain, t2.remain_count AS video_count_origin
            FROM (
                SELECT COUNT(DISTINCT a.user_id) AS remain_count
                FROM (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${month1_01}'
                        AND dt <= '${date}'
                        AND new_short_video_count > 0
                        AND set_type = 'T_USER')
                ) a
                    JOIN (
                        SELECT user_id
                        FROM bigdata.kuaishou_user_r_t_data
                        WHERE (dt >= '${month2_01}'
                            AND dt <= '${month1_01_reduce_1}'
                            AND new_short_video_count > 0
                            AND set_type = 'T_USER')
                    ) b
                    ON a.user_id = b.user_id
            ) t1
                LEFT JOIN (
                    SELECT COUNT(DISTINCT user_id) AS remain_count
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${month2_01}'
                        AND dt <= '${month1_01_reduce_1}'
                        AND new_short_video_count > 0
                        AND set_type = 'T_USER')
                ) t2
        ) s2;
    "


    echo "###################### 抽样集数据的月留存#############################"
    tmp_kuaishou_r_user_month_remain="
    insert into bigdata.kuaishou_remain_r_t_data partition(dt='${date}')
    SELECT 'kuaishou' AS meta_app_name, 'kuaishou_remain_r_t_data' AS meta_table_name, '${stat_month}' as stat_month, 'month' AS remain_type, 'R_USER' AS set_type
        , '${month2_01_1}' AS extract_date, s1.comment_count_remain, s1.comment_count_origin, s2.video_count_remain, s2.video_count_origin
    FROM (
        SELECT t1.remain_count AS comment_count_remain, t2.remain_count AS comment_count_origin
        FROM (
            SELECT COUNT(DISTINCT a.user_id) AS remain_count
            FROM (
                SELECT user_id
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt >= '${month1_01}'
                    AND dt <= '${date}'
                    AND new_user_comment_count > 0
                    AND set_type = 'R_USER'
                    AND extract_date = '${month2_01_1}')
            ) a
                JOIN (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${month2_01}'
                        AND dt <= '${month1_01_reduce_1}'
                        AND new_user_comment_count > 0
                        AND set_type = 'R_USER'
                        AND extract_date = '${month2_01_1}')
                ) b
                ON a.user_id = b.user_id
        ) t1
            LEFT JOIN (
                SELECT COUNT(DISTINCT user_id) AS remain_count
                FROM bigdata.kuaishou_user_r_t_data
                WHERE (dt >= '${month2_01}'
                    AND dt <= '${month1_01_reduce_1}'
                    AND new_user_comment_count > 0
                    AND set_type = 'R_USER'
                    AND extract_date = '${month2_01_1}')
            ) t2
    ) s1
        LEFT JOIN (
            SELECT t1.remain_count AS video_count_remain, t2.remain_count AS video_count_origin
            FROM (
                SELECT COUNT(DISTINCT a.user_id) AS remain_count
                FROM (
                    SELECT user_id
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${month1_01}'
                        AND dt <= '${date}'
                        AND new_short_video_count > 0
                        AND set_type = 'R_USER'
                        AND extract_date = '${month2_01_1}')
                ) a
                    JOIN (
                        SELECT user_id
                        FROM bigdata.kuaishou_user_r_t_data
                        WHERE (dt >= '${month2_01}'
                            AND dt <= '${month1_01_reduce_1}'
                            AND new_short_video_count > 0
                            AND set_type = 'R_USER'
                            AND extract_date = '${month2_01_1}')
                    ) b
                    ON a.user_id = b.user_id
            ) t1
                LEFT JOIN (
                    SELECT COUNT(DISTINCT user_id) AS remain_count
                    FROM bigdata.kuaishou_user_r_t_data
                    WHERE (dt >= '${month2_01}'
                        AND dt <= '${month1_01_reduce_1}'
                        AND new_short_video_count > 0
                        AND set_type = 'R_USER'
                        AND extract_date = '${month2_01_1}')
                ) t2
        ) s2;
    "


    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_remain_r_t_data DROP IF EXISTS PARTITION (dt='${date}');
    "
    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_remain_r_t_data/dt=${date}

    echo "${tmp_kuaishou_t_user_day_remain}"
    echo "${tmp_kuaishou_r_user_day_remain}"

    executeHiveCommand "
    ${delete_hive_partitions}
    ${tmp_kuaishou_t_user_day_remain}
    ${tmp_kuaishou_r_user_day_remain}
    "

    if [ ${week} -eq '1' ]
        then
            echo "${tmp_kuaishou_t_user_week_remain}"
            echo "${tmp_kuaishou_r_user_week_remain}"
            echo "${tmp_kuaishou_r_user_week_remain_old}"
            executeHiveCommand "
            ${tmp_kuaishou_t_user_week_remain}
            ${tmp_kuaishou_r_user_week_remain}
            ${tmp_kuaishou_r_user_week_remain_old}
            "

        else
            echo "不是自然周的最后一天，不进行计算！"
    fi

    if [ ${day} -eq '01' ]
        then
            echo "${tmp_kuaishou_t_user_month_remain}"
            echo "${tmp_kuaishou_r_user_month_remain}"
            executeHiveCommand "
            ${tmp_kuaishou_t_user_month_remain}
            ${tmp_kuaishou_r_user_month_remain}
            "

        else
            echo "不是自然月的最后一天，不进行计算！"
    fi







