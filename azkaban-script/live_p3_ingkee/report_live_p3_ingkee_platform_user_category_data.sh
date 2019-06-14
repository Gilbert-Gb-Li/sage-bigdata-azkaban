#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p3_util.sh

day=$1
yesterday=`date -d "-1 day $day" +%Y-%m-%d`
week=`date -d "-6 day $day" +%Y-%m-%d`
month=`date -d "-29 day $day" +%Y-%m-%d`
stat_date=`date -d "$day" +%Y%m%d`
yearMonth=`date -d "$day" +%Y%m`
tmp_dir=/tmp/ingkee

echo '################# 映客相关统计信息 start   ########################'

    echo '################平台全量用户表####################'
    tmp_app_user_info_all_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_info_all_p3 AS
    SELECT appPackageName,
         user_id,
         user_sex as gender,
         user_hometown as hometown,
         user_constellation as constellation,
         data_generate_time
    FROM live_p3.tbl_ex_live_user_info_daily_snapshot
    WHERE dt = '${day}' AND appPackageName in ('${finance_live_app}')
    UNION
    SELECT appPackageName,
         audience_id as user_id,
         audience_sex as gender,
         audience_hometown as hometown,
         null as constellation,
         data_generate_time
    FROM live_p3.tbl_ex_live_viewer_info_daily_snapshot
    WHERE dt = '${day}' AND appPackageName in ('${finance_live_app}');
    "

    echo '################平台全量用户性别表1####################'
    tmp_app_user_all_gender_1_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_gender_1_p3 AS
    SELECT
        appPackageName,
        gender,
        count(distinct user_id) AS num
    FROM
        (
            SELECT
                appPackageName,
                user_id,
                data_generate_time,
            CASE gender WHEN 0 THEN '女' WHEN 1 THEN '男' ELSE '其他' END AS gender
            FROM
                default.tmp_app_user_info_all_p3
        ) AS a
    WHERE
        EXISTS (
            SELECT
                1
            FROM
                (
                    SELECT
                        MAX(data_generate_time) data_generate_time,
                        appPackageName,
                        user_id
                    FROM
                        default.tmp_app_user_info_all_p3
                    GROUP BY
                        appPackageName,
                        user_id
                ) b
            WHERE
                a.data_generate_time = b.data_generate_time
            AND a.user_id = b.user_id
            AND a.appPackageName = b.appPackageName
        )
    GROUP BY
        appPackageName,
        gender;
    "

    echo '################平台全量用户性别表2####################'
    tmp_app_user_all_gender_2_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_gender_2_p3 AS
    SELECT
        a.appPackageName,
        a.gender,
        a.num,
        b.gender_num_all
    FROM
        default.tmp_app_user_all_gender_1_p3 as a
    FULL JOIN (
        SELECT
            appPackageName,
            sum(num) as gender_num_all
        FROM
            default.tmp_app_user_all_gender_1_p3
        GROUP BY
            appPackageName
    ) as b ON a.appPackageName = b.appPackageName;
    "

    echo '################平台全量用户性别比例####################'
    tmp_app_user_all_gender_percent_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_gender_percent_p3 AS
    SELECT
        appPackageName,
        gender,
        num,
        num/gender_num_all as gender_percent
    FROM
        default.tmp_app_user_all_gender_2_p3;
    "

    echo '################平台全量用户地区表1####################'
    tmp_app_user_all_hometown_1_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_hometown_1_p3 AS
    SELECT
        appPackageName,
        hometown,
        sum(num) as num
    FROM
        (
            SELECT
                t3.appPackageName,
                COALESCE (
                    t3.province,
                    t4.province,
                    '其它'
                ) AS hometown,
                num
            FROM
                (
                    SELECT
                        t1.appPackageName,
                        t1.num,
                        substr(t1.hometown, 1, 2) AS hometown,
                        t2.province
                    FROM
                        (
                            SELECT
                                appPackageName,
                                substr(hometown, 1, 3) AS hometown,
                                count(distinct user_id) AS num
                            FROM
                                default.tmp_app_user_info_all_p3 a
                            WHERE
                                EXISTS (
                                    SELECT
                                        1
                                    FROM
                                        (
                                            SELECT
                                                MAX(data_generate_time) data_generate_time,
                                                appPackageName,
                                                user_id
                                            FROM
                                                default.tmp_app_user_info_all_p3
                                            GROUP BY
                                                appPackageName,
                                                user_id
                                        ) b
                                    WHERE
                                        a.data_generate_time = b.data_generate_time
                                    AND a.user_id = b.user_id
                                    AND a.appPackageName = b.appPackageName
                                )
                            GROUP BY
                                appPackageName,
                                hometown
                        ) AS t1
                    LEFT JOIN live_p2.tbl_util_location AS t2 ON t1.hometown = t2.city
                ) AS t3
            LEFT JOIN live_p2.tbl_util_location AS t4 ON t3.hometown = t4.city
        ) as result GROUP BY appPackageName,hometown;
    "

    echo '################平台全量用户地区表2####################'
    tmp_app_user_all_hometown_2_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_hometown_2_p3 AS
    SELECT
        a.appPackageName,
        a.hometown,
        a.num,
        b.hometown_num_all
    FROM
        default.tmp_app_user_all_hometown_1_p3 as a
    FULL JOIN (
        SELECT
            appPackageName,
            sum(num) as hometown_num_all
        FROM
            default.tmp_app_user_all_hometown_1_p3
        GROUP BY
            appPackageName
    ) as b ON a.appPackageName = b.appPackageName;
    "
    echo '################平台全量用户地区比例####################'
    tmp_app_user_all_hometown_percent_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_hometown_percent_p3 AS
    SELECT
        appPackageName,
        hometown,
        num,
        num/hometown_num_all as hometown_percent
    FROM
        default.tmp_app_user_all_hometown_2_p3;
    "


    echo '################平台全量用户星座表1####################'
    tmp_app_user_all_constellation_1_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_constellation_1_p3 AS
    select b3.appPackageName,b3.constellation,
        sum(b3.num) as num
    from(
        SELECT
            appPackageName,
            if(constellation is null or constellation='','其它',constellation) as constellation,
            count(distinct user_id) as num
        FROM
            default.tmp_app_user_info_all_p3 a
        WHERE
            EXISTS (
                SELECT
                    1
                FROM
                    (
                        SELECT
                            MAX(data_generate_time) data_generate_time,
                            appPackageName,
                            user_id
                        FROM
                            default.tmp_app_user_info_all_p3
                        GROUP BY
                            appPackageName,
                            user_id
                    ) b
                WHERE
                    a.data_generate_time = b.data_generate_time
                AND a.user_id = b.user_id
                AND a.appPackageName = b.appPackageName
            ) GROUP BY appPackageName,constellation
     ) as b3
     GROUP BY b3.appPackageName,b3.constellation
    ;
    "

    echo '################平台全量用户星座表2####################'
    tmp_app_user_all_constellation_2_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_constellation_2_p3 AS
    SELECT
        a.appPackageName,
        a.constellation,
        a.num,
        b.constellation_num_all
    FROM
        default.tmp_app_user_all_constellation_1_p3 as a
    FULL JOIN (
        SELECT
            appPackageName,
            sum(num) as constellation_num_all
        FROM
            default.tmp_app_user_all_constellation_1_p3
        GROUP BY
            appPackageName
    ) as b ON a.appPackageName = b.appPackageName;
    "

    echo '################平台全量用户星座比例####################'
    tmp_app_user_all_constellation_percent_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_constellation_percent_p3 AS
    SELECT
        appPackageName,
        constellation,
        num,
        num/constellation_num_all as constellation_percent
    FROM
        default.tmp_app_user_all_constellation_2_p3;
    "

    echo '################平台每日开播主播性别表1####################'
    tmp_app_user_live_gender_1_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_gender_1_p3 AS
    SELECT a.appPackageName, a.gender, COUNT(DISTINCT user_id) AS num
        FROM (
            SELECT a2.appPackageName
                , CASE if(a2.user_sex IS NOT NULL
                        AND a2.user_sex != '-1', a2.user_sex, c.user_sex)
                    WHEN 0 THEN '女'
                    WHEN 1 THEN '男'
                    ELSE '其他'
                END AS gender, a2.user_id
            FROM (
                SELECT appPackageName, user_id, user_sex
                FROM ias_p3.tbl_ex_live_user_info_data_origin_orc a
                WHERE a.dt =  '${day}'
                    AND EXISTS (
                        SELECT 1
                        FROM (
                            SELECT MAX(data_generate_time) AS data_generate_time, appPackageName, user_id
                            FROM ias_p3.tbl_ex_live_user_info_data_origin_orc
                            WHERE (dt =  '${day}'
                                AND user_id IS NOT NULL
                                AND user_id != ''
                                AND appPackageName IN ('${finance_live_app}')
                                AND is_live = 1)
                            GROUP BY appPackageName, user_id
                        ) b
                        WHERE (a.data_generate_time = b.data_generate_time
                            AND a.user_id = b.user_id
                            AND a.appPackageName = b.appPackageName)
                    )
            ) a2
                LEFT JOIN (
                    SELECT appPackageName, user_id, user_sex
                    FROM live_p3.tbl_ex_live_user_info_daily_snapshot
                    WHERE dt =  '${day}'
                ) c
                ON a2.user_id = c.user_id
                    AND a2.appPackageName = c.appPackageName
        ) a
        GROUP BY a.appPackageName, a.gender;
        "

    echo '################平台每日开播主播性别表2####################'
    tmp_app_user_live_gender_2_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_gender_2_p3 AS
    SELECT
        a.appPackageName,
        a.gender,
        a.num,
        b.gender_num_all
    FROM
        default.tmp_app_user_live_gender_1_p3 as a
    FULL JOIN (
        SELECT
            appPackageName,
            sum(num) as gender_num_all
        FROM
            default.tmp_app_user_live_gender_1_p3
        GROUP BY
            appPackageName
    ) as b ON a.appPackageName = b.appPackageName;
    "

    echo '################平台每日开播主播性别比例####################'
    tmp_app_user_live_gender_percent_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_gender_percent_p3 AS
    SELECT
        appPackageName,
        gender,
        num,
        num/gender_num_all as gender_percent
    FROM
        default.tmp_app_user_live_gender_2_p3;
    "

echo '################平台每日开播主播地区表1####################'
    tmp_app_user_live_hometown_1_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_hometown_1_p3 AS
    SELECT
        appPackageName,
        hometown,
        sum(num) AS num
    FROM
        (
            SELECT
                t3.appPackageName,
                COALESCE (
                    t3.province,
                    t4.province,
                    '其它'
                ) AS hometown,
                num
            FROM
                (
                    SELECT
                        t1.appPackageName,
                        t1.num,
                        substr(t1.hometown, 1, 2) AS hometown,
                        t2.province
                    FROM
                        (
                            SELECT
                                appPackageName,
                                substr(user_hometown, 1, 3) AS hometown,
                                count(distinct user_id) AS num
                            FROM
                                ias_p3.tbl_ex_live_user_info_data_origin_orc a
                            WHERE
                                a.dt = '${day}'
                                AND
                                EXISTS (
                                    SELECT
                                        1
                                    FROM
                                        (
                                            SELECT
                                                MAX(data_generate_time) data_generate_time,
                                                appPackageName,
                                                user_id
                                            FROM
                                                ias_p3.tbl_ex_live_user_info_data_origin_orc
                                            WHERE dt = '${day}'
                                            AND appPackageName in ('${finance_live_app}')
                                            AND is_live = 1
                                            GROUP BY
                                                appPackageName,
                                                user_id
                                        ) b
                                    WHERE
                                        a.data_generate_time = b.data_generate_time
                                    AND a.user_id = b.user_id
                                    AND a.appPackageName = b.appPackageName
                                )
                            GROUP BY
                                appPackageName,
                                user_hometown
                        ) AS t1
                    LEFT JOIN live_p2.tbl_util_location AS t2 ON t1.hometown = t2.city
                ) AS t3
            LEFT JOIN live_p2.tbl_util_location AS t4 ON t3.hometown = t4.city
        ) AS result
    GROUP BY
        appPackageName,
        hometown;
     "

    echo '################平台每日开播主播地区表2####################'
    tmp_app_user_live_hometown_2_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_hometown_2_p3 AS
    SELECT
        a.appPackageName,
        a.hometown,
        a.num,
        b.hometown_num_all
    FROM
        default.tmp_app_user_live_hometown_1_p3 as a
    FULL JOIN (
        SELECT
            appPackageName,
            sum(num) as hometown_num_all
        FROM
            default.tmp_app_user_live_hometown_1_p3
        GROUP BY
            appPackageName
    ) as b ON a.appPackageName = b.appPackageName;
    "

    echo '################平台每日开播主播地区比例####################'
    tmp_app_user_live_hometown_percent_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_hometown_percent_p3 AS
    SELECT
        appPackageName,
        hometown,
        num,
        num/hometown_num_all as hometown_percent
    FROM
        default.tmp_app_user_live_hometown_2_p3;
    "

    echo '################平台每日开播主播星座表1####################'
    tmp_app_user_live_constellation_1_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_constellation_1_p3 AS
    SELECT
        appPackageName,
        if(user_constellation is null or user_constellation='','其它',user_constellation) as constellation,
        count(distinct user_id) as num
    FROM
        ias_p3.tbl_ex_live_user_info_data_origin_orc a
    WHERE
        a.dt = '${day}'
        AND
        EXISTS (
            SELECT
                1
            FROM
                (
                    SELECT
                        MAX(data_generate_time) data_generate_time,
                        appPackageName,
                        user_id
                    FROM
                        ias_p3.tbl_ex_live_user_info_data_origin_orc
                    WHERE dt = '${day}'
                    AND appPackageName in ('${finance_live_app}')
                    AND is_live = 1
                    GROUP BY
                        appPackageName,
                        user_id
                ) b
            WHERE
                a.data_generate_time = b.data_generate_time
            AND a.user_id = b.user_id
            AND a.appPackageName = b.appPackageName
        ) GROUP BY appPackageName,user_constellation;
        "

    echo '################平台每日开播主播星座表2####################'
    tmp_app_user_live_constellation_2_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_constellation_2_p3 AS
    SELECT
        a.appPackageName,
        a.constellation,
        a.num,
        b.constellation_num_all
    FROM
        default.tmp_app_user_live_constellation_1_p3 as a
    FULL JOIN (
        SELECT
            appPackageName,
            sum(num) as constellation_num_all
        FROM
            default.tmp_app_user_live_constellation_1_p3
        GROUP BY
            appPackageName
    ) as b ON a.appPackageName = b.appPackageName;
    "

    echo '################平台每日开播主播星座比例####################'
    tmp_app_user_live_constellation_percent_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_constellation_percent_p3 AS
    SELECT
        appPackageName,
        constellation,
        num,
        num/constellation_num_all as constellation_percent
    FROM
        default.tmp_app_user_live_constellation_2_p3;
    "

    echo '################平台直播时长分布表####################'
    tmp_app_user_live_interval_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_interval_p3 AS
    SELECT
        t1.appPackageName,
        t1.category,
        t1.num,
        t2.user_all_count
    FROM
        (
            SELECT
                appPackageName,
                category,
                count(user_id) AS num
            FROM
                (
                    SELECT
                        appPackageName,
                        user_id,
                        CASE
                    WHEN live_online_length_day >= 0
                    AND live_online_length_day < 3600000 THEN
                        '0-1小时人数'
                    WHEN live_online_length_day >= 3600000
                    AND live_online_length_day < 7200000 THEN
                        '1-2小时人数'
                    WHEN live_online_length_day >= 7200000
                    AND live_online_length_day < 10800000 THEN
                        '2-3小时人数'
                    WHEN live_online_length_day >= 10800000
                    AND live_online_length_day < 14400000 THEN
                        '3-4小时人数'
                    WHEN live_online_length_day >= 14400000
                    AND live_online_length_day < 18000000 THEN
                        '4-5小时人数'
                    WHEN live_online_length_day >= 18000000
                    AND live_online_length_day < 21600000 THEN
                        '5-6小时人数'
                    WHEN live_online_length_day >= 21600000
                    AND live_online_length_day < 86400000 THEN
                        '6-24小时人数'
                    END AS category
                    FROM
                        live_p3.tbl_ex_live_user_online_daily_snapshot_of_id_list
                    WHERE
                        dt = '${day}'
                    AND appPackageName in ('${finance_live_app}')
                ) AS a
            GROUP BY
                appPackageName,
                category
        ) AS t1
    LEFT JOIN (
        SELECT
            appPackageName,
            count(DISTINCT user_id) AS user_all_count
        FROM
            live_p3.tbl_ex_live_user_online_daily_snapshot_of_id_list
        WHERE
            dt = '${day}'
        AND live_online_length_day is not null
        AND appPackageName in ('${finance_live_app}')
        GROUP BY
            appPackageName
    ) AS t2 ON t1.appPackageName = t2.appPackageName;
    "

    echo '################平台直播时长分布比例####################'
    tmp_app_user_live_interval_percent_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_interval_percent_p3 AS
    SELECT
        appPackageName,
        category,
        num,
        num/user_all_count as live_length_percent
    FROM
        default.tmp_app_user_live_interval_p3;
    "


    echo '################平台全量用户分类比例统计合并####################'

    app_user_all_category_save="insert into table live_p3_finance.tbl_ex_live_user_gender_percent_data partition(dt='${day}')
        SELECT
            'ALL_USER' as set_type,
            '${yearMonth}' as stat_month,
            CONCAT('${stat_date}','ALL_USER_Gender',gender) as keyWord,
            'ingkee' as meta_app_name,
            'tbl_ex_live_user_gender_percent_data' as meta_table_name,
            appPackageName,
            cast(gender as string) as gender,
            num,
            gender_percent
        FROM
            default.tmp_app_user_all_gender_percent_p3
        WHERE gender is not null AND appPackageName is not null;

    insert into table live_p3_finance.tbl_ex_live_user_hometown_percent_data partition(dt='${day}')
        SELECT
            'ALL_USER' as set_type,
            '${yearMonth}' as stat_month,
            CONCAT('${stat_date}','ALL_USER_Hometown',hometown) as keyWord,
            'ingkee' as meta_app_name,
            'tbl_ex_live_user_hometown_percent_data' as meta_table_name,
            appPackageName,
            hometown,
            num,
            hometown_percent
        FROM
            default.tmp_app_user_all_hometown_percent_p3
        WHERE hometown is not null AND appPackageName is not null;

    insert into table live_p3_finance.tbl_ex_live_user_constellation_percent_data partition(dt='${day}')
        SELECT
            'ALL_USER' as set_type,
            '${yearMonth}' as stat_month,
            CONCAT('${stat_date}','ALL_USER_Constellation',constellation) as keyWord,
            'ingkee' as meta_app_name,
            'tbl_ex_live_user_constellation_percent_data' as meta_table_name,
            appPackageName,
            constellation,
            num,
            constellation_percent
        FROM
            default.tmp_app_user_all_constellation_percent_p3
        WHERE constellation is not null AND appPackageName is not null;
        "

    echo '################平台每日开播主播分类比例统计合并####################'

    app_user_live_category_save="insert into table live_p3_finance.tbl_ex_live_user_gender_percent_data partition(dt='${day}')
        SELECT

           'LIVE_USER' as set_type,
            '${yearMonth}' as stat_month,
            CONCAT('${stat_date}','LIVE_USER_Gender',gender) as keyWord,
            'ingkee' as meta_app_name,
            'tbl_ex_live_user_gender_percent_data' as meta_table_name,
            appPackageName,
            cast(gender as string) as gender,
            num,
            gender_percent as num_percent
        FROM
            default.tmp_app_user_live_gender_percent_p3
        WHERE gender is not null  AND appPackageName is not null;

    insert into table live_p3_finance.tbl_ex_live_user_hometown_percent_data partition(dt='${day}')
        SELECT
            'LIVE_USER' as set_type,
            '${yearMonth}' as stat_month,
            CONCAT('${stat_date}','LIVE_USER_Hometown',hometown) as keyWord,
            'ingkee' as meta_app_name,
            'tbl_ex_live_user_hometown_percent_data' as meta_table_name,
            appPackageName,
            hometown,
            num,
            hometown_percent
        FROM
            default.tmp_app_user_live_hometown_percent_p3
        WHERE hometown is not null AND appPackageName is not null;

    insert into table live_p3_finance.tbl_ex_live_user_constellation_percent_data partition(dt='${day}')
        SELECT
            'LIVE_USER' as set_type,
            '${yearMonth}' as stat_month,
            CONCAT('${stat_date}','LIVE_USER_Constellation',constellation) as keyWord,
            'ingkee' as meta_app_name,
            'tbl_ex_live_user_constellation_percent_data' as meta_table_name,
            appPackageName,
            constellation,
            num,
            constellation_percent
        FROM
            default.tmp_app_user_live_constellation_percent_p3
        WHERE constellation is not null AND appPackageName is not null;
    "

    echo '################平台直播时长分布比例统计合并####################'

    app_user_live_interval_save="insert into table live_p3_finance.tbl_ex_live_user_interval_percent_data partition(dt='${day}')
    SELECT
        '${yearMonth}' as stat_month,
        CONCAT('${stat_date}','LIVE_USER_Interval',category) as keyWord,
        'ingkee' as meta_app_name,
        'tbl_ex_live_user_interval_percent_data' as meta_table_name,
        appPackageName,
        category,
        num,
        live_length_percent
    FROM
        default.tmp_app_user_live_interval_percent_p3
    WHERE category is not null AND appPackageName is not null;
    "

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE live_p3_finance.tbl_ex_live_user_gender_percent_data DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE live_p3_finance.tbl_ex_live_user_hometown_percent_data DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE live_p3_finance.tbl_ex_live_user_constellation_percent_data DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE live_p3_finance.tbl_ex_live_user_interval_percent_data DROP IF EXISTS PARTITION (dt='${day}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/live_p3_finance/tbl_ex_live_user_gender_percent_data/dt=${day}
    hdfs dfs -rm -r /data/live_p3_finance/tbl_ex_live_user_hometown_percent_data/dt=${day}
    hdfs dfs -rm -r /data/live_p3_finance/tbl_ex_live_user_constellation_percent_data/dt=${day}
    hdfs dfs -rm -r /data/live_p3_finance/tbl_ex_live_user_interval_percent_data/dt=${day}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${tmp_app_user_info_all_p3}
    ${tmp_app_user_all_gender_1_p3}
    ${tmp_app_user_all_gender_2_p3}
    ${tmp_app_user_all_gender_percent_p3}
    ${tmp_app_user_all_hometown_1_p3}
    ${tmp_app_user_all_hometown_2_p3}
    ${tmp_app_user_all_hometown_percent_p3}
    ${tmp_app_user_all_constellation_1_p3}
    ${tmp_app_user_all_constellation_2_p3}
    ${tmp_app_user_all_constellation_percent_p3}
    ${tmp_app_user_live_gender_1_p3}
    ${tmp_app_user_live_gender_2_p3}
    ${tmp_app_user_live_gender_percent_p3}
    ${tmp_app_user_live_hometown_1_p3}
    ${tmp_app_user_live_hometown_2_p3}
    ${tmp_app_user_live_hometown_percent_p3}
    ${tmp_app_user_live_constellation_1_p3}
    ${tmp_app_user_live_constellation_2_p3}
    ${tmp_app_user_live_constellation_percent_p3}
    ${tmp_app_user_live_interval_p3}
    ${tmp_app_user_live_interval_percent_p3}
    ${app_user_all_category_save}
    ${app_user_live_category_save}
    ${app_user_live_interval_save}
    "

echo '################平台全量用户分类比例结果输出####################'
#sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userall/category/1/000000_0
#iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userall/category/1/000000_0 -o ${tmp_dir}/全量用户性别比例_${1}.csv

#sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userall/category/2/000000_0
#iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userall/category/2/000000_0 -o ${tmp_dir}/全量用户地区比例_${1}.csv

#sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userall/category/3/000000_0
#iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userall/category/3/000000_0 -o ${tmp_dir}/全量用户星座比例_${1}.csv

echo '################平台每日开播主播分类比例结果输出####################'
#sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userlive/category/1/000000_0
#iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userlive/category/1/000000_0 -o ${tmp_dir}/每日开播主播性别比例_${1}.csv

#sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userlive/category/2/000000_0
#iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userlive/category/2/000000_0 -o ${tmp_dir}/每日开播主播地区比例_${1}.csv

#sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userlive/category/3/000000_0
#iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userlive/category/3/000000_0 -o ${tmp_dir}/每日开播主播星座比例_${1}.csv

#sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userlive/live-interval/000000_0
#iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userlive/live-interval/000000_0 -o ${tmp_dir}/主播每日直播时长分布比例_${1}.csv

echo '################# 映客相关统计信息 end  ########################'
