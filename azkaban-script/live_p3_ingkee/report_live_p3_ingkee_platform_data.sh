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
tmp_dir=/tmp/ingkee

echo '################# 映客相关统计信息 start  ########################'

    echo '################平台每日活跃主播####################'
    tmp_app_active_user_day_p3="
    CREATE TEMPORARY TABLE default.tmp_app_active_user_day_p3 AS
    SELECT
            appPackageName,
            count(DISTINCT user_id) as active_user_num_day
    FROM
            live_p3.tbl_ex_live_user_online_time_daily_snapshot_of_id_list
    WHERE
        dt = '${day}'
    AND user_id is not null AND user_id !=''
    AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName;
    "

    echo '################平台7天活跃主播####################'
    tmp_app_active_user_week_p3="
    CREATE TEMPORARY TABLE default.tmp_app_active_user_week_p3 AS
    SELECT
            appPackageName,
            count(DISTINCT user_id) as active_user_num_week
    FROM
            live_p3.tbl_ex_live_user_online_time_daily_snapshot_of_id_list
    WHERE
        dt>= '${week}'
    AND dt <= '${day}'
    AND user_id is not null AND user_id !=''
    AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName;
    "

    echo '################平台30天活跃主播####################'
    tmp_app_active_user_month_p3="
    CREATE TEMPORARY TABLE default.tmp_app_active_user_month_p3 AS
    SELECT
            appPackageName,
            count(DISTINCT user_id) as active_user_num_month
    FROM
            live_p3.tbl_ex_live_user_online_time_daily_snapshot_of_id_list
    WHERE
        dt>= '${month}'
    AND dt <= '${day}'
    AND user_id is not null AND user_id !=''
    AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName;
    "

    echo '################平台每日总礼物流水未去重####################'
    tmp_app_gift_money_day_p3_heavy="
    CREATE TEMPORARY TABLE default.tmp_app_gift_money_day_p3_heavy AS
    SELECT
            appPackageName,
            sum(gift_val) as gift_money_day_heavy
    FROM
            live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot_heavy
    WHERE
            dt = '${day}'
        AND gift_val>0
        AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName;
    "

    echo '################平台每日总礼物流水####################'
    tmp_app_gift_money_day_p3="
    CREATE TEMPORARY TABLE default.tmp_app_gift_money_day_p3 AS
    SELECT
            appPackageName,
            sum(gift_val) as gift_money_day
    FROM
            live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
    WHERE
            dt = '${day}'
        AND gift_val>0
        AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName;
    "


    echo '################平台7日总礼物流水####################'
    tmp_app_gift_money_week_p3="
    CREATE TEMPORARY TABLE default.tmp_app_gift_money_week_p3 AS
    SELECT
            appPackageName,
            sum(gift_val) as gift_money_week
    FROM
            live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
    WHERE
            dt >= '${week}' and dt<='${day}'
        AND gift_val>0
        AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName;
    "

    echo '################平台30日总礼物流水####################'
    tmp_app_gift_money_month_p3="
    CREATE TEMPORARY TABLE default.tmp_app_gift_money_month_p3 AS
    SELECT
            appPackageName,
            sum(gift_val) as gift_money_month
    FROM
            live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
    WHERE
            dt >= '${month}' and dt<='${day}'
        AND gift_val>0
        AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName;
    "


    echo '################平台每日守护收益####################'
    tmp_app_guard_money_day_p3="
    CREATE TEMPORARY TABLE default.tmp_app_guard_money_day_p3 AS
    SELECT a.appPackageName, SUM(a.guard_money_day) AS guard_money_day
    FROM (
        SELECT
                appPackageName, user_id,
                COUNT(DISTINCT guarder_id) * 8.57 * 0.83 / 2 + COUNT(DISTINCT guarder_id) * 10 * 0.83 / 2 + COUNT(DISTINCT guarder_id) * 27.4 * 0.17 AS guard_money_day
        FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
        WHERE  guarder_id is not null AND guarder_id!='' AND user_id !=''  AND guarder_id!='@system_info'
            AND appPackageName in ('${finance_live_app}')
            AND dt =  '${day}'
        GROUP BY appPackageName, user_id
    ) a
    GROUP BY a.appPackageName;
    "

    echo '################平台7日守护收益####################'
    tmp_app_guard_money_week_p3="
    CREATE TEMPORARY TABLE default.tmp_app_guard_money_week_p3 AS
    SELECT a.appPackageName, SUM(a.guard_money_week) AS guard_money_week
    FROM (
        SELECT appPackageName, user_id,
            COUNT(DISTINCT guarder_id) * 8.57 * 0.83 / 2 + COUNT(DISTINCT guarder_id) * 10 * 0.83 / 2 + COUNT(DISTINCT guarder_id) * 27.4 * 0.17 AS guard_money_week
        FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
        WHERE (guarder_id IS NOT NULL AND guarder_id!='' AND user_id !='' AND guarder_id!='@system_info'
            AND dt >= '${week}'
            AND dt <= '${day}'
            AND appPackageName IN ('${finance_live_app}'))
        GROUP BY dt, appPackageName, user_id
    ) a
    GROUP BY a.appPackageName;
    "


    echo '################平台30日守护收益####################'
    tmp_app_guard_money_month_p3="
    CREATE TEMPORARY TABLE default.tmp_app_guard_money_month_p3 AS
    SELECT a.appPackageName, SUM(a.guard_money_month) AS guard_money_month
    FROM (
        SELECT appPackageName, user_id,
            COUNT(DISTINCT guarder_id) * 8.57 * 0.83 / 2 + COUNT(DISTINCT guarder_id) * 10 * 0.83 / 2 + COUNT(DISTINCT guarder_id) * 27.4 * 0.17 AS guard_money_month
        FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
        WHERE (guarder_id IS NOT NULL AND guarder_id!='' AND user_id !='' AND guarder_id!='@system_info'
            AND dt >= '${month}'
            AND dt <= '${day}'
            AND appPackageName IN ('${finance_live_app}'))
        GROUP BY dt, appPackageName, user_id
    ) a
    GROUP BY a.appPackageName;
    "

    echo '################平台每日送礼物用户数####### 送弹幕礼物用户 #############'
    tmp_app_cost_gift_user_num_day_p3="
    CREATE TEMPORARY TABLE default.tmp_app_cost_gift_user_num_day_p3 AS
    SELECT
        appPackageName,
        count(DISTINCT user_id) AS user_cost_gift_num_day
    FROM
        (   SELECT
                appPackageName,
                audience_id AS user_id
            FROM
                live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
            WHERE
                dt = '${day}'
            AND audience_id !='' AND audience_id is not null
            AND gift_val >0
            AND (gift_id!='' or gift_name!='' or gift_image_url!='')
            AND appPackageName IN ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName;
    "

    echo '################平台每日守护付费用户数####### 守护用户去重  #############'
    tmp_app_cost_guarder_user_num_day_p3="
    CREATE TEMPORARY TABLE default.tmp_app_cost_guarder_user_num_day_p3 AS
    SELECT
        appPackageName,
        COUNT(DISTINCT guarder_id) AS user_cost_guarder_num_day
    FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
    WHERE (dt = '${day}'
        AND guarder_id IS NOT NULL
        AND guarder_id != ''
        AND guarder_id != '@system_info'
        AND appPackageName IN ('${finance_live_app}'))
    GROUP BY appPackageName;
    "

    echo '################平台每日守护人次####################'
    tmp_app_guard_count_day_p3="
    CREATE TEMPORARY TABLE default.tmp_app_guard_count_day_p3 AS
    SELECT a.appPackageName , SUM(a.guard_count) AS guard_count_day
    FROM (
        SELECT
                appPackageName, user_id,
                count(distinct guarder_id) as guard_count
        FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
        WHERE  guarder_id is not null AND guarder_id!='' AND user_id !=''  AND guarder_id!='@system_info'
            AND appPackageName in ('${finance_live_app}')
            AND dt =  '${day}'
        GROUP BY appPackageName, user_id
    ) a
    GROUP BY a.appPackageName;
    "

    echo '################平台每日付费用户数####### 包含弹幕礼物和守护用户  #############'
    tmp_app_cost_money_num_all_day_p3="
    CREATE TEMPORARY TABLE default.tmp_app_cost_money_num_all_day_p3 AS
    SELECT
        appPackageName,
        count(DISTINCT user_id) AS user_cost_money_num_day
    FROM
        (
            SELECT
                appPackageName,
                audience_id AS user_id
            FROM
                live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
            WHERE
                dt = '${day}'
            AND audience_id !='' AND audience_id is not null
            AND gift_val >0
            AND (gift_id!='' or gift_name!='' or gift_image_url!='')
            AND appPackageName IN ('${finance_live_app}')
            UNION
                SELECT
                    appPackageName,
                    guarder_id AS user_id
                FROM
                    ias_p3.tbl_ex_live_guard_list_data_origin_orc
                WHERE
                    dt = '${day}'
                AND guarder_id is not null AND guarder_id !='' AND guarder_id!='@system_info'
                AND appPackageName IN ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName;
    "


    echo '################平台7日打赏用户数########  只有礼物弹幕用户数 ############'
    tmp_app_cost_gift_user_num_week_p3="
    CREATE TEMPORARY TABLE default.tmp_app_cost_gift_user_num_week_p3 AS
    SELECT
        appPackageName,
        count(DISTINCT user_id) as user_cost_gift_num_week
    FROM
        (
            SELECT
                appPackageName,
                audience_id as user_id
            FROM
                live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
            WHERE
                dt >= '${week}' and dt<='${day}'
            AND audience_id!='' AND audience_id is not null
            AND gift_val>0
            AND (gift_id!='' or gift_name!='' or gift_image_url!='')
            AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName;
    "

    echo '################平台7日守护用户数####################'
    tmp_app_cost_guarder_user_num_week_p3="
    CREATE TEMPORARY TABLE default.tmp_app_cost_guarder_user_num_week_p3 AS
    SELECT
        appPackageName,
        count(DISTINCT user_id) as user_cost_guarder_num_week
    FROM
        (
                SELECT
                    appPackageName,
                    guarder_id as user_id
                FROM
                    ias_p3.tbl_ex_live_guard_list_data_origin_orc
                WHERE
                    dt >= '${week}' and dt<='${day}'
                AND guarder_id is not null AND guarder_id !='' AND guarder_id!='@system_info'
                AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName;
    "

    echo '################平台7日守护人次####################'
    tmp_app_guard_count_week_p3="
    CREATE TEMPORARY TABLE default.tmp_app_guard_count_week_p3 AS
    SELECT a.appPackageName, SUM(a.guard_count_week) AS guard_count_week
    FROM (
        SELECT appPackageName, user_id,
            COUNT(DISTINCT guarder_id) as guard_count_week
        FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
        WHERE (guarder_id IS NOT NULL AND guarder_id!='' AND user_id !='' AND guarder_id!='@system_info'
            AND dt >= '${week}'
            AND dt <= '${day}'
            AND appPackageName IN ('${finance_live_app}'))
        GROUP BY dt, appPackageName, user_id
    ) a
    GROUP BY a.appPackageName;
    "

    echo '################平台7日付费用户数####################'
    tmp_app_cost_money_num_all_week_p3="
    CREATE TEMPORARY TABLE default.tmp_app_cost_money_num_all_week_p3 AS
    SELECT
        appPackageName,
        count(DISTINCT user_id) as user_cost_money_num_week
    FROM
        (
            SELECT
                appPackageName,
                audience_id as user_id
            FROM
                live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
            WHERE
                dt >= '${week}' and dt<='${day}'
            AND audience_id!='' AND audience_id is not null
            AND gift_val>0
            AND (gift_id!='' or gift_name!='' or gift_image_url!='')
            AND appPackageName in ('${finance_live_app}')
            UNION
                SELECT
                    appPackageName,
                    guarder_id as user_id
                FROM
                    ias_p3.tbl_ex_live_guard_list_data_origin_orc
                WHERE
                    dt >= '${week}' and dt<='${day}'
                AND guarder_id is not null AND guarder_id !='' AND guarder_id!='@system_info'
                AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName;
    "


    echo '################平台30日打赏用户数####################'
    tmp_app_cost_gift_user_num_month_p3="
    CREATE TEMPORARY TABLE default.tmp_app_cost_gift_user_num_month_p3 AS
    SELECT
        appPackageName,
        count(DISTINCT user_id) as user_cost_gift_num_month
    FROM
        (
            SELECT
                appPackageName,
                audience_id as user_id
            FROM
                live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
            WHERE
                dt >= '${month}' and dt<='${day}'
            AND audience_id!='' AND audience_id is not null
            AND gift_val>0
            AND (gift_id!='' or gift_name!='' or gift_image_url!='')
            AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName;
    "

    echo '################平台30日守护用户数####################'
    tmp_app_cost_guarder_user_num_month_p3="
    CREATE TEMPORARY TABLE default.tmp_app_cost_guarder_user_num_month_p3 AS
    SELECT
        appPackageName,
        count(DISTINCT user_id) as user_cost_guarder_num_month
    FROM
        (
                SELECT
                    appPackageName,
                    guarder_id as user_id
                FROM
                    ias_p3.tbl_ex_live_guard_list_data_origin_orc
                WHERE
                    dt >= '${month}' and dt<='${day}'
                AND guarder_id is not null AND guarder_id !='' AND guarder_id!='@system_info'
                AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName;
    "

    echo '################平台30日守护人次####################'
    tmp_app_guard_count_month_p3="
    CREATE TEMPORARY TABLE default.tmp_app_guard_count_month_p3 AS
    SELECT a.appPackageName, SUM(a.guard_count_month) AS guard_count_month
    FROM (
        SELECT appPackageName, user_id,
            COUNT(DISTINCT guarder_id) as guard_count_month
        FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
        WHERE (guarder_id IS NOT NULL AND guarder_id!='' AND user_id !='' AND guarder_id!='@system_info'
            AND dt >= '${month}'
            AND dt <= '${day}'
            AND appPackageName IN ('${finance_live_app}'))
        GROUP BY dt, appPackageName, user_id
    ) a
    GROUP BY a.appPackageName;
    "

    echo '################平台30日付费用户数####################'
    tmp_app_cost_money_num_all_month_p3="
    CREATE TEMPORARY TABLE default.tmp_app_cost_money_num_all_month_p3 AS
    SELECT
        appPackageName,
        count(DISTINCT user_id) as user_cost_money_num_month
    FROM
        (
            SELECT
                appPackageName,
                audience_id as user_id
            FROM
                live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
            WHERE
                dt >= '${month}' and dt<='${day}'
            AND audience_id!='' AND audience_id is not null
            AND gift_val>0
            AND (gift_id!='' or gift_name!='' or gift_image_url!='')
            AND appPackageName in ('${finance_live_app}')
            UNION
                SELECT
                    appPackageName,
                    guarder_id as user_id
                FROM
                    ias_p3.tbl_ex_live_guard_list_data_origin_orc
                WHERE
                    dt >= '${month}' and dt<='${day}'
                AND guarder_id is not null AND guarder_id !='' AND guarder_id!='@system_info'
                AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName;
    "

    echo '################平台每日arpu预统计数据####################'
    tmp_app_arpu_day_p3="
    CREATE TEMPORARY TABLE default.tmp_app_arpu_day_p3 AS
    SELECT
        c.appPackageName,
        c.money_day,
        d.user_cost_money_num_day
    FROM
        (
            SELECT
                a.appPackageName,
                (a.gift_money_day + b.guard_money_day) as money_day
            FROM
                default.tmp_app_gift_money_day_p3 as a
            FULL JOIN default.tmp_app_guard_money_day_p3 as b
            ON a.appPackageName = b.appPackageName
        ) as c
    FULL JOIN default.tmp_app_cost_money_num_all_day_p3 as d
    ON c.appPackageName = d.appPackageName;
    "

    echo '################平台每日arpu百分比例数据####################'
    tmp_app_arpu_percent_day_p3="
    CREATE TEMPORARY TABLE default.tmp_app_arpu_percent_day_p3 AS
    SELECT
        appPackageName,
        money_day/user_cost_money_num_day as arpu_day
    FROM
        default.tmp_app_arpu_day_p3;
    "

    echo '################平台7日arpu预统计数据####################'
    tmp_app_arpu_week_p3="
    CREATE TEMPORARY TABLE default.tmp_app_arpu_week_p3 AS
    SELECT
        c.appPackageName,
        c.money_week,
        d.user_cost_money_num_week
    FROM
        (
            SELECT
                a.appPackageName,
                (a.gift_money_week + b.guard_money_week) as money_week
            FROM
                default.tmp_app_gift_money_week_p3 as a
            FULL JOIN default.tmp_app_guard_money_week_p3 as b
            ON a.appPackageName = b.appPackageName
        ) as c
    FULL JOIN default.tmp_app_cost_money_num_all_week_p3 as d
    ON c.appPackageName = d.appPackageName;
    "

    echo '################平台7日arpu百分比例数据####################'
    tmp_app_arpu_percent_week_p3="
    CREATE TEMPORARY TABLE default.tmp_app_arpu_percent_week_p3 AS
    SELECT
        appPackageName,
        money_week/user_cost_money_num_week as arpu_week
    FROM
        default.tmp_app_arpu_week_p3;
    "

    echo '################平台30日arpu预统计数据####################'
    tmp_app_arpu_month_p3="
    CREATE TEMPORARY TABLE default.tmp_app_arpu_month_p3 AS
    SELECT
        c.appPackageName,
        c.money_month,
        d.user_cost_money_num_month
    FROM
        (
            SELECT
                a.appPackageName,
                (a.gift_money_month + b.guard_money_month) as money_month
            FROM
                default.tmp_app_gift_money_month_p3 as a
            FULL JOIN default.tmp_app_guard_money_month_p3 as b
            ON a.appPackageName = b.appPackageName
        ) as c
    FULL JOIN default.tmp_app_cost_money_num_all_month_p3 as d
    ON c.appPackageName = d.appPackageName;
    "

    echo '################平台30日arpu百分比例数据####################'
    tmp_app_arpu_percent_month_p3="
    CREATE TEMPORARY TABLE default.tmp_app_arpu_percent_month_p3 AS
    SELECT
        appPackageName,
        money_month/user_cost_money_num_month as arpu_month
    FROM
        default.tmp_app_arpu_month_p3;
    "

    echo '################平台累计主播数####################'
    tmp_app_user_num_all_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_num_all_p3 AS
    SELECT
            appPackageName,
            count(distinct user_id) as user_num_all
    FROM
            live_p3.tbl_ex_live_user_info_daily_snapshot
    WHERE
            dt = '${day}'
        AND user_id is not null AND user_id !=''
        AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName;
    "

    echo '################平台每日新增活跃主播数####################'
    tmp_app_active_user_num_new_p3="
    CREATE TEMPORARY TABLE default.tmp_app_active_user_num_new_p3 AS
    SELECT
        a.appPackageName,
        (a.app_active_user_num - if(b.app_active_user_num is null,0,b.app_active_user_num)) as active_user_num_day_new
    FROM
        (
            SELECT
                appPackageName,
                count(user_id) as app_active_user_num
            FROM
                live_p3.tbl_ex_live_user_known_daily_snapshot
            WHERE
                dt = '${day}'
            AND appPackageName in ('${finance_live_app}')
            GROUP BY
                appPackageName
        ) as a
        LEFT JOIN
        (
            SELECT
                appPackageName,
                count(user_id) as app_active_user_num
            FROM
                live_p3.tbl_ex_live_user_known_daily_snapshot
            WHERE
                dt = '${yesterday}'
            AND appPackageName in ('${finance_live_app}')
            GROUP BY
                appPackageName
        ) as b
        ON a.appPackageName = b.appPackageName;
    "

    echo '################平台每日开播数####################'
    tmp_app_live_count_day_p3="
    CREATE TEMPORARY TABLE default.tmp_app_live_count_day_p3 AS
    SELECT
        appPackageName,
        count(1) as live_count_day
    FROM
        live_p3.tbl_ex_live_user_online_time_daily_snapshot_of_id_list
    WHERE
        dt = '${day}'
    AND user_id is not null AND user_id !=''
    AND appPackageName in ('${finance_live_app}')
    GROUP BY
        appPackageName;
    "

    echo '################平台每日新增付费用户数####################'
    tmp_app_user_money_num_new_p3="
    CREATE TEMPORARY TABLE default.tmp_app_user_money_num_new_p3 AS
    SELECT
        a.appPackageName,
        (a.audience_money_num - if(b.audience_money_num is null,0,b.audience_money_num)) AS audience_cost_money_num_day_new
    FROM
        (
            SELECT
                appPackageName,
                count(DISTINCT user_id) AS audience_money_num
            FROM
                (
                    SELECT
                        appPackageName,
                        audience_id AS user_id
                    FROM
                        live_p3.tbl_ex_live_user_audience_gift_daily_snapshot
                    WHERE
                        dt = '${day}'
                    AND appPackageName in ('${finance_live_app}')
                    UNION
                        SELECT
                            appPackageName,
                            guarder_id AS user_id
                        FROM
                            live_p3.tbl_ex_live_guard_info_daily_snapshot
                        WHERE
                            dt = '${day}'
                        AND guarder_id is not null AND guarder_id !='' AND guarder_id!='@system_info'
                        AND appPackageName in ('${finance_live_app}')
                ) AS t
            GROUP BY
                appPackageName
        ) AS a
    LEFT JOIN (
        SELECT
            appPackageName,
            count(DISTINCT user_id) AS audience_money_num
        FROM
            (
                SELECT
                    appPackageName,
                    audience_id AS user_id
                FROM
                    live_p3.tbl_ex_live_user_audience_gift_daily_snapshot
                WHERE
                    dt = '${yesterday}'
                AND appPackageName in ('${finance_live_app}')
                UNION
                    SELECT
                        appPackageName,
                        guarder_id AS user_id
                    FROM
                        live_p3.tbl_ex_live_guard_info_daily_snapshot
                    WHERE
                        dt = '${yesterday}'
                    AND guarder_id is not null AND guarder_id !='' AND guarder_id!='@system_info'
                    AND appPackageName in ('${finance_live_app}')
            ) AS t
        GROUP BY
            appPackageName
    ) AS b ON a.appPackageName = b.appPackageName;
    "

    echo '################平台统计信息合并存储到HDFS####################'
    app_save_to_hdfs="insert into table live_p3_finance.tbl_ex_live_platform_h partition(dt='${day}')
    SELECT
        concat('${stat_date}','INGKEE_PLATFORM') as keyWord,
        a.appPackageName,
        a.active_user_num_day,
        b.active_user_num_week,
        c.active_user_num_month,
        d.gift_money_day,
        e.gift_money_week,
        f.gift_money_month,
        if(g.guard_money_day is not null,g.guard_money_day,0) as guard_money_day,
        if(h.guard_money_week is not null,h.guard_money_week,0) as guard_money_week,
        if(i.guard_money_month is not null,i.guard_money_month,0) as guard_money_month,
        u.user_cost_gift_num_day,
        u2.user_cost_gift_num_week,
        u3.user_cost_gift_num_month,
        if(v.user_cost_guarder_num_day is not null,v.user_cost_guarder_num_day,0) as user_cost_guarder_num_day,
        if(v2.user_cost_guarder_num_week is not null,v2.user_cost_guarder_num_week,0) as user_cost_guarder_num_week,
        if(v3.user_cost_guarder_num_month is not null,v3.user_cost_guarder_num_month,0) as user_cost_guarder_num_month,
        if(w.guard_count_day is not null,w.guard_count_day,0) as guard_count_day,
        if(w2.guard_count_week is not null,w2.guard_count_week,0) as guard_count_week,
        if(w3.guard_count_month is not null,w3.guard_count_month,0) as guard_count_month,
        j.user_cost_money_num_day,
        k.user_cost_money_num_week,
        l.user_cost_money_num_month,
        m.arpu_day,
        n.arpu_week,
        o.arpu_month,
        p.user_num_all,
        q.active_user_num_day_new,
        if(s.live_count_day is not null,s.live_count_day,0) as live_count_day,
        t.audience_cost_money_num_day_new,
        q3.gift_money_day_heavy

    FROM
        default.tmp_app_active_user_day_p3 as a
    FULL JOIN default.tmp_app_active_user_week_p3 as b
        ON a.appPackageName = b.appPackageName
    FULL JOIN default.tmp_app_active_user_month_p3 as c
        ON a.appPackageName = c.appPackageName
    FULL JOIN default.tmp_app_gift_money_day_p3 as d
        ON a.appPackageName = d.appPackageName
    FULL JOIN default.tmp_app_gift_money_week_p3 as e
        ON a.appPackageName = e.appPackageName
    FULL JOIN default.tmp_app_gift_money_month_p3 as f
        ON a.appPackageName = f.appPackageName
    FULL JOIN default.tmp_app_guard_money_day_p3 as g
        ON a.appPackageName = g.appPackageName
    FULL JOIN default.tmp_app_guard_money_week_p3 as h
        ON a.appPackageName = h.appPackageName
    FULL JOIN default.tmp_app_guard_money_month_p3 as i
        ON a.appPackageName = i.appPackageName
    FULL JOIN default.tmp_app_cost_money_num_all_day_p3 as j
        ON a.appPackageName = j.appPackageName
    FULL JOIN default.tmp_app_cost_money_num_all_week_p3 as k
        ON a.appPackageName = k.appPackageName
    FULL JOIN default.tmp_app_cost_money_num_all_month_p3 as l
        ON a.appPackageName = l.appPackageName
    FULL JOIN default.tmp_app_arpu_percent_day_p3 as m
        ON a.appPackageName = m.appPackageName
    FULL JOIN default.tmp_app_arpu_percent_week_p3 as n
        ON a.appPackageName = n.appPackageName
    FULL JOIN default.tmp_app_arpu_percent_month_p3 as o
        ON a.appPackageName = o.appPackageName
    FULL JOIN default.tmp_app_user_num_all_p3 as p
        ON a.appPackageName = p.appPackageName
    FULL JOIN default.tmp_app_active_user_num_new_p3 as q
        ON a.appPackageName = q.appPackageName
    FULL JOIN default.tmp_app_live_count_day_p3 as s
        ON a.appPackageName = s.appPackageName
    FULL JOIN default.tmp_app_user_money_num_new_p3 as t
        ON a.appPackageName = t.appPackageName
    FULL JOIN default.tmp_app_cost_gift_user_num_day_p3 as u
        ON a.appPackageName = u.appPackageName
    FULL JOIN default.tmp_app_cost_guarder_user_num_day_p3 as v
        ON a.appPackageName = v.appPackageName
    FULL JOIN default.tmp_app_guard_count_day_p3 as w
        ON a.appPackageName = w.appPackageName
    FULL JOIN default.tmp_app_cost_gift_user_num_week_p3 as u2
        ON a.appPackageName = u2.appPackageName
    FULL JOIN default.tmp_app_cost_guarder_user_num_week_p3 as v2
        ON a.appPackageName = v2.appPackageName
    FULL JOIN default.tmp_app_guard_count_week_p3 as w2
        ON a.appPackageName = w2.appPackageName
    FULL JOIN default.tmp_app_cost_gift_user_num_month_p3 as u3
        ON a.appPackageName = u3.appPackageName
    FULL JOIN default.tmp_app_cost_guarder_user_num_month_p3 as v3
        ON a.appPackageName = v3.appPackageName
    FULL JOIN default.tmp_app_guard_count_month_p3 as w3
        ON a.appPackageName = w3.appPackageName
    FULL JOIN default.tmp_app_gift_money_day_p3_heavy as q3
        ON a.appPackageName = q3.appPackageName
        ;
    "

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE live_p3_finance.tbl_ex_live_platform_h DROP IF EXISTS PARTITION (dt='${day}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rmr /data/live_p3_finance/tbl_ex_live_platform_h/dt=${day}


    executeHiveCommand "
    ${delete_hive_partitions}
    ${tmp_app_active_user_day_p3}
    ${tmp_app_active_user_week_p3}
    ${tmp_app_active_user_month_p3}
    ${tmp_app_gift_money_day_p3}
    ${tmp_app_gift_money_day_p3_heavy}
    ${tmp_app_gift_money_week_p3}
    ${tmp_app_gift_money_month_p3}
    ${tmp_app_guard_money_day_p3}
    ${tmp_app_guard_money_week_p3}
    ${tmp_app_guard_money_month_p3}
    ${tmp_app_cost_gift_user_num_day_p3}
    ${tmp_app_cost_gift_user_num_week_p3}
    ${tmp_app_cost_gift_user_num_month_p3}
    ${tmp_app_cost_guarder_user_num_day_p3}
    ${tmp_app_cost_guarder_user_num_week_p3}
    ${tmp_app_cost_guarder_user_num_month_p3}
    ${tmp_app_guard_count_day_p3}
    ${tmp_app_guard_count_week_p3}
    ${tmp_app_guard_count_month_p3}
    ${tmp_app_cost_money_num_all_day_p3}
    ${tmp_app_cost_money_num_all_week_p3}
    ${tmp_app_cost_money_num_all_month_p3}
    ${tmp_app_arpu_day_p3}
    ${tmp_app_arpu_percent_day_p3}
    ${tmp_app_arpu_week_p3}
    ${tmp_app_arpu_percent_week_p3}
    ${tmp_app_arpu_month_p3}
    ${tmp_app_arpu_percent_month_p3}
    ${tmp_app_user_num_all_p3}
    ${tmp_app_active_user_num_new_p3}
    ${tmp_app_live_count_day_p3}
    ${tmp_app_user_money_num_new_p3}
    ${app_save_to_hdfs}
    "

#echo '################平台统计结果输出####################'
#sed -i "1i 包名,每日活跃主播,7日活跃主播,30日活跃主播,每日礼物流水金额,7日礼物流水金额,30日礼物流水金额,每日守护金额高,每日守护金额低,7日守护金额高,7日守护金额低,30日守护金额高,30日守护金额低,每日付费用户,7日付费用户,30日付费用户,每日arpu高,每日arpu低,7日arpu高,7日arpu低,30日arpu高,30日arpu低,全量主播,每日新增开播主播,每日最大在线观众数,每日开播数,每日新增付费用户数" ${tmp_dir}/app/000000_0
#iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/000000_0 -o ${tmp_dir}/平台统计相关_${1}.csv
#echo '################# 映客相关统计信息 end  ########################'


    echo '################# 发送数据到prometheus start  ########################'
        report_sql="
        SELECT concat(a.tag, '-', a.val)
        FROM (
            SELECT 'active_user_num_day' AS tag, active_user_num_day AS val
            FROM live_p3_finance.tbl_ex_live_platform_h
            WHERE dt = '${day}'
            UNION ALL
            SELECT 'user_cost_money_num_day' AS tag, user_cost_money_num_day AS val
            FROM live_p3_finance.tbl_ex_live_platform_h
            WHERE dt = '${day}'
            UNION ALL
            SELECT 'user_cost_gift_num_day' AS tag, user_cost_gift_num_day AS val
            FROM live_p3_finance.tbl_ex_live_platform_h
            WHERE dt = '${day}'
            UNION ALL
            SELECT 'gift_money_day' AS tag, gift_money_day AS val
            FROM live_p3_finance.tbl_ex_live_platform_h
            WHERE dt = '${day}'
            UNION ALL
            SELECT 'user_cost_guarder_num_day' AS tag, user_cost_guarder_num_day AS val
            FROM live_p3_finance.tbl_ex_live_platform_h
            WHERE dt = '${day}'
            UNION ALL
            SELECT 'guard_count_day' AS tag, guard_count_day AS val
            FROM live_p3_finance.tbl_ex_live_platform_h
            WHERE dt = '${day}'
            UNION ALL
            SELECT 'live_count_day' AS tag, live_count_day AS val
            FROM live_p3_finance.tbl_ex_live_platform_h
            WHERE dt = '${day}'
        ) a;
        "

    data=$(executeHiveCommandResult "${report_sql}" )

    echo "#########################"
    echo $data
    echo "#########################"
    curl -X DELETE http://pushgateway.haimacloud.com/metrics/job/report_live_p3_ingkee_platform_data/app/com.meelive.ingkee/instance/MVP-HADOOP31

    for var in ${data[@]}
    do
       echo ${var//'-'/' '}
       echo ${var//'-'/' '} | curl --data-binary @- http://pushgateway.haimacloud.com/metrics/job/report_live_p3_ingkee_platform_data/instance/MVP-HADOOP31/app/com.meelive.ingkee
    done

    echo '################# 发送数据到prometheus  end  ########################'