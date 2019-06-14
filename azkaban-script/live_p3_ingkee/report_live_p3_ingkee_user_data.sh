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

echo '################# 映客相关统计信息 start   ########################'

    echo '################主播详细信息，user_id, user_name,follow_num, fans_num, guard_num ####################'
    tmp_user_info_p3="
    CREATE TEMPORARY TABLE default.tmp_user_info_p3 AS
    SELECT a.appPackageName, a.user_id, a.user_name, a.follow_num, a.fans_num
        , if(b.guard_num IS NULL, 0, b.guard_num) AS guard_num
    FROM (
        SELECT appPackageName, user_id, user_name, follow_num, fans_num
            , guard_num
        FROM live_p3.tbl_ex_live_user_info_daily_snapshot
        WHERE (appPackageName IN ('${finance_live_app}')
            AND user_id IS NOT NULL
            AND user_id != ''
            AND dt =  '${day}')
    ) a
        LEFT JOIN (
            SELECT appPackageName, user_id, COUNT(DISTINCT guarder_id) AS guard_num
            FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
            WHERE (dt = '${day}'
                AND user_id IS NOT NULL
                AND user_id != ''
                AND guarder_id != '@system_info'
                AND appPackageName IN ('${finance_live_app}'))
            GROUP BY user_id, appPackageName
        ) b
        ON a.user_id = b.user_id
            AND a.appPackageName = b.appPackageName;
    "

    echo '################每日主播总弹幕，以及互动人数####################'
    tmp_user_danmu_p3="
    CREATE TEMPORARY TABLE default.tmp_user_danmu_p3 AS
    SELECT
        appPackageName,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id,
        count(1) AS danmu_num_day,
        count(DISTINCT audience_id) AS audience_interact_num_day
    FROM
        ias_p3.tbl_ex_live_danmu_data_origin_orc
        WHERE
            appPackageName in ('${finance_live_app}')
        AND dt = '${day}'
    GROUP BY
        appPackageName,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id ));
    "

    echo '############# 主播礼物弹幕信息,每日收到礼物金额，每日送礼物用户数，每日收到礼物弹幕数 #############'
    tmp_user_danmu_gift_p3="
    CREATE TEMPORARY TABLE default.tmp_user_danmu_gift_p3 AS
    SELECT
        appPackageName,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id,
        sum(gift_val) AS gift_income_money_day,
        count(DISTINCT audience_id) AS audience_gift_num_day,
        count(1) AS danmu_gift_num_day
    FROM
        live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
    WHERE
        dt = '${day}'
    AND gift_val>0
    AND appPackageName in ('${finance_live_app}')
    GROUP BY
        appPackageName,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id ));
    "

    echo '################主播每日新增守护数量####################'
    tmp_user_guard_num_new_day_p3="
    CREATE TEMPORARY TABLE default.tmp_user_guard_num_new_day_p3 AS
    SELECT
        a.appPackageName,
        a.user_id,
        count(DISTINCT a.guarder_id) AS guard_num_day_new
    FROM
        (
            SELECT
                appPackageName,
                user_id,
                guarder_id
            FROM
                live_p3.tbl_ex_live_guard_info_daily_snapshot
            WHERE
                dt = '${day}'
            AND appPackageName in ('${finance_live_app}')
        ) AS a
    LEFT JOIN (
        SELECT
            appPackageName,
            user_id,
            guarder_id
        FROM
            live_p3.tbl_ex_live_guard_info_daily_snapshot
        WHERE
            dt = '${yesterday}'
        AND appPackageName in ('${finance_live_app}')
    ) AS b ON a.appPackageName = b.appPackageName
    AND a.user_id = b.user_id
    AND a.guarder_id = b.guarder_id
    WHERE
        b.guarder_id IS NULL
    GROUP BY
        a.appPackageName,
        a.user_id;
    "


    echo '################主播每日守护收益####################'
    tmp_user_guard_money_day_p3="
    CREATE TEMPORARY TABLE default.tmp_user_guard_money_day_p3 AS
    SELECT
            appPackageName,
            user_id,
            COUNT(DISTINCT guarder_id) * 8.57 * 0.83 / 2 + COUNT(DISTINCT guarder_id) * 10 * 0.83 / 2 + COUNT(DISTINCT guarder_id) * 27.4 * 0.17 AS guard_money_day
    FROM
            ias_p3.tbl_ex_live_guard_list_data_origin_orc
    WHERE
        guarder_id IS NOT NULL AND guarder_id!='' AND user_id !='' AND guarder_id!='@system_info'
    AND appPackageName in ('${finance_live_app}')
    AND dt = '${day}'
    GROUP BY
        appPackageName,
        user_id;
    "

    echo '################主播每日总收益####################'
    tmp_user_money_day_p3="
    CREATE TEMPORARY TABLE default.tmp_user_money_day_p3 AS
    SELECT
            coalesce(a.appPackageName, b.appPackageName) as appPackageName,
            coalesce(a.user_id, b.user_id) as user_id,
            (if(a.guard_money_day is null, 0, a.guard_money_day) + if(b.gift_income_money_day is null, 0, b.gift_income_money_day)) as income_money_day
    FROM
            default.tmp_user_guard_money_day_p3 as a
    FULL JOIN default.tmp_user_danmu_gift_p3 as b
    ON a.appPackageName = b.appPackageName
    AND a.user_id = b.user_id;
    "



        echo '################主播7日礼物收入####################'
    tmp_user_gift_money_week_p3="
    CREATE TEMPORARY TABLE default.tmp_user_gift_money_week_p3 AS
    SELECT
            appPackageName,
            if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id,
            sum(gift_val) as gift_money_week
    FROM
            live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
    WHERE
            dt >= '${week}' and dt<='${day}'
        AND gift_val>0
        AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName,
            if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id ));
    "

    echo '################主播30日礼物收入####################'
    tmp_user_gift_money_month_p3="
    CREATE TEMPORARY TABLE default.tmp_user_gift_money_month_p3 AS
    SELECT
            appPackageName,
            if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id,
            sum(gift_val) as gift_money_month
    FROM
            live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
    WHERE
            dt >= '${month}' and dt<='${day}'
        AND gift_val>0
        AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName,
            if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id ));
    "

    echo '################主播历史累计礼物收入####################'
    tmp_user_gift_money_history_p3="
    CREATE TEMPORARY TABLE default.tmp_user_gift_money_history_p3 AS
    SELECT
            appPackageName,
            user_id,
            sum(gift_val) as gift_money_history
    FROM
            live_p3.tbl_ex_live_user_audience_gift_daily_snapshot
    WHERE
            dt ='${day}'
        AND gift_val>0
        AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName,
            user_id;
    "

    echo '################主播7日守护收益####################'
    tmp_user_guard_money_week_p3="
    CREATE TEMPORARY TABLE default.tmp_user_guard_money_week_p3 AS
    SELECT a.appPackageName, a.user_id, SUM(a.guard_money_week) AS guard_money_week
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
    GROUP BY a.appPackageName, a.user_id;
    "

    echo '################主播30日守护收益####################'
    tmp_user_guard_money_month_p3="
    CREATE TEMPORARY TABLE default.tmp_user_guard_money_month_p3 AS
    SELECT a.appPackageName, a.user_id, SUM(a.guard_money_month) AS guard_money_month
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
    GROUP BY a.appPackageName, a.user_id;
    "

    echo '################主播历史累计守护收益####################'
    tmp_user_guard_money_history_p3="
    CREATE TEMPORARY TABLE default.tmp_user_guard_money_history_p3 AS
    SELECT appPackageName, user_id,
        sum(guard_count) * 8.57 * 0.83 / 2 + sum(guard_count) * 10 * 0.83 / 2 + sum(guard_count) * 27.4 * 0.17 AS guard_money_history
    FROM live_p3.tbl_ex_live_user_audience_guard_daily_snapshot
    WHERE dt = '${day}' AND appPackageName IN ('${finance_live_app}')
        and user_id is not null and user_id!='' and guard_count>0
    GROUP BY appPackageName, user_id;
    "


    echo '################主播7日付费用户数####################'
    tmp_user_income_money_num_all_week_p3="
    CREATE TEMPORARY TABLE default.tmp_user_income_money_num_all_week_p3 AS
    SELECT
        appPackageName,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id,
        count(DISTINCT audience_id) as audience_cost_money_num_week
    FROM
        (
            SELECT
                appPackageName,
                user_id,
                audience_id
            FROM
                live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
            WHERE
                dt >= '${week}' and dt<='${day}'
            AND audience_id!=''
            AND gift_val>0
            AND (gift_id!='' or gift_name!='' or gift_image_url!='')
            AND appPackageName in ('${finance_live_app}')
            UNION
                SELECT
                    appPackageName,
                    user_id,
                    guarder_id as audience_id
                FROM
                    ias_p3.tbl_ex_live_guard_list_data_origin_orc
                WHERE
                    dt >= '${week}' and dt<='${day}'
                AND guarder_id IS NOT NULL AND guarder_id!='' AND user_id !='' AND guarder_id!='@system_info'
                AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id ));
    "

    echo '################主播30日付费用户数####################'
    tmp_user_income_money_num_all_month_p3="
    CREATE TEMPORARY TABLE default.tmp_user_income_money_num_all_month_p3 AS
    SELECT
        appPackageName,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id,
        count(DISTINCT audience_id) as audience_cost_money_num_month
    FROM
        (
            SELECT
                appPackageName,
                user_id,
                audience_id
            FROM
                live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
            WHERE
                dt >= '${month}' and dt<='${day}'
            AND audience_id!=''
            AND gift_val>0
            AND (gift_id!='' or gift_name!='' or gift_image_url!='')
            AND appPackageName in ('${finance_live_app}')
            UNION
                SELECT
                    appPackageName,
                    user_id,
                    guarder_id as audience_id
                FROM
                    ias_p3.tbl_ex_live_guard_list_data_origin_orc
                WHERE
                    dt >= '${month}' and dt<='${day}'
                AND guarder_id IS NOT NULL AND guarder_id!='' AND user_id !='' AND guarder_id!='@system_info'
                AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id ));
    "

    echo '################主播历史累计付费用户数####################'
    tmp_user_income_money_num_all_history_p3="
    CREATE TEMPORARY TABLE default.tmp_user_income_money_num_all_history_p3 AS
    SELECT
        appPackageName,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id,
        count(DISTINCT audience_id) as audience_cost_money_num_history
    FROM
        (
            SELECT
                appPackageName,
                user_id,
                audience_id
            FROM
                live_p3.tbl_ex_live_user_audience_gift_daily_snapshot
            WHERE
                dt ='${day}'
            AND appPackageName in ('${finance_live_app}')
            UNION
                SELECT
                    appPackageName,
                    user_id,
                    guarder_id as audience_id
                FROM
                    live_p3.tbl_ex_live_guard_info_daily_snapshot
                WHERE
                    dt ='${day}'
                AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id ));
    "

    echo '################主播7日arpu预统计数据####################'
    tmp_user_arpu_week_p3="
    CREATE TEMPORARY TABLE default.tmp_user_arpu_week_p3 AS
    SELECT
        coalesce(c.appPackageName, d.appPackageName) as appPackageName,
        coalesce(c.user_id, d.user_id) as user_id,
        c.money_week,
        d.audience_cost_money_num_week
    FROM
        (
            SELECT
                coalesce(a.appPackageName, b.appPackageName) as appPackageName,
                coalesce(a.user_id, b.user_id) as user_id,
                (if(a.gift_money_week is null,0,a.gift_money_week) + if(b.guard_money_week is null,0,b.guard_money_week)) as money_week
            FROM
                default.tmp_user_gift_money_week_p3 as a
            FULL JOIN default.tmp_user_guard_money_week_p3 as b
            ON a.appPackageName = b.appPackageName AND a.user_id = b.user_id
        ) as c
    FULL JOIN default.tmp_user_income_money_num_all_week_p3 as d
    ON c.appPackageName = d.appPackageName AND c.user_id = d.user_id;
    "

    echo '################主播7日arpu百分比例数据####################'
    tmp_user_arpu_percent_week_p3="
    CREATE TEMPORARY TABLE default.tmp_user_arpu_percent_week_p3 AS
    SELECT
        appPackageName,
        user_id,
        money_week/audience_cost_money_num_week as arpu_week
    FROM
        default.tmp_user_arpu_week_p3;
    "

    echo '################主播30日arpu预统计数据####################'
    tmp_user_arpu_month_p3="
    CREATE TEMPORARY TABLE default.tmp_user_arpu_month_p3 AS
    SELECT
        coalesce(c.appPackageName, d.appPackageName) as appPackageName,
        coalesce(c.user_id, d.user_id) as user_id,
        c.money_month,
        d.audience_cost_money_num_month
    FROM
        (
            SELECT
                coalesce(a.appPackageName, b.appPackageName) as appPackageName,
                coalesce(a.user_id, b.user_id) as user_id,
                (if(a.gift_money_month is null,0,a.gift_money_month) + if(b.guard_money_month is null,0,b.guard_money_month)) as money_month
            FROM
                default.tmp_user_gift_money_month_p3 as a
            FULL JOIN default.tmp_user_guard_money_month_p3 as b
            ON a.appPackageName = b.appPackageName AND a.user_id = b.user_id
        ) as c
    FULL JOIN default.tmp_user_income_money_num_all_month_p3 as d
    ON c.appPackageName = d.appPackageName AND c.user_id = d.user_id;
    "

    echo '################主播30日arpu百分比例数据####################'
    tmp_user_arpu_percent_month_p3="
    CREATE TEMPORARY TABLE default.tmp_user_arpu_percent_month_p3 AS
    SELECT
        appPackageName,
        user_id,
        money_month/audience_cost_money_num_month as arpu_month
    FROM
        default.tmp_user_arpu_month_p3;
    "

     echo '################主播历史累计arpu预统计数据####################'
    tmp_user_arpu_history_p3="
    CREATE TEMPORARY TABLE default.tmp_user_arpu_history_p3 AS
    SELECT
        coalesce(c.appPackageName, d.appPackageName) as appPackageName,
        coalesce(c.user_id, d.user_id) as user_id,
        c.money_history,
        d.audience_cost_money_num_history
    FROM
        (
            SELECT
                coalesce(a.appPackageName, b.appPackageName) as appPackageName,
                coalesce(a.user_id, b.user_id) as user_id,
                (if(a.gift_money_history is null,0,a.gift_money_history) + if(b.guard_money_history is null,0,b.guard_money_history)) as money_history
            FROM
                default.tmp_user_gift_money_history_p3 as a
            FULL JOIN default.tmp_user_guard_money_history_p3 as b
            ON a.appPackageName = b.appPackageName AND a.user_id = b.user_id
        ) as c
    FULL JOIN default.tmp_user_income_money_num_all_history_p3 as d
    ON c.appPackageName = d.appPackageName AND c.user_id = d.user_id;
    "

    echo '################主播历史累计arpu百分比例数据####################'
    tmp_user_arpu_percent_history_p3="
    CREATE TEMPORARY TABLE default.tmp_user_arpu_percent_history_p3 AS
    SELECT
        appPackageName,
        user_id,
        money_history/audience_cost_money_num_history as arpu_history
    FROM
        default.tmp_user_arpu_history_p3;
    "



    #------------------------观众相关统计-------------------------------#
    echo '################观众每日送礼信息####################'
    tmp_audience_cost_gift_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_cost_gift_p3 AS
    SELECT
            appPackageName,
            audience_id,
            sum(gift_val) as gift_cost_money_day
    FROM
            live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
    WHERE
            dt = '${day}'
        AND appPackageName in ('${finance_live_app}')
        AND gift_val>0
    GROUP BY
            appPackageName,
            audience_id;
    "

    echo '################观众7日送礼金额####################'
    tmp_audience_gift_money_week_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_gift_money_week_p3 AS
    SELECT
            appPackageName,
            audience_id,
            sum(gift_val) as gift_money_week
    FROM
            live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
    WHERE
            dt >= '${week}' and dt<='${day}'
        AND gift_val>0
        AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName,
            audience_id;
    "

    echo '################观众30日送礼金额####################'
    tmp_audience_gift_money_month_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_gift_money_month_p3 AS
    SELECT
            appPackageName,
            audience_id,
            sum(gift_val) as gift_money_month
    FROM
            live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
    WHERE
            dt >= '${month}' and dt<='${day}'
        AND gift_val>0
        AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName,
            audience_id;
    "

    echo '################观众历史累计送礼金额####################'
    tmp_audience_gift_money_history_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_gift_money_history_p3 AS
    SELECT
            appPackageName,
            audience_id,
            sum(gift_val) as gift_money_history
    FROM
            live_p3.tbl_ex_live_user_audience_gift_daily_snapshot
    WHERE
            dt ='${day}'
        AND gift_val>0
        AND appPackageName in ('${finance_live_app}')
    GROUP BY
            appPackageName,
            audience_id;
    "

     echo '################观众7日守护支出####################'
    tmp_audience_guard_money_week_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_guard_money_week_p3 AS
    SELECT a.appPackageName, a.guarder_id AS audience_id, SUM(a.guard_money_week) AS guard_money_week
    FROM (
        SELECT appPackageName, guarder_id,
            COUNT(DISTINCT user_id) * 8.57 * 0.83 / 2 + COUNT(DISTINCT user_id) * 10 * 0.83 / 2 + COUNT(DISTINCT user_id) * 27.4 * 0.17 AS guard_money_week
        FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
        WHERE (user_id IS NOT NULL AND user_id !=''
            AND guarder_id IS NOT NULL AND guarder_id!='' AND guarder_id!='@system_info'
            AND dt >= '${week}'
            AND dt <= '${day}'
            AND appPackageName IN ('${finance_live_app}'))
        GROUP BY dt, appPackageName, guarder_id
    ) a
    GROUP BY a.appPackageName, a.guarder_id;
    "

    echo '################观众30日守护支出####################'
    tmp_audience_guard_money_month_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_guard_money_month_p3 AS
    SELECT a.appPackageName, a.guarder_id AS audience_id, SUM(a.guard_money_month) AS guard_money_month
    FROM (
        SELECT appPackageName, guarder_id,
            COUNT(DISTINCT user_id) * 8.57 * 0.83 / 2 + COUNT(DISTINCT user_id) * 10 * 0.83 / 2 + COUNT(DISTINCT user_id) * 27.4 * 0.17 AS guard_money_month
        FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
        WHERE (user_id IS NOT NULL AND user_id !=''
            AND guarder_id IS NOT NULL AND guarder_id!='' AND guarder_id!='@system_info'
            AND dt >= '${month}'
            AND dt <= '${day}'
            AND appPackageName IN ('${finance_live_app}'))
        GROUP BY dt, appPackageName, guarder_id
    ) a
    GROUP BY a.appPackageName, a.guarder_id;
    "

    echo '################观众历史累计守护支出####################'
    tmp_audience_guard_money_history_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_guard_money_history_p3 AS
    SELECT appPackageName, audience_id,
        sum(guard_count) * 8.57 * 0.83 / 2 + sum(guard_count) * 10 * 0.83 / 2 + sum(guard_count) * 27.4 * 0.17 AS guard_money_history
    FROM live_p3.tbl_ex_live_user_audience_guard_daily_snapshot
    WHERE dt = '${day}' AND appPackageName IN ('${finance_live_app}')
        and user_id is not null and user_id!='' and guard_count>0
    GROUP BY appPackageName, audience_id;
    "


    echo '################观众每日打赏主播数####################'
    tmp_audience_cost_money_num_daily_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_cost_money_num_daily_p3 AS
    SELECT
        appPackageName,
        audience_id,
        count(DISTINCT user_id) as user_income_money_num_daily
    FROM
        (
            SELECT
                appPackageName,
                audience_id,
                if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id
            FROM
                live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
            WHERE
                dt='${day}'
            AND audience_id!=''
            AND gift_val>0
            AND (gift_id!='' or gift_name!='' or gift_image_url!='')
            AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName,
        audience_id;
    "

    echo '################观众7日支持主播数####################'
    tmp_audience_cost_money_num_all_week_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_cost_money_num_all_week_p3 AS
    SELECT
        appPackageName,
        audience_id,
        count(DISTINCT user_id) as user_income_money_num_week
    FROM
        (
            SELECT
                appPackageName,
                audience_id,
                if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id
            FROM
                live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
            WHERE
                dt >= '${week}' and dt<='${day}'
            AND audience_id!=''
            AND gift_val>0
            AND (gift_id!='' or gift_name!='' or gift_image_url!='')
            AND appPackageName in ('${finance_live_app}')
            UNION
                SELECT
                    appPackageName,
                    guarder_id as audience_id,
                    user_id
                FROM
                    ias_p3.tbl_ex_live_guard_list_data_origin_orc
                WHERE
                    dt >= '${week}' and dt<='${day}'
                AND guarder_id IS NOT NULL AND guarder_id!='' AND guarder_id!='@system_info'
                AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName,
        audience_id;
    "

    echo '################观众30日支持主播数####################'
    tmp_audience_cost_money_num_all_month_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_cost_money_num_all_month_p3 AS
    SELECT
        appPackageName,
        audience_id,
        count(DISTINCT user_id) as user_income_money_num_month
    FROM
        (
            SELECT
                appPackageName,
                audience_id,
                if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id
            FROM
                live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot
            WHERE
                dt >= '${month}' and dt<='${day}'
            AND audience_id!=''
            AND gift_val>0
            AND (gift_id!='' or gift_name!='' or gift_image_url!='')
            AND appPackageName in ('${finance_live_app}')
            UNION
                SELECT
                    appPackageName,
                    guarder_id as audience_id,
                    user_id
                FROM
                    ias_p3.tbl_ex_live_guard_list_data_origin_orc
                WHERE
                    dt >= '${month}' and dt<='${day}'
                AND guarder_id IS NOT NULL AND guarder_id!='' AND guarder_id!='@system_info'
                AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName,
        audience_id;
    "

    echo '################观众历史累计支持主播数####################'
    tmp_audience_cost_money_num_all_history_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_cost_money_num_all_history_p3 AS
    SELECT
        appPackageName,
        audience_id,
        count(DISTINCT user_id) as user_income_money_num_history
    FROM
        (
            SELECT
                appPackageName,
                audience_id,
                user_id
            FROM
                live_p3.tbl_ex_live_user_audience_gift_daily_snapshot
            WHERE
                dt ='${day}'
            AND appPackageName in ('${finance_live_app}')
            UNION
                SELECT
                    appPackageName,
                    guarder_id as audience_id,
                    user_id
                FROM
                    live_p3.tbl_ex_live_guard_info_daily_snapshot
                WHERE
                    dt ='${day}'
                AND appPackageName in ('${finance_live_app}')
        ) result
    GROUP BY
        appPackageName,
        audience_id;
    "

    echo '################观众7日arpu预统计数据####################'
    tmp_audience_arpu_week_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_arpu_week_p3 AS
    SELECT
        coalesce(c.appPackageName, d.appPackageName) as appPackageName,
        coalesce(c.audience_id, d.audience_id) as audience_id,
        c.money_week,
        d.user_income_money_num_week
    FROM
        (
            SELECT
                coalesce(a.appPackageName, b.appPackageName) as appPackageName,
                coalesce(a.audience_id, b.audience_id) as audience_id,
                (if(a.gift_money_week is null,0,a.gift_money_week) + if(b.guard_money_week is null,0,b.guard_money_week)) as money_week
            FROM
                default.tmp_audience_gift_money_week_p3 as a
            FULL JOIN default.tmp_audience_guard_money_week_p3 as b
            ON a.appPackageName = b.appPackageName AND a.audience_id = b.audience_id
        ) as c
    FULL JOIN default.tmp_audience_cost_money_num_all_week_p3 as d
    ON c.appPackageName = d.appPackageName AND c.audience_id = d.audience_id;
    "

    echo '################观众7日arpu百分比例数据####################'
    tmp_audience_arpu_percent_week_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_arpu_percent_week_p3 AS
    SELECT
        appPackageName,
        audience_id,
        money_week/user_income_money_num_week as arpu_week
    FROM
        default.tmp_audience_arpu_week_p3;
    "

    echo '################观众30日arpu预统计数据####################'
    tmp_audience_arpu_month_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_arpu_month_p3 AS
    SELECT
        coalesce(c.appPackageName, d.appPackageName) as appPackageName,
        coalesce(c.audience_id, d.audience_id) as audience_id,
        c.money_month,
        d.user_income_money_num_month
    FROM
        (
            SELECT
                coalesce(a.appPackageName, b.appPackageName) as appPackageName,
                coalesce(a.audience_id, b.audience_id) as audience_id,
                (if(a.gift_money_month is null,0,a.gift_money_month) + if(b.guard_money_month is null,0,b.guard_money_month)) as money_month
            FROM
                default.tmp_audience_gift_money_month_p3 as a
            FULL JOIN default.tmp_audience_guard_money_month_p3 as b
            ON a.appPackageName = b.appPackageName AND a.audience_id = b.audience_id
        ) as c
    FULL JOIN default.tmp_audience_cost_money_num_all_month_p3 as d
    ON c.appPackageName = d.appPackageName AND c.audience_id = d.audience_id;
    "

    echo '################观众30日arpu百分比例数据####################'
    tmp_audience_arpu_percent_month_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_arpu_percent_month_p3 AS
    SELECT
        appPackageName,
        audience_id,
        money_month/user_income_money_num_month as arpu_month
    FROM
        default.tmp_audience_arpu_month_p3;
    "

    echo '################观众历史累计arpu预统计数据####################'
    tmp_audience_arpu_history_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_arpu_history_p3 AS
    SELECT
        coalesce(c.appPackageName, d.appPackageName) as appPackageName,
        coalesce(c.audience_id, d.audience_id) as audience_id,
        c.money_history,
        d.user_income_money_num_history
    FROM
        (
            SELECT
                coalesce(a.appPackageName, b.appPackageName) as appPackageName,
                coalesce(a.audience_id, b.audience_id) as audience_id,
                (if(a.gift_money_history is null,0,a.gift_money_history) + if(b.guard_money_history is null,0,b.guard_money_history)) as money_history
            FROM
                default.tmp_audience_gift_money_history_p3 as a
            FULL JOIN default.tmp_audience_guard_money_history_p3 as b
            ON a.appPackageName = b.appPackageName AND a.audience_id = b.audience_id
        ) as c
    FULL JOIN default.tmp_audience_cost_money_num_all_history_p3 as d
    ON c.appPackageName = d.appPackageName AND c.audience_id = d.audience_id;
    "

    echo '################观众历史累计arpu百分比例数据####################'
    tmp_audience_arpu_percent_history_p3="
    CREATE TEMPORARY TABLE default.tmp_audience_arpu_percent_history_p3 AS
    SELECT
        appPackageName,
        audience_id,
        money_history/user_income_money_num_history as arpu_history
    FROM
        default.tmp_audience_arpu_history_p3;
    "


    echo '################主播统计信息合并####################'
    tmp_user_save_p3="CREATE TEMPORARY TABLE default.tmp_user_save_p3 AS
    SELECT
            coalesce(a.appPackageName,b.appPackageName,c.appPackageName,d.appPackageName,e.appPackageName,f.appPackageName,g.appPackageName,h.appPackageName,i.appPackageName,j.appPackageName,k.appPackageName,l.appPackageName,m.appPackageName) as appPackageName,
            coalesce(a.user_id,b.user_id,c.user_id,d.user_id,e.user_id,f.user_id,g.user_id,h.user_id,i.user_id,j.user_id,k.user_id,l.user_id,m.user_id) as user_id,
            a.follow_num,
            a.fans_num,
            a.guard_num,
            b.danmu_num_day,
            b.audience_interact_num_day,
            c.gift_income_money_day,
            c.audience_gift_num_day,
            c.danmu_gift_num_day,
            d.guard_num_day_new,
            e.guard_money_day,
            f.income_money_day,
            g.live_online_length_day,
            g.live_day_count_history,
            g.live_online_count_history,
            h.money_week,
            h.audience_cost_money_num_week,
            i.arpu_week,
            j.money_month,
            j.audience_cost_money_num_month,
            k.arpu_month,
            l.money_history,
            l.audience_cost_money_num_history,
            m.arpu_history,
            '${day}' as dt
    FROM
            default.tmp_user_info_p3 as a
    FULL JOIN default.tmp_user_danmu_p3 as b
        ON a.appPackageName = b.appPackageName
    AND a.user_id = b.user_id
    FULL JOIN default.tmp_user_danmu_gift_p3 as c
        ON a.appPackageName = c.appPackageName
    AND a.user_id = c.user_id
    FULL JOIN default.tmp_user_guard_num_new_day_p3 as d
        ON a.appPackageName = d.appPackageName
    AND a.user_id = d.user_id
    FULL JOIN default.tmp_user_guard_money_day_p3 as e
        ON a.appPackageName = e.appPackageName
    AND a.user_id = e.user_id
    FULL JOIN default.tmp_user_money_day_p3 as f
        ON a.appPackageName = f.appPackageName
    AND a.user_id = f.user_id
    FULL JOIN (
         SELECT *
         FROM  live_p3.tbl_ex_live_user_online_daily_snapshot_of_id_list
         where dt='${day}' AND appPackageName in ('${finance_live_app}')
    ) as g
        ON a.appPackageName = g.appPackageName
    AND a.user_id = g.user_id
    FULL JOIN default.tmp_user_arpu_week_p3 as h
        ON a.appPackageName = h.appPackageName
    AND a.user_id = h.user_id
    FULL JOIN default.tmp_user_arpu_percent_week_p3 as i
        ON a.appPackageName = i.appPackageName
    AND a.user_id = i.user_id
    FULL JOIN default.tmp_user_arpu_month_p3 as j
        ON a.appPackageName = j.appPackageName
    AND a.user_id = j.user_id
    FULL JOIN default.tmp_user_arpu_percent_month_p3 as k
        ON a.appPackageName = k.appPackageName
    AND a.user_id = k.user_id
    FULL JOIN default.tmp_user_arpu_history_p3 as l
        ON a.appPackageName = l.appPackageName
    AND a.user_id = l.user_id
    FULL JOIN default.tmp_user_arpu_percent_history_p3 as m
        ON a.appPackageName = m.appPackageName
    AND a.user_id = m.user_id;
    "

    echo '################主播统计信息存储hive####################'
    user_save_hive="insert into table live_p3_finance.tbl_ex_live_user_h partition(dt='${day}')
    SELECT  a.keyWord,
            a.appPackageName,
            a.user_id,
            max(a.follow_num),
            max(a.fans_num),
            max(a.guard_num),
            max(a.danmu_num_day),
            max(a.audience_interact_num_day),
            max(a.gift_income_money_day),
            max(a.audience_gift_num_day),
            max(a.danmu_gift_num_day),
            max(a.guard_num_day_new),
            max(a.guard_money_day),
            max(a.income_money_day),
            max(a.live_online_length_day),
            max(a.live_day_count_history),
            max(a.live_online_count_history),
            max(a.money_week),
            max(a.audience_cost_money_num_week),
            max(a.arpu_week),
            max(a.money_month),
            max(a.audience_cost_money_num_month),
            max(a.arpu_month),
            max(a.money_history),
            max(a.audience_cost_money_num_history),
            max(a.arpu_history)
    FROM
    (
        select *,concat('${stat_date}','INGKEE_LIVE_USER',user_id) as keyWord from default.tmp_user_save_p3 WHERE dt is not null AND appPackageName is not null AND user_id is not null AND user_id!=''
    ) as a
    GROUP BY a.appPackageName,a.user_id,a.dt,a.keyWord;
    "

    echo '################观众统计信息合并####################'
    tmp_audience_save_p3="CREATE TEMPORARY TABLE default.tmp_audience_save_p3 AS
    SELECT
            coalesce(a.appPackageName,b.appPackageName,c.appPackageName,d.appPackageName,e.appPackageName,f.appPackageName,g.appPackageName) as appPackageName,
            coalesce(a.audience_id,b.audience_id,c.audience_id,d.audience_id,e.audience_id,f.audience_id,g.audience_id) as user_id,
            a.gift_cost_money_day,
            h.user_income_money_num_daily,
            b.money_week,
            b.user_income_money_num_week,
            c.arpu_week,
            d.money_month,
            d.user_income_money_num_month,
            e.arpu_month,
            f.money_history,
            f.user_income_money_num_history,
            g.arpu_history,
            '${day}' as dt
    FROM
            default.tmp_audience_cost_gift_p3  as a
    FULL JOIN default.tmp_audience_cost_money_num_daily_p3 as h
        ON a.appPackageName = h.appPackageName
        AND a.audience_id = h.audience_id
    FULL JOIN default.tmp_audience_arpu_week_p3 as b
        ON a.appPackageName = b.appPackageName
    AND a.audience_id = b.audience_id
    FULL JOIN default.tmp_audience_arpu_percent_week_p3 as c
        ON a.appPackageName = c.appPackageName
    AND a.audience_id = c.audience_id
    FULL JOIN default.tmp_audience_arpu_month_p3 as d
        ON a.appPackageName = d.appPackageName
    AND a.audience_id = d.audience_id
    FULL JOIN default.tmp_audience_arpu_percent_month_p3 as e
        ON a.appPackageName = e.appPackageName
    AND a.audience_id = e.audience_id
    FULL JOIN default.tmp_audience_arpu_history_p3 as f
        ON a.appPackageName = f.appPackageName
    AND a.audience_id = f.audience_id
    FULL JOIN default.tmp_audience_arpu_percent_history_p3 as g
        ON a.appPackageName = g.appPackageName
    AND a.audience_id = g.audience_id;
    "


    echo '################观众统计信息存储hive####################'
    audience_save_hive="insert into table live_p3_finance.tbl_ex_live_audience_h partition(dt='${day}')
    SELECT a.keyWord,
            a.appPackageName,
            a.user_id,
            max(a.gift_cost_money_day),
            max(a.user_income_money_num_daily),
            max(a.money_week),
            max(a.user_income_money_num_week),
            max(a.arpu_week),
            max(a.money_month),
            max(a.user_income_money_num_month),
            max(a.arpu_month),
            max(a.money_history),
            max(a.user_income_money_num_history),
            max(a.arpu_history)
    FROM
    (
        select *,concat('${stat_date}','INGKEE_LIVE_AUDIENCE',user_id) as keyWord from default.tmp_audience_save_p3 WHERE dt is not null AND appPackageName is not null AND user_id is not null AND user_id!=''
    ) as a
     GROUP BY a.appPackageName,a.user_id,a.dt,a.keyWord;
    "

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE live_p3_finance.tbl_ex_live_user_h DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE live_p3_finance.tbl_ex_live_audience_h DROP IF EXISTS PARTITION (dt='${day}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rmr /data/live_p3_finance/tbl_ex_live_audience_h/dt=${day}
    hdfs dfs -rmr /data/live_p3_finance/tbl_ex_live_user_h/dt=${day}

    echo '################主播统计信息存储local####################'
    user_save_local="set mapred.reduce.tasks=1; insert overwrite local directory '${tmp_dir}/user' row format delimited fields terminated by ','
        SELECT apppackagename,user_id,follow_num,fans_num,guard_num,danmu_num_day,audience_interact_num_day,
               gift_income_money_day,audience_gift_num_day,danmu_gift_num_day,guard_num_day_new,
               guard_money_day,income_money_day,live_online_length_day,
               live_day_count_history,live_online_count_history,money_week,audience_cost_money_num_week,
               arpu_week,money_month,audience_cost_money_num_month,arpu_month,
               money_history,audience_cost_money_num_history,arpu_history,dt
        FROM live_p3_finance.tbl_ex_live_user_h WHERE dt='${day}' order by dt;
    "

    echo '################观众统计信息存储local####################'
    audience_save_local="set mapred.reduce.tasks=1; insert overwrite local directory '${tmp_dir}/audience' row format delimited fields terminated by ',' \
        SELECT apppackagename,user_id,gift_cost_money_day,user_income_money_num_daily,money_week,user_income_money_num_week,
               arpu_week,money_month,user_income_money_num_month,arpu_month,
               money_history,user_income_money_num_history,arpu_history,dt
        FROM live_p3_finance.tbl_ex_live_audience_h WHERE dt='${day}' order by dt;
    "


    executeHiveCommand "
    ${delete_hive_partitions}
    ${tmp_user_info_p3}
    ${tmp_user_danmu_p3}
    ${tmp_user_danmu_gift_p3}
    ${tmp_user_guard_num_new_day_p3}
    ${tmp_user_guard_money_day_p3}
    ${tmp_user_money_day_p3}
    ${tmp_user_gift_money_week_p3}
    ${tmp_user_gift_money_month_p3}
    ${tmp_user_gift_money_history_p3}
    ${tmp_user_guard_money_week_p3}
    ${tmp_user_guard_money_month_p3}
    ${tmp_user_guard_money_history_p3}
    ${tmp_user_income_money_num_all_week_p3}
    ${tmp_user_income_money_num_all_month_p3}
    ${tmp_user_income_money_num_all_history_p3}
    ${tmp_user_arpu_week_p3}
    ${tmp_user_arpu_percent_week_p3}
    ${tmp_user_arpu_month_p3}
    ${tmp_user_arpu_percent_month_p3}
    ${tmp_user_arpu_history_p3}
    ${tmp_user_arpu_percent_history_p3}
    ${tmp_audience_cost_gift_p3}
    ${tmp_audience_gift_money_week_p3}
    ${tmp_audience_gift_money_month_p3}
    ${tmp_audience_gift_money_history_p3}
    ${tmp_audience_guard_money_week_p3}
    ${tmp_audience_guard_money_month_p3}
    ${tmp_audience_guard_money_history_p3}
    ${tmp_audience_cost_money_num_daily_p3}
    ${tmp_audience_cost_money_num_all_week_p3}
    ${tmp_audience_cost_money_num_all_month_p3}
    ${tmp_audience_cost_money_num_all_history_p3}
    ${tmp_audience_arpu_week_p3}
    ${tmp_audience_arpu_percent_week_p3}
    ${tmp_audience_arpu_month_p3}
    ${tmp_audience_arpu_percent_month_p3}
    ${tmp_audience_arpu_history_p3}
    ${tmp_audience_arpu_percent_history_p3}
    ${tmp_user_save_p3}
    ${tmp_audience_save_p3}
    ${user_save_hive}
    ${audience_save_hive}
    ${user_save_local}
    ${audience_save_local}
    "

echo '################主播统计结果输出####################'
sed -i "1i 包名,用户id,关注数,粉丝数,守护数,每日弹幕,每日互动观众数,每日收到礼物金额,每日送礼观众数,每日礼物弹幕数,每日新增守护人数,每日守护收益,每日总收入,每日直播时长,累计直播天数,累计直播时长,7日总收入,7日付费观众数,7日arpu,30日总收入,30日付费观众数,30日arpu,累计送收入,累计付费观众数,累计apru,日期" ${tmp_dir}/user/000000_0
iconv -f UTF-8 -t GBK -c ${tmp_dir}/user/000000_0 -o ${tmp_dir}/主播统计相关_${day}.csv

echo '################观众统计结果输出####################'
sed -i "1i 包名,用户id,每日送礼金额,每日打赏支持主播人数,7日总花费,7日支持主播人数,7日arpu,30日总花费,30日支持主播数,30日arpu,累计总花费,累计支持主播数,累计apru,日期" ${tmp_dir}/audience/000000_0
iconv -f UTF-8 -t GBK -c ${tmp_dir}/audience/000000_0 -o ${tmp_dir}/观众统计相关_${day}.csv

echo '################# 映客相关统计信息 end  ########################'
