#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
yesterday=`date -d "-1 day $day" +%Y-%m-%d`
week=`date -d "-6 day $day" +%Y-%m-%d`
month=`date -d "-29 day $day" +%Y-%m-%d`
tmp_dir=/tmp/ingkee

echo '################# 映客相关统计信息 start   ########################'

    echo '################主播详细信息####################'
    tmp_user_info_p2="
    CREATE TEMPORARY TABLE default.tmp_user_info_p2 AS
    SELECT
            app_package_name,
            user_id,
            user_name,
            follow_count,
            fans_count,
            guardian_count
    FROM
            live_p2.tbl_ex_live_user_info_daily_snapshot_new
    WHERE
            app_package_name = 'com.meelive.ingkee'
        AND dt = '${day}';
    "

    echo '################主播总弹幕信息####################'
    tmp_user_danmu_p2="
    CREATE TEMPORARY TABLE default.tmp_user_danmu_p2 AS
    SELECT
        app_package_name,
        user_id,
        count(1) AS danmu_num_day,
        count(DISTINCT audience_id) AS audience_interact_num_day
    FROM
        (
                SELECT
                        app_package_name,
                        user_id,
                        audience_id
                FROM
                        ias_p2.tbl_ex_live_gift_info_orc
                WHERE
                        app_package_name = 'com.meelive.ingkee'
                AND dt = '${day}'
                UNION ALL
                        SELECT
                                app_package_name,
                                user_id,
                                audience_id
                        FROM
                                ias_p2.tbl_ex_live_message_info_orc
                        WHERE
                                app_package_name = 'com.meelive.ingkee'
                        AND dt = '${day}'
        ) result
    GROUP BY
        app_package_name,
        user_id;
    "

    echo '############# 主播礼物弹幕信息 #############'
    tmp_user_danmu_gift_p2="
    CREATE TEMPORARY TABLE default.tmp_user_danmu_gift_p2 AS
    SELECT
        app_package_name,
        user_id,
        sum(gift_val) AS gift_money_day,
        count(DISTINCT audience_id) AS audience_gift_num_day,
        count(1) AS danmu_gift_num_day
    FROM
        ias_p2.tbl_ex_live_gift_info_orc
    WHERE
        dt = '${day}'
    AND app_package_name = 'com.meelive.ingkee'
    GROUP BY
        app_package_name,
        user_id;
    "

    echo '################主播每日新增守护数量####################'
    tmp_user_guard_num_new_day_p2="
    CREATE TEMPORARY TABLE default.tmp_user_guard_num_new_day_p2 AS
    SELECT
	    a.app_package_name,
	    a.user_id,
	    count(DISTINCT a.guarder_id) AS user_guard_num_new_day
    FROM
	    (
		    SELECT
			    app_package_name,
			    user_id,
			    guarder_id
		    FROM
			    live_p2.tbl_ex_live_guard_info_daily_snapshot
		    WHERE
			    dt = '${day}'
		    AND app_package_name = 'com.meelive.ingkee'
	    ) AS a
    LEFT JOIN (
	    SELECT
		    app_package_name,
		    user_id,
		    guarder_id
	    FROM
		    live_p2.tbl_ex_live_guard_info_daily_snapshot
	    WHERE
		    dt = '${yesterday}'
	    AND app_package_name = 'com.meelive.ingkee'
    ) AS b ON a.app_package_name = b.app_package_name
    AND a.user_id = b.user_id
    AND a.guarder_id = b.guarder_id
    WHERE
	    b.guarder_id IS NULL
    GROUP BY
	    a.app_package_name,
	    a.user_id;
    "

    echo '################主播每日守护收益####################'
    tmp_user_guard_money_day_p2="
    CREATE TEMPORARY TABLE default.tmp_user_guard_money_day_p2 AS
    SELECT
            app_package_name,
            user_id,
            guardian_count*10000/365 as user_guard_money_day_u,
            guardian_count*60/7 as user_guard_money_day_l
    FROM
            live_p2.tbl_ex_live_user_info_daily_snapshot_new
    WHERE
            app_package_name = 'com.meelive.ingkee'
        AND dt = '${day}'
        AND guardian_count!=-1
        AND guardian_count is not null;
    "

    echo '################主播每日总收益####################'
    tmp_user_money_day_p2="
    CREATE TEMPORARY TABLE default.tmp_user_money_day_p2 AS
    SELECT
            coalesce(a.app_package_name, b.app_package_name) as app_package_name,
            coalesce(a.user_id, b.user_id) as user_id,
            (if(a.user_guard_money_day_u is null, 0, a.user_guard_money_day_u) + if(b.gift_money_day is null, 0, b.gift_money_day)) as user_money_day_u,
            (if(a.user_guard_money_day_l is null ,0, a.user_guard_money_day_l) + if(b.gift_money_day is null, 0, b.gift_money_day)) as user_money_day_l
    FROM
            default.tmp_user_guard_money_day_p2 as a
    FULL JOIN default.tmp_user_danmu_gift_p2 as b
    ON a.app_package_name = b.app_package_name
    AND a.user_id = b.user_id;
    "

        echo '################观众送礼信息####################'
    tmp_user_cost_gift_p2="
    CREATE TEMPORARY TABLE default.tmp_user_cost_gift_p2 AS
    SELECT
            app_package_name,
            audience_id as user_id,
            sum(gift_val) as user_gift_cost_money_day
    FROM
            ias_p2.tbl_ex_live_gift_info_orc
    WHERE
            dt = '${day}'
        AND app_package_name = 'com.meelive.ingkee'
        AND gift_val!=-1
        AND gift_val is not null
    GROUP BY
            app_package_name,
            audience_id;
    "

    echo '################主播统计信息合并####################'
    user_save="insert overwrite local directory '${tmp_dir}/user' row format delimited fields terminated by ',' \
    SELECT
            coalesce(a.app_package_name, b.app_package_name, c.app_package_name, d.app_package_name, e.app_package_name, f.app_package_name,g.app_package_name) as app_package_name,
            coalesce(a.user_id, b.user_id, c.user_id, d.user_id, e.user_id, f.user_id, g.user_id) as user_id,
            a.follow_count,
            a.fans_count,
            a.guardian_count,
            b.danmu_num_day,
            b.audience_interact_num_day,
            c.gift_money_day,
            c.audience_gift_num_day,
            c.danmu_gift_num_day,
            d.user_guard_num_new_day,
            e.user_guard_money_day_u,
            e.user_guard_money_day_l,
            f.user_money_day_u,
            f.user_money_day_l,
            g.live_day_count,
            g.live_online_count,
            h.live_online_length
    FROM
            default.tmp_user_info_p2 as a
    FULL JOIN default.tmp_user_danmu_p2 as b
        ON a.app_package_name = b.app_package_name
    AND a.user_id = b.user_id
    FULL JOIN default.tmp_user_danmu_gift_p2 as c
        ON a.app_package_name = c.app_package_name
    AND a.user_id = c.user_id
    FULL JOIN default.tmp_user_guard_num_new_day_p2 as d
        ON a.app_package_name = d.app_package_name
    AND a.user_id = d.user_id
    FULL JOIN default.tmp_user_guard_money_day_p2 as e
        ON a.app_package_name = e.app_package_name
    AND a.user_id = e.user_id
    FULL JOIN default.tmp_user_money_day_p2 as f
        ON a.app_package_name = f.app_package_name
    AND a.user_id = f.user_id
    FULL JOIN ( SELECT * FROM live_p2.tbl_ex_live_user_online_data_daily_snapshot where dt='${day}' AND app_package_name = 'com.meelive.ingkee') as g
        ON a.app_package_name = g.app_package_name
    AND a.user_id = g.user_id
    FULL JOIN ( SELECT * FROM live_p2.tbl_ex_live_user_online_data_snapshot where dt='${day}' AND app_package_name = 'com.meelive.ingkee') as h
        ON a.app_package_name = h.app_package_name
    AND a.user_id = h.user_id;
    "

    echo '################观众统计信息合并####################'
    audience_save="set mapred.reduce.tasks=1;insert overwrite local directory '${tmp_dir}/audience' row format delimited fields terminated by ',' \
    SELECT
            app_package_name,
            user_id,
            user_gift_cost_money_day
    FROM
            default.tmp_user_cost_gift_p2
    LIMIT 1000000000;
    "

    executeHiveCommand "
    ${tmp_user_info_p2}
    ${tmp_user_danmu_p2}
    ${tmp_user_danmu_gift_p2}
    ${tmp_user_guard_num_new_day_p2}
    ${tmp_user_guard_money_day_p2}
    ${tmp_user_money_day_p2}
    ${tmp_user_cost_gift_p2}
    ${user_save}
    ${audience_save}
    "

echo '################主播统计结果输出####################'
sed -i "1i 包名,用户id,关注数,粉丝数,守护数,每日弹幕总数,每日互动人数,每日收到礼物流水金额,每日送礼人数,每日收到礼物弹幕数,每日新政守护人数,每日守护收益高,每日守护收益高,每日总收入高,每日总收入低,历史累计直播天数,历史累计直播次数,每日直播时长" ${tmp_dir}/user/000000_0
iconv -f UTF-8 -t GBK -c ${tmp_dir}/user/000000_0 -o ${tmp_dir}/主播统计相关_${1}.csv

echo '################观众统计结果输出####################'
sed -i "1i 包名,用户id,每日送出礼物金额" ${tmp_dir}/audience/000000_0
iconv -f UTF-8 -t GBK -c ${tmp_dir}/audience/000000_0 -o ${tmp_dir}/观众统计相关_${1}.csv

echo '################# 映客相关统计信息 end  ########################'
