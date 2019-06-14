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
tmp_mysql_result_file=${tmp_dir}/mysql_result.csv

echo '################# 映客相关统计信息 start  ########################'

    echo '################平台每日活跃主播####################'
    tmp_app_active_user_day_p2="
    CREATE TEMPORARY TABLE default.tmp_app_active_user_day_p2 AS
    SELECT
       	    app_package_name,
            count(DISTINCT user_id) as active_user_num_day
    FROM
            ias_p2.tbl_ex_live_user_info_data_origin_orc
    WHERE
            is_live = 1
    AND dt = '${day}'
    AND app_package_name = 'com.meelive.ingkee'
    GROUP BY
            app_package_name;
    "

    echo '################平台7天活跃主播####################'
    tmp_app_active_user_week_p2="
    CREATE TEMPORARY TABLE default.tmp_app_active_user_week_p2 AS
    SELECT
            app_package_name,
            count(DISTINCT user_id) as active_user_num_week
    FROM
            ias_p2.tbl_ex_live_user_info_data_origin_orc
    WHERE
            is_live = 1
        AND dt>= '${week}'
    AND dt <= '${day}'
    AND app_package_name = 'com.meelive.ingkee'
    GROUP BY
            app_package_name;
    "

    echo '################平台30天活跃主播####################'
    tmp_app_active_user_month_p2="
    CREATE TEMPORARY TABLE default.tmp_app_active_user_month_p2 AS
    SELECT
            app_package_name,
            count(DISTINCT user_id) as active_user_num_month
    FROM
            ias_p2.tbl_ex_live_user_info_data_origin_orc
    WHERE
            is_live = 1
        AND dt>= '${month}'
    AND dt <= '${day}'
    AND app_package_name = 'com.meelive.ingkee'
    GROUP BY
            app_package_name;
    "

    echo '################平台每日总礼物流水####################'
    tmp_app_gift_money_day_p2="
    CREATE TEMPORARY TABLE default.tmp_app_gift_money_day_p2 AS
    SELECT
            app_package_name,
            sum(gift_val) as app_gift_money_day
    FROM
            ias_p2.tbl_ex_live_gift_info_orc
    WHERE
            dt = '${day}'
        AND app_package_name = 'com.meelive.ingkee'
    GROUP BY
            app_package_name;
    "

    echo '################平台7日总礼物流水####################'
    tmp_app_gift_money_week_p2="
    CREATE TEMPORARY TABLE default.tmp_app_gift_money_week_p2 AS
    SELECT
            app_package_name,
            sum(gift_val) as app_gift_money_week
    FROM
            ias_p2.tbl_ex_live_gift_info_orc
    WHERE
            dt >= '${week}' and dt<='${day}'
        AND app_package_name = 'com.meelive.ingkee'
    GROUP BY
            app_package_name;
    "

    echo '################平台30日总礼物流水####################'
    tmp_app_gift_money_month_p2="
    CREATE TEMPORARY TABLE default.tmp_app_gift_money_month_p2 AS
    SELECT
            app_package_name,
            sum(gift_val) as app_gift_money_month
    FROM
            ias_p2.tbl_ex_live_gift_info_orc
    WHERE
            dt >= '${month}' and dt<='${day}'
        AND app_package_name = 'com.meelive.ingkee'
    GROUP BY
            app_package_name;
    "

    echo '################平台每日守护收益####################'
    tmp_app_guard_money_day_p2="
    CREATE TEMPORARY TABLE default.tmp_app_guard_money_day_p2 AS
    SELECT
	    app_package_name,
	    sum(guardian_count) * 10000 / 365 as app_guard_money_day_u,
	    sum(guardian_count) * 60 / 7 as app_guard_money_day_l
    FROM
	    live_p2.tbl_ex_live_user_info_daily_snapshot_new
    WHERE
	    guardian_count != -1
    AND guardian_count IS NOT NULL
    AND dt = '${day}'
    AND app_package_name = 'com.meelive.ingkee'
    GROUP BY
	    app_package_name;
    "

    echo '################平台7日守护收益####################'
    tmp_app_guard_money_week_p2="
    CREATE TEMPORARY TABLE default.tmp_app_guard_money_week_p2 AS
    SELECT
	    app_package_name,
	    sum(guardian_count) * 10000 / 365 as app_guard_money_week_u,
	    sum(guardian_count) * 60 / 7 as app_guard_money_week_l
    FROM
	    live_p2.tbl_ex_live_user_info_daily_snapshot_new
    WHERE
	    guardian_count != -1
    AND guardian_count IS NOT NULL
    AND dt >= '${week}' and dt <= '${day}'
    AND app_package_name = 'com.meelive.ingkee'
    GROUP BY
	    app_package_name;
    "

    echo '################平台30日守护收益####################'
    tmp_app_guard_money_month_p2="
    CREATE TEMPORARY TABLE default.tmp_app_guard_money_month_p2 AS
    SELECT
	    app_package_name,
	    sum(guardian_count) * 10000 / 365 as app_guard_money_month_u,
	    sum(guardian_count) * 60 / 7 as app_guard_money_month_l
    FROM
	    live_p2.tbl_ex_live_user_info_daily_snapshot_new
    WHERE
	    guardian_count != -1
    AND guardian_count IS NOT NULL
    AND dt >= '${month}' and dt <= '${day}'
    AND app_package_name = 'com.meelive.ingkee'
    GROUP BY
	    app_package_name;
    "

    echo '################平台每日付费用户数####################'
    tmp_app_user_cost_money_num_all_day_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_cost_money_num_all_day_p2 AS
    SELECT
	    app_package_name,
	    count(DISTINCT user_id) as user_cost_money_num_all_day
    FROM
	    (
		    SELECT
			    app_package_name,
			    audience_id as user_id
		    FROM
			    ias_p2.tbl_ex_live_gift_info_orc
		    WHERE
			    dt = '${day}'
		    AND app_package_name = 'com.meelive.ingkee'
		    UNION
			    SELECT
				    app_package_name,
				    guarder_id as user_id
			    FROM
				    ias_p2.tbl_ex_live_guard_info_data_origin_orc
			    WHERE
				    dt = '${day}'
			    AND app_package_name = 'com.meelive.ingkee'
	    ) result
    GROUP BY
	    app_package_name;
    "

    echo '################平台7日付费用户数####################'
    tmp_app_user_cost_money_num_all_week_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_cost_money_num_all_week_p2 AS
    SELECT
	    app_package_name,
	    count(DISTINCT user_id) as user_cost_money_num_all_week
    FROM
	    (
		    SELECT
			    app_package_name,
			    audience_id as user_id
		    FROM
			    ias_p2.tbl_ex_live_gift_info_orc
		    WHERE
			    dt >= '${week}' and dt<='${day}'
		    AND app_package_name = 'com.meelive.ingkee'
		    UNION
			    SELECT
				    app_package_name,
				    guarder_id as user_id
			    FROM
				    ias_p2.tbl_ex_live_guard_info_data_origin_orc
			    WHERE
				    dt >= '${week}' and dt<='${day}'
			    AND app_package_name = 'com.meelive.ingkee'
	    ) result
    GROUP BY
	    app_package_name;
    "

    echo '################平台30日付费用户数####################'
    tmp_app_user_cost_money_num_all_month_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_cost_money_num_all_month_p2 AS
    SELECT
	    app_package_name,
	    count(DISTINCT user_id) as user_cost_money_num_all_month
    FROM
	    (
		    SELECT
			    app_package_name,
			    audience_id as user_id
		    FROM
			    ias_p2.tbl_ex_live_gift_info_orc
		    WHERE
			    dt >= '${month}' and dt<='${day}'
		    AND app_package_name = 'com.meelive.ingkee'
		    UNION
			    SELECT
				    app_package_name,
				    guarder_id as user_id
			    FROM
				    ias_p2.tbl_ex_live_guard_info_data_origin_orc
			    WHERE
				    dt >= '${month}' and dt<='${day}'
			    AND app_package_name = 'com.meelive.ingkee'
	    ) result
    GROUP BY
	    app_package_name;
    "

    echo '################平台每日arpu预统计数据####################'
    tmp_app_arpu_day_p2="
    CREATE TEMPORARY TABLE default.tmp_app_arpu_day_p2 AS
    SELECT
	    c.app_package_name,
	    c.app_money_day_u,
	    c.app_money_day_l,
	    d.user_cost_money_num_all_day
    FROM
	    (
		    SELECT
			    a.app_package_name,
			    (a.app_gift_money_day + b.app_guard_money_day_u) as app_money_day_u,
			    (a.app_gift_money_day + b.app_guard_money_day_l) as app_money_day_l
		    FROM
			    default.tmp_app_gift_money_day_p2 as a
		    FULL JOIN default.tmp_app_guard_money_day_p2 as b
		    ON a.app_package_name = b.app_package_name
	    ) as c
    FULL JOIN default.tmp_app_user_cost_money_num_all_day_p2 as d
    ON c.app_package_name = d.app_package_name;
    "

    echo '################平台每日arpu百分比例数据####################'
    tmp_app_arpu_percent_day_p2="
    CREATE TEMPORARY TABLE default.tmp_app_arpu_percent_day_p2 AS
    SELECT
	    app_package_name,
	    app_money_day_u/user_cost_money_num_all_day as arpu_percent_day_u,
	    app_money_day_l/user_cost_money_num_all_day as arpu_percent_day_l
    FROM
	    default.tmp_app_arpu_day_p2;
    "

    echo '################平台7日arpu预统计数据####################'
    tmp_app_arpu_week_p2="
    CREATE TEMPORARY TABLE default.tmp_app_arpu_week_p2 AS
    SELECT
	    c.app_package_name,
	    c.app_money_week_u,
	    c.app_money_week_l,
	    d.user_cost_money_num_all_week
    FROM
	    (
		    SELECT
			    a.app_package_name,
			    (a.app_gift_money_week + b.app_guard_money_week_u) as app_money_week_u,
			    (a.app_gift_money_week + b.app_guard_money_week_l) as app_money_week_l
		    FROM
			    default.tmp_app_gift_money_week_p2 as a
		    FULL JOIN default.tmp_app_guard_money_week_p2 as b
		    ON a.app_package_name = b.app_package_name
	    ) as c
    FULL JOIN default.tmp_app_user_cost_money_num_all_week_p2 as d
    ON c.app_package_name = d.app_package_name;
    "

    echo '################平台7日arpu百分比例数据####################'
    tmp_app_arpu_percent_week_p2="
    CREATE TEMPORARY TABLE default.tmp_app_arpu_percent_week_p2 AS
    SELECT
	    app_package_name,
	    app_money_week_u/user_cost_money_num_all_week as arpu_percent_week_u,
	    app_money_week_l/user_cost_money_num_all_week as arpu_percent_week_l
    FROM
	    default.tmp_app_arpu_week_p2;
    "

    echo '################平台30日arpu预统计数据####################'
    tmp_app_arpu_month_p2="
    CREATE TEMPORARY TABLE default.tmp_app_arpu_month_p2 AS
    SELECT
	    c.app_package_name,
	    c.app_money_month_u,
	    c.app_money_month_l,
	    d.user_cost_money_num_all_month
    FROM
	    (
		    SELECT
			    a.app_package_name,
			    (a.app_gift_money_month + b.app_guard_money_month_u) as app_money_month_u,
			    (a.app_gift_money_month + b.app_guard_money_month_l) as app_money_month_l
		    FROM
			    default.tmp_app_gift_money_month_p2 as a
		    FULL JOIN default.tmp_app_guard_money_month_p2 as b
		    ON a.app_package_name = b.app_package_name
	    ) as c
    FULL JOIN default.tmp_app_user_cost_money_num_all_month_p2 as d
    ON c.app_package_name = d.app_package_name;
    "

    echo '################平台30日arpu百分比例数据####################'
    tmp_app_arpu_percent_month_p2="
    CREATE TEMPORARY TABLE default.tmp_app_arpu_percent_month_p2 AS
    SELECT
	    app_package_name,
	    app_money_month_u/user_cost_money_num_all_month as arpu_percent_month_u,
	    app_money_month_l/user_cost_money_num_all_month as arpu_percent_month_l
    FROM
	    default.tmp_app_arpu_month_p2;
    "

    echo '################平台累计主播数####################'
    tmp_app_user_num_all_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_num_all_p2 AS
    SELECT
            app_package_name,
            count(distinct user_id) as app_user_num_all
    FROM
            live_p2.tbl_ex_live_user_info_daily_snapshot_new
    WHERE
            dt = '${day}'
        AND app_package_name = 'com.meelive.ingkee'
    GROUP BY
            app_package_name;
    "

    echo '################平台每日新增活跃主播数####################'
    tmp_app_active_user_num_new_p2="
    CREATE TEMPORARY TABLE default.tmp_app_active_user_num_new_p2 AS
    SELECT
	    'com.meelive.ingkee' as app_package_name,
	    a.app_active_user_num - b.app_active_user_num as app_active_user_num_new
    FROM
	    (
		    SELECT
			    count(user_id) as app_active_user_num
		    FROM
			    live_p2.tbl_ex_live_user_known_daily_snapshot
		    WHERE
			    dt = '${day}'
		    AND app_package_name = 'com.meelive.ingkee'
		    GROUP BY
			    app_package_name
	    ) as a,
	    (
		    SELECT
			    count(user_id) as app_active_user_num
		    FROM
			    live_p2.tbl_ex_live_user_known_daily_snapshot
		    WHERE
			    dt = '${yesterday}'
		    AND app_package_name = 'com.meelive.ingkee'
		    GROUP BY
			    app_package_name
	    ) as b;
    "

    echo '################平台每日开播数####################'
    tmp_app_live_count_day_p2="
    CREATE TEMPORARY TABLE default.tmp_app_live_count_day_p2 AS
    SELECT
	    app_package_name,
	    sum(live_online_count) as app_live_count_day
    FROM
	    live_p2.tbl_ex_live_user_online_data_snapshot
	WHERE
	    dt = '${day}'
    AND app_package_name = 'com.meelive.ingkee'
	GROUP BY
	    app_package_name;
    "

    echo '################平台每日新增付费用户数####################'
    tmp_app_user_money_num_new_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_money_num_new_p2 AS
    SELECT
	    a.app_package_name,
	    (a.audience_money_num - if(b.audience_money_num is null,0,b.audience_money_num)) AS audience_money_num_new
    FROM
	    (
		    SELECT
			    app_package_name,
			    count(DISTINCT user_id) AS audience_money_num
		    FROM
			    (
				    SELECT
					    app_package_name,
					    audience_id AS user_id
				    FROM
					    live_p2.tbl_ex_live_audience_gift_daily_snapshot
				    WHERE
					    dt = '${day}'
				    AND app_package_name = 'com.meelive.ingkee'
				    UNION
				    	SELECT
						    app_package_name,
						    guarder_id AS user_id
					    FROM
						    live_p2.tbl_ex_live_guard_info_daily_snapshot
					    WHERE
						    dt = '${day}'
					    AND app_package_name = 'com.meelive.ingkee'
			    ) AS t
		    GROUP BY
			    app_package_name
	    ) AS a
    LEFT JOIN (
	    SELECT
		    app_package_name,
		    count(DISTINCT user_id) AS audience_money_num
	    FROM
		    (
			    SELECT
				    app_package_name,
				    audience_id AS user_id
			    FROM
				    live_p2.tbl_ex_live_audience_gift_daily_snapshot
			    WHERE
				    dt = '${yesterday}'
			    AND app_package_name = 'com.meelive.ingkee'
			    UNION
				    SELECT
					    app_package_name,
					    guarder_id AS user_id
				    FROM
					    live_p2.tbl_ex_live_guard_info_daily_snapshot
				    WHERE
					    dt = '${yesterday}'
				    AND app_package_name = 'com.meelive.ingkee'
		    ) AS t
	    GROUP BY
		    app_package_name
    ) AS b ON a.app_package_name = b.app_package_name;
    "


    echo "################# dump mysql 每日最大观众数 start ########################"
    mysql_sql="
    SELECT
	    app_package_name,
	    max(audience_count_current) as audience_num_max_day
    FROM
	    sage_bigdata.tbl_live_p2_app_count_all
    WHERE
	    time >= unix_timestamp('${day} 00:00:00')*1000 and time<= unix_timestamp('${day} 23:59:59')*1000
    AND app_package_name = 'com.meelive.ingkee '
    GROUP BY
	    app_package_name
    "
    ${mysql} -h${host} -P${port} -u${user} -p${password} --default-character-set=utf8 -e "${mysql_sql}" > ${tmp_mysql_result_file}
    hive_from_mysql_create_p2="
    CREATE TEMPORARY TABLE default.hive_from_mysql_create_p2(app_package_name string,audience_num_max_day int)
    row format delimited
    fields terminated by '\t'
    stored as textfile;
    "
    hive_from_mysql_load_p2="
    load data local inpath '${tmp_mysql_result_file}' overwrite into table default.hive_from_mysql_create_p2;
    "

    echo '################平台统计信息合并####################'
    app_save="insert overwrite local directory '${tmp_dir}/app' row format delimited fields terminated by ',' \
    SELECT
	    a.app_package_name,
	    a.active_user_num_day,
	    b.active_user_num_week,
	    c.active_user_num_month,
	    d.app_gift_money_day,
	    e.app_gift_money_week,
	    f.app_gift_money_month,
	    g.app_guard_money_day_u,
	    g.app_guard_money_day_l,
	    h.app_guard_money_week_u,
	    h.app_guard_money_week_l,
	    i.app_guard_money_month_u,
	    i.app_guard_money_month_l,
	    j.user_cost_money_num_all_day,
	    k.user_cost_money_num_all_week,
	    l.user_cost_money_num_all_month,
	    m.arpu_percent_day_u,
	    m.arpu_percent_day_l,
	    n.arpu_percent_week_u,
	    n.arpu_percent_week_l,
	    o.arpu_percent_month_u,
	    o.arpu_percent_month_l,
	    p.app_user_num_all,
	    q.app_active_user_num_new,
	    r.audience_num_max_day,
	    s.app_live_count_day,
	    t.audience_money_num_new
    FROM
	    default.tmp_app_active_user_day_p2 as a
    FULL JOIN default.tmp_app_active_user_week_p2 as b
		ON a.app_package_name = b.app_package_name
    FULL JOIN default.tmp_app_active_user_month_p2 as c
		ON a.app_package_name = c.app_package_name
    FULL JOIN default.tmp_app_gift_money_day_p2 as d
		ON a.app_package_name = d.app_package_name
    FULL JOIN default.tmp_app_gift_money_week_p2 as e
		ON a.app_package_name = e.app_package_name
    FULL JOIN default.tmp_app_gift_money_month_p2 as f
		ON a.app_package_name = f.app_package_name
	FULL JOIN default.tmp_app_guard_money_day_p2 as g
		ON a.app_package_name = g.app_package_name
	FULL JOIN default.tmp_app_guard_money_week_p2 as h
		ON a.app_package_name = h.app_package_name
	FULL JOIN default.tmp_app_guard_money_month_p2 as i
		ON a.app_package_name = i.app_package_name
	FULL JOIN default.tmp_app_user_cost_money_num_all_day_p2 as j
		ON a.app_package_name = j.app_package_name
	FULL JOIN default.tmp_app_user_cost_money_num_all_week_p2 as k
		ON a.app_package_name = k.app_package_name
	FULL JOIN default.tmp_app_user_cost_money_num_all_month_p2 as l
		ON a.app_package_name = l.app_package_name
	FULL JOIN default.tmp_app_arpu_percent_day_p2 as m
		ON a.app_package_name = m.app_package_name
	FULL JOIN default.tmp_app_arpu_percent_week_p2 as n
		ON a.app_package_name = n.app_package_name
	FULL JOIN default.tmp_app_arpu_percent_month_p2 as o
		ON a.app_package_name = o.app_package_name
	FULL JOIN default.tmp_app_user_num_all_p2 as p
		ON a.app_package_name = p.app_package_name
	FULL JOIN default.tmp_app_active_user_num_new_p2 as q
		ON a.app_package_name = q.app_package_name
	LEFT JOIN default.hive_from_mysql_create_p2 as r
	    ON a.app_package_name = r.app_package_name
	FULL JOIN default.tmp_app_live_count_day_p2 as s
	    ON a.app_package_name = s.app_package_name
	FULL JOIN default.tmp_app_user_money_num_new_p2 as t
	    ON a.app_package_name = t.app_package_name;
    "

    executeHiveCommand "
    ${tmp_app_active_user_day_p2}
    ${tmp_app_active_user_week_p2}
    ${tmp_app_active_user_month_p2}
    ${tmp_app_gift_money_day_p2}
    ${tmp_app_gift_money_week_p2}
    ${tmp_app_gift_money_month_p2}
    ${tmp_app_guard_money_day_p2}
    ${tmp_app_guard_money_week_p2}
    ${tmp_app_guard_money_month_p2}
    ${tmp_app_user_cost_money_num_all_day_p2}
    ${tmp_app_user_cost_money_num_all_week_p2}
    ${tmp_app_user_cost_money_num_all_month_p2}
    ${tmp_app_arpu_day_p2}
    ${tmp_app_arpu_percent_day_p2}
    ${tmp_app_arpu_week_p2}
    ${tmp_app_arpu_percent_week_p2}
    ${tmp_app_arpu_month_p2}
    ${tmp_app_arpu_percent_month_p2}
    ${tmp_app_user_num_all_p2}
    ${tmp_app_active_user_num_new_p2}
    ${tmp_app_live_count_day_p2}
    ${tmp_app_user_money_num_new_p2}
    ${hive_from_mysql_create_p2}
    ${hive_from_mysql_load_p2}
    ${app_save}
    "

echo '################平台统计结果输出####################'
sed -i "1i 包名,每日活跃主播,7日活跃主播,30日活跃主播,每日礼物流水金额,7日礼物流水金额,30日礼物流水金额,每日守护金额高,每日守护金额低,7日守护金额高,7日守护金额低,30日守护金额高,30日守护金额低,每日付费用户,7日付费用户,30日付费用户,每日arpu高,每日arpu低,7日arpu高,7日arpu低,30日arpu高,30日arpu低,全量主播,每日新增开播主播,每日最大在线观众数,每日开播数,每日新增付费用户数" ${tmp_dir}/app/000000_0
iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/000000_0 -o ${tmp_dir}/平台统计相关_${1}.csv
echo '################# 映客相关统计信息 end  ########################'
