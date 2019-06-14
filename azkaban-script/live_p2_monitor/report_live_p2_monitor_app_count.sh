#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
tomorrow=`date -d "+1 day $day" +%Y-%m-%d`
yesterday=`date -d "-1 day $day" +%Y-%m-%d`
week=`date -d "-6 day $day" +%Y-%m-%d`
month=`date -d "-29 day $day" +%Y-%m-%d`
tmp_dir=/tmp/monitor
tmp_live_app_audience_file=${tmp_dir}/mysql_live_app_audience.txt
tmp_live_app_count_file=${tmp_dir}/mysql_live_app_count.txt

echo '################# 直播监测1.2累计数据统计 start  ########################'

    echo "################# dump mysql 平台实时观众数########################"
    mysql_app_audience_sql="
    SELECT
	    app_package_name,
	    audience_count_current,
	    time
    FROM
	    sage_bigdata.tbl_live_p2_app_count_all
    WHERE
	    app_package_name IN (
		    SELECT
			    biz_name
		    FROM
			    sage_bigdata.tbl_live_p2_app_info
		    WHERE
			type = 1
	    ) and time >= unix_timestamp('${day} 00:00:00')*1000 and time<= unix_timestamp('${day} 23:59:59')*1000+999;
    "
    ${mysql} -h${host} -P${port} -u${user} -p${password} --default-character-set=utf8 -e "${mysql_app_audience_sql}" > ${tmp_live_app_audience_file}
    table_live_app_audience="
    CREATE TABLE IF NOT EXISTS live_p2.tbl_live_app_audience(app_package_name string,audience_count_current int,time bigint)
    row format delimited
    fields terminated by '\t'
    stored as textfile;
    "
    load_live_app_audience_p2="
    load data local inpath '${tmp_live_app_audience_file}' overwrite into table live_p2.tbl_live_app_audience;
    "

    echo "################# dump mysql 平台每日累计统计数据########################"
    mysql_app_count_sql="
    select app_package_name,sum(violating_num),replace('${day}','-','') as time from sage_bigdata.tbl_live_platform_monitor_day where time_string<replace('${day}','-','')  group by app_package_name,replace('${day}','-','')
    "
    ${mysql} -h${host} -P${port} -u${user} -p${password} --default-character-set=utf8 -e "${mysql_app_count_sql}" > ${tmp_live_app_count_file}
    table_live_app_count="
    CREATE TABLE IF NOT EXISTS live_p2.tbl_live_app_count(app_package_name string,case_count_all int,time string)
    row format delimited
    fields terminated by '\t'
    stored as textfile;
    "
    load_live_p2_app_count="
    load data local inpath '${tmp_live_app_count_file}' overwrite into table live_p2.tbl_live_app_count;
    "

    live_location_audience_max_output="
    SELECT
	    location,
	    cast(max(audience_count) as int) AS audience_count_max,
	    time_string
    FROM
	    (
		    SELECT
			    b.location_p AS location,
			    sum(a.audience_count_current) AS audience_count,
			    a.time,
			    from_unixtime(
				    floor(a.time / 1000),
				    'yyyyMMddHH'
			    ) AS time_string
		    FROM
			    live_p2.tbl_live_app_audience AS a
		    LEFT JOIN (
			    SELECT
				    biz_name,
				    location_p
			    FROM
				    live_p2.tbl_live_p2_app_location_all
			    GROUP BY
				    biz_name,
				    location_p
		    ) AS b ON a.app_package_name = b.biz_name
		    GROUP BY
			    a.time,
			    b.location_p
	    ) as c
	    WHERE location IS NOT NULL
    GROUP BY
	    c.location,
	    c.time_string
	UNION ALL
	SELECT
	    location,
	    cast(max(audience_count) as int) AS audience_count_max,
	    time_string
    FROM
	    (
		    SELECT
			    b.location_p AS location,
			    sum(a.audience_count_current) AS audience_count,
			    a.time,
			    from_unixtime(
				    floor(a.time / 1000),
				    'yyyyMMdd'
			    ) AS time_string
		    FROM
			    live_p2.tbl_live_app_audience AS a
		    LEFT JOIN (
			    SELECT
				    biz_name,
				    location_p
			    FROM
				    live_p2.tbl_live_p2_app_location_all
			    GROUP BY
				    biz_name,
				    location_p
		    ) AS b ON a.app_package_name = b.biz_name
		    GROUP BY
			    a.time,
			    b.location_p
	    ) as c
	    WHERE location IS NOT NULL
    GROUP BY
	    c.location,
	    c.time_string;
    "

    live_app_count_all_output="
    SELECT app_package_name, CAST(SUM(live_count_history) AS INT) AS live_count_history, CAST(SUM(active_user_num_history) AS INT) AS active_user_num_history, CAST(SUM(audience_gift_num_history) AS INT) AS audience_gift_num_history, CAST(SUM(gift_money_history) AS DOUBLE) AS gift_money_history
    	, CAST(SUM(violating_num_history) AS INT) AS violating_num_history, time_string
    FROM (
    	SELECT app_package_name, CAST(SUM(live_online_count) AS INT) AS live_count_history, CAST(0 AS INT) AS active_user_num_history, CAST(0 AS INT) AS audience_gift_num_history, CAST(0 AS DOUBLE) AS gift_money_history
    		, CAST(0 AS INT) AS violating_num_history, regexp_replace('${day}2359', '-', '') AS time_string
    	FROM live_p2.tbl_ex_live_user_online_data_daily_snapshot
    	WHERE dt = '${day}'
    		AND app_package_name IN (
    			SELECT biz_name
    			FROM live_p2.tbl_live_p2_app_info
    			WHERE type = 1
    		)
    	GROUP BY app_package_name
    	UNION ALL
    	SELECT app_package_name, CAST(0 AS INT) AS live_count_history, CAST(COUNT(DISTINCT user_id) AS INT) AS active_user_num_history, CAST(0 AS INT) AS audience_gift_num_history, CAST(0 AS DOUBLE) AS gift_money_history
    		, CAST(0 AS INT) AS violating_num_history, regexp_replace('${day}2359', '-', '') AS time_string
    	FROM live_p2.tbl_ex_live_user_known_daily_snapshot
    	WHERE dt = '${day}'
    		AND app_package_name IN (
    			SELECT biz_name
    			FROM live_p2.tbl_live_p2_app_info
    			WHERE type = 1
    		)
    	GROUP BY app_package_name
    	UNION ALL
    	SELECT app_package_name, CAST(0 AS INT) AS live_count_history, CAST(0 AS INT) AS active_user_num_history, CAST(COUNT(DISTINCT audience_id) AS INT) AS audience_gift_num_history, CAST(0 AS DOUBLE) AS gift_money_history
    		, CAST(0 AS INT) AS violating_num_history, regexp_replace('${day}2359', '-', '') AS time_string
    	FROM live_p2.tbl_ex_live_audience_gift_daily_snapshot
    	WHERE dt = '${day}'
    		AND app_package_name IN (
    			SELECT biz_name
    			FROM live_p2.tbl_live_p2_app_info
    			WHERE type = 1
    		)
    	GROUP BY app_package_name
    	UNION ALL
    	SELECT app_package_name, CAST(0 AS INT) AS live_count_history, CAST(0 AS INT) AS active_user_num_history, CAST(0 AS INT) AS audience_gift_num_history, CAST(0 AS DOUBLE) AS gift_money_history
    		, CAST(case_count_all AS INT) AS violating_num_history, regexp_replace('${day}2359', '-', '') AS time_string
    	FROM live_p2.tbl_live_app_count
    	WHERE app_package_name != 'app_package_name'
    	UNION ALL
    	SELECT a16.app_package_name, CAST(0 AS INT) AS live_count_history, CAST(0 AS INT) AS active_user_num_history, CAST(0 AS INT) AS audience_gift_num_history, CAST(0 AS DOUBLE) AS gift_money_history
    		, a16.violating_num AS violating_num_history, regexp_replace('${day}2359', '-', '') AS time_string
    	FROM (
    		SELECT from_unixtime(CAST(substr(CAST(start_time AS string), 0, 10) AS bigint), 'yyyyMMdd') AS time_string, app_package_name
    			, COUNT(DISTINCT order_id) AS violating_num
    		FROM ias_p2.tbl_ex_live_record_data_origin_orc
    		WHERE (dt >= '${yesterday}'
    			AND dt <= '${tomorrow}'
    			AND result_code = 0
    			AND end_time - start_time >= ${live_record_video_length}
    			AND from_unixtime(CAST(substr(CAST(start_time AS string), 0, 10) AS bigint), 'yyyy-MM-dd') = '${day}')
    		GROUP BY app_package_name, from_unixtime(CAST(substr(CAST(start_time AS string), 0, 10) AS bigint), 'yyyyMMdd')
    	) a16
    	UNION ALL
    	SELECT app_package_name, CAST(0 AS INT) AS live_count_history, CAST(0 AS INT) AS active_user_num_history, CAST(0 AS INT) AS audience_gift_num_history, CAST(if(SUM(gift_val) IS NULL, 0, SUM(gift_val)) AS DOUBLE) AS gift_money_history
    		, CAST(0 AS INT) AS violating_num_history, regexp_replace('${day}2359', '-', '') AS time_string
    	FROM ias_p2.tbl_ex_live_gift_info_orc
    	WHERE dt <= '${day}'
    		AND app_package_name IN (
    			SELECT biz_name
    			FROM live_p2.tbl_live_p2_app_info
    			WHERE type = 1
    		)
    	GROUP BY app_package_name
    ) t
    GROUP BY app_package_name, time_string;
    "

    executeHiveCommand "
    ${table_live_app_audience}
    ${load_live_app_audience_p2}
    ${table_live_app_count}
    ${load_live_p2_app_count}
    "

echo '################平台统计结果输出到mysql####################'

echo ${live_app_count_all_output}
hiveSqlToMysqlNoDeleteUTF8MB4 "${live_app_count_all_output}" "sage_bigdata.tbl_live_platform_monitor_minute" "app_package_name,live_count_history,active_user_num_history,audience_gift_num_history,gift_money_history,violating_num_history,time_string"

echo ${live_location_audience_max_output}
hiveSqlToMysqlNoDeleteUTF8MB4 "${live_location_audience_max_output}" "sage_bigdata.tbl_live_platform_monitor_region_audience" "location,audience_count_max,time_string"

echo '################# 直播监测1.2累计数据统计 end  ########################'
