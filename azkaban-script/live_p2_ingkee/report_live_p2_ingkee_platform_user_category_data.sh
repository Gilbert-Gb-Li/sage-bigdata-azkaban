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

     echo '################平台全量用户表####################'
    tmp_app_user_info_all_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_info_all_p2 AS
    SELECT app_package_name,
         user_id,
         gender,
         hometown,
         constellation,
         data_generate_time
    FROM live_p2.tbl_ex_live_user_info_daily_snapshot_new
    WHERE dt = '${day}' AND app_package_name = 'com.meelive.ingkee'
    UNION
    SELECT app_package_name,
         audience_user_id as user_id,
         audience_user_sex as gender,
         audience_user_hometown as hometown,
         null,
         data_generate_time
    FROM live_p2.tbl_ex_live_viewer_info_daily_snapshot
    WHERE dt = '${day}' AND app_package_name = 'com.meelive.ingkee';
    "

    echo '################平台全量用户性别表1####################'
    tmp_app_user_all_gender_1_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_gender_1_p2 AS
    SELECT
	    app_package_name,
	    gender,
	    count(distinct user_id) AS num
    FROM
	    (
		    SELECT
			    app_package_name,
			    user_id,
			    data_generate_time,
			CASE gender WHEN 0 THEN '男' WHEN 1 THEN '女' ELSE '其他' END AS gender
		    FROM
			    default.tmp_app_user_info_all_p2
	    ) AS a
    WHERE
	    EXISTS (
		    SELECT
			    1
		    FROM
			    (
				    SELECT
					    MAX(data_generate_time) data_generate_time,
					    app_package_name,
					    user_id
				    FROM
					    default.tmp_app_user_info_all_p2
				    GROUP BY
				        app_package_name,
					    user_id
			    ) b
		    WHERE
			    a.data_generate_time = b.data_generate_time
		    AND a.user_id = b.user_id
		    AND a.app_package_name = b.app_package_name
	    )
    GROUP BY
	    app_package_name,
	    gender;
    "

    echo '################平台全量用户性别表2####################'
    tmp_app_user_all_gender_2_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_gender_2_p2 AS
    SELECT
	    a.app_package_name,
	    a.gender,
	    a.num,
	    b.gender_num_all
    FROM
	    default.tmp_app_user_all_gender_1_p2 as a
    FULL JOIN (
	    SELECT
		    app_package_name,
		    sum(num) as gender_num_all
	    FROM
		    default.tmp_app_user_all_gender_1_p2
	    GROUP BY
		    app_package_name
    ) as b ON a.app_package_name = b.app_package_name;
    "

    echo '################平台全量用户性别比例####################'
    tmp_app_user_all_gender_percent_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_gender_percent_p2 AS
    SELECT
	    app_package_name,
	    gender,
	    num,
	    num/gender_num_all as gender_percent
    FROM
	    default.tmp_app_user_all_gender_2_p2;
    "

    echo '################平台全量用户地区表1####################'
    tmp_app_user_all_hometown_1_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_hometown_1_p2 AS
    SELECT
	    app_package_name,
	    hometown,
	    sum(num) as num
    FROM
	    (
		    SELECT
			    t3.app_package_name,
			    COALESCE (
				    t3.province,
				    t4.province,
				    '其它'
			    ) AS hometown,
			    num
		    FROM
			    (
				    SELECT
					    t1.app_package_name,
					    t1.num,
					    substr(t1.hometown, 1, 2) AS hometown,
					    t2.province
				    FROM
					    (
						    SELECT
							    app_package_name,
							    substr(hometown, 1, 3) AS hometown,
							    count(distinct user_id) AS num
						    FROM
							    default.tmp_app_user_info_all_p2 a
						    WHERE
							    EXISTS (
								    SELECT
									    1
								    FROM
									    (
										    SELECT
											    MAX(data_generate_time) data_generate_time,
											    app_package_name,
											    user_id
										    FROM
											    default.tmp_app_user_info_all_p2
										    GROUP BY
										        app_package_name,
											    user_id
									    ) b
								    WHERE
									    a.data_generate_time = b.data_generate_time
								    AND a.user_id = b.user_id
								    AND a.app_package_name = b.app_package_name
							    )
						    GROUP BY
							    app_package_name,
							    hometown
					    ) AS t1
				    LEFT JOIN live_p2.tbl_util_location AS t2 ON t1.hometown = t2.city
			    ) AS t3
		    LEFT JOIN live_p2.tbl_util_location AS t4 ON t3.hometown = t4.city
	    ) as result GROUP BY app_package_name,hometown;
    "

    echo '################平台全量用户地区表2####################'
    tmp_app_user_all_hometown_2_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_hometown_2_p2 AS
    SELECT
	    a.app_package_name,
	    a.hometown,
	    a.num,
	    b.hometown_num_all
    FROM
	    default.tmp_app_user_all_hometown_1_p2 as a
    FULL JOIN (
	    SELECT
		    app_package_name,
		    sum(num) as hometown_num_all
	    FROM
		    default.tmp_app_user_all_hometown_1_p2
	    GROUP BY
		    app_package_name
    ) as b ON a.app_package_name = b.app_package_name;
    "
    echo '################平台全量用户地区比例####################'
    tmp_app_user_all_hometown_percent_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_hometown_percent_p2 AS
    SELECT
	    app_package_name,
	    hometown,
	    num,
	    num/hometown_num_all as hometown_percent
    FROM
	    default.tmp_app_user_all_hometown_2_p2;
    "


    echo '################平台全量用户星座表1####################'
    tmp_app_user_all_constellation_1_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_constellation_1_p2 AS
    SELECT
	    app_package_name,
	    if(constellation is null,'其它',constellation) as constellation,
	    count(distinct user_id) as num
    FROM
	    default.tmp_app_user_info_all_p2 a
    WHERE
    	EXISTS (
		    SELECT
			    1
		    FROM
			    (
				    SELECT
					    MAX(data_generate_time) data_generate_time,
					    app_package_name,
					    user_id
				    FROM
					    default.tmp_app_user_info_all_p2
				    GROUP BY
				        app_package_name,
					    user_id
			    ) b
		    WHERE
			    a.data_generate_time = b.data_generate_time
		    AND a.user_id = b.user_id
		    AND a.app_package_name = b.app_package_name
	    ) GROUP BY app_package_name,constellation;
    "

    echo '################平台全量用户星座表2####################'
    tmp_app_user_all_constellation_2_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_constellation_2_p2 AS
    SELECT
	    a.app_package_name,
	    a.constellation,
	    a.num,
	    b.constellation_num_all
    FROM
	    default.tmp_app_user_all_constellation_1_p2 as a
    FULL JOIN (
	    SELECT
		    app_package_name,
		    sum(num) as constellation_num_all
	    FROM
		    default.tmp_app_user_all_constellation_1_p2
	    GROUP BY
		    app_package_name
    ) as b ON a.app_package_name = b.app_package_name;
    "

    echo '################平台全量用户星座比例####################'
    tmp_app_user_all_constellation_percent_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_all_constellation_percent_p2 AS
    SELECT
	    app_package_name,
	    constellation,
	    num,
	    num/constellation_num_all as constellation_percent
    FROM
	    default.tmp_app_user_all_constellation_2_p2;
    "

    echo '################平台每日开播主播性别表1####################'
    tmp_app_user_live_gender_1_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_gender_1_p2 AS
    SELECT
        app_package_name,
        CASE sex WHEN 0 THEN '男' WHEN 1 THEN '女' ELSE '其他' END AS gender,
	    count(distinct user_id) as num
    FROM
	    ias_p2.tbl_ex_live_user_info_data_origin_orc a
    WHERE
    	EXISTS (
		    SELECT
			    1
		    FROM
			    (
				    SELECT
					    MAX(data_generate_time) data_generate_time,
					    app_package_name,
					    user_id
				    FROM
					    ias_p2.tbl_ex_live_user_info_data_origin_orc
					WHERE dt = '${day}'
					AND app_package_name = 'com.meelive.ingkee'
					AND is_live = 1
				    GROUP BY
				        app_package_name,
					    user_id
			    ) b
		    WHERE
			    a.data_generate_time = b.data_generate_time
		    AND a.user_id = b.user_id
		    AND a.app_package_name = b.app_package_name
	    ) GROUP BY app_package_name,sex;
	    "

    echo '################平台每日开播主播性别表2####################'
    tmp_app_user_live_gender_2_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_gender_2_p2 AS
    SELECT
	    a.app_package_name,
	    a.gender,
	    a.num,
	    b.gender_num_all
    FROM
	    default.tmp_app_user_live_gender_1_p2 as a
    FULL JOIN (
	    SELECT
		    app_package_name,
		    sum(num) as gender_num_all
	    FROM
		    default.tmp_app_user_live_gender_1_p2
	    GROUP BY
		    app_package_name
    ) as b ON a.app_package_name = b.app_package_name;
    "

    echo '################平台每日开播主播性别比例####################'
    tmp_app_user_live_gender_percent_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_gender_percent_p2 AS
    SELECT
	    app_package_name,
	    gender,
	    num,
	    num/gender_num_all as gender_percent
    FROM
	    default.tmp_app_user_live_gender_2_p2;
    "

echo '################平台每日开播主播地区表1####################'
    tmp_app_user_live_hometown_1_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_hometown_1_p2 AS
    SELECT
	    app_package_name,
	    hometown,
	    sum(num) AS num
    FROM
	    (
		    SELECT
			    t3.app_package_name,
			    COALESCE (
				    t3.province,
				    t4.province,
				    '其它'
			    ) AS hometown,
			    num
		    FROM
			    (
				    SELECT
					    t1.app_package_name,
					    t1.num,
					    substr(t1.hometown, 1, 2) AS hometown,
					    t2.province
				    FROM
					    (
						    SELECT
							    app_package_name,
							    substr(hometown, 1, 3) AS hometown,
							    count(distinct user_id) AS num
						    FROM
							    ias_p2.tbl_ex_live_user_info_data_origin_orc a
						    WHERE
							    EXISTS (
								    SELECT
									    1
								    FROM
									    (
										    SELECT
											    MAX(data_generate_time) data_generate_time,
											    app_package_name,
											    user_id
										    FROM
											    ias_p2.tbl_ex_live_user_info_data_origin_orc
										    WHERE dt = '${day}'
										    AND app_package_name = 'com.meelive.ingkee'
										    AND is_live = 1
										    GROUP BY
										        app_package_name,
											    user_id
									    ) b
								    WHERE
									    a.data_generate_time = b.data_generate_time
								    AND a.user_id = b.user_id
								    AND a.app_package_name = b.app_package_name
							    )
						    GROUP BY
							    app_package_name,
							    hometown
					    ) AS t1
				    LEFT JOIN live_p2.tbl_util_location AS t2 ON t1.hometown = t2.city
			    ) AS t3
		    LEFT JOIN live_p2.tbl_util_location AS t4 ON t3.hometown = t4.city
	    ) AS result
    GROUP BY
	    app_package_name,
	    hometown;
	 "

    echo '################平台每日开播主播地区表2####################'
    tmp_app_user_live_hometown_2_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_hometown_2_p2 AS
    SELECT
	    a.app_package_name,
	    a.hometown,
	    a.num,
	    b.hometown_num_all
    FROM
	    default.tmp_app_user_live_hometown_1_p2 as a
    FULL JOIN (
	    SELECT
		    app_package_name,
		    sum(num) as hometown_num_all
	    FROM
		    default.tmp_app_user_live_hometown_1_p2
	    GROUP BY
		    app_package_name
    ) as b ON a.app_package_name = b.app_package_name;
    "

    echo '################平台每日开播主播地区比例####################'
    tmp_app_user_live_hometown_percent_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_hometown_percent_p2 AS
    SELECT
	    app_package_name,
	    hometown,
	    num,
	    num/hometown_num_all as hometown_percent
    FROM
	    default.tmp_app_user_live_hometown_2_p2;
    "

    echo '################平台每日开播主播星座表1####################'
    tmp_app_user_live_constellation_1_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_constellation_1_p2 AS
    SELECT
        app_package_name,
	    if(constellation is null,'其它',constellation) as constellation,
	    count(distinct user_id) as num
    FROM
	    ias_p2.tbl_ex_live_user_info_data_origin_orc a
    WHERE
    	EXISTS (
		    SELECT
			    1
		    FROM
			    (
				    SELECT
					    MAX(data_generate_time) data_generate_time,
					    app_package_name,
					    user_id
				    FROM
					    ias_p2.tbl_ex_live_user_info_data_origin_orc
				    WHERE dt = '${day}'
					AND app_package_name = 'com.meelive.ingkee'
					AND is_live = 1
				    GROUP BY
				        app_package_name,
					    user_id
			    ) b
		    WHERE
			    a.data_generate_time = b.data_generate_time
		    AND a.user_id = b.user_id
		    AND a.app_package_name = b.app_package_name
	    ) GROUP BY app_package_name,constellation;
	    "

    echo '################平台每日开播主播星座表2####################'
    tmp_app_user_live_constellation_2_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_constellation_2_p2 AS
    SELECT
	    a.app_package_name,
	    a.constellation,
	    a.num,
	    b.constellation_num_all
    FROM
	    default.tmp_app_user_live_constellation_1_p2 as a
    FULL JOIN (
	    SELECT
		    app_package_name,
		    sum(num) as constellation_num_all
	    FROM
		    default.tmp_app_user_live_constellation_1_p2
	    GROUP BY
		    app_package_name
    ) as b ON a.app_package_name = b.app_package_name;
    "

    echo '################平台每日开播主播星座比例####################'
    tmp_app_user_live_constellation_percent_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_constellation_percent_p2 AS
    SELECT
	    app_package_name,
	    constellation,
	    num,
	    num/constellation_num_all as constellation_percent
    FROM
	    default.tmp_app_user_live_constellation_2_p2;
    "

    echo '################平台直播时长分布表####################'
    tmp_app_user_live_interval_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_interval_p2 AS
    SELECT
	    t1.app_package_name,
	    t1.category,
	    t1.num,
	    t2.user_all_count
    FROM
	    (
		    SELECT
			    app_package_name,
			    category,
			    count(user_id) AS num
		    FROM
			    (
				    SELECT
					    app_package_name,
					    user_id,
					    CASE
				    WHEN live_online_length >= 0
				    AND live_online_length < 3600000 THEN
					    '0-1小时人数'
				    WHEN live_online_length >= 3600000
				    AND live_online_length < 7200000 THEN
					    '1-2小时人数'
				    WHEN live_online_length >= 7200000
				    AND live_online_length < 10800000 THEN
					    '2-3小时人数'
				    WHEN live_online_length >= 10800000
				    AND live_online_length < 14400000 THEN
					    '3-4小时人数'
				    WHEN live_online_length >= 14400000
				    AND live_online_length < 18000000 THEN
					    '4-5小时人数'
				    WHEN live_online_length >= 18000000
				    AND live_online_length < 21600000 THEN
					    '5-6小时人数'
				    WHEN live_online_length >= 21600000
				    AND live_online_length < 86400000 THEN
					    '6-24小时人数'
				    END AS category
				    FROM
					    live_p2.tbl_ex_live_user_online_data_snapshot
				    WHERE
					    dt = '${day}'
				    AND app_package_name = 'com.meelive.ingkee'
			    ) AS a
		    GROUP BY
			    app_package_name,
			    category
	    ) AS t1
    LEFT JOIN (
	    SELECT
		    app_package_name,
		    count(DISTINCT user_id) AS user_all_count
	    FROM
		    live_p2.tbl_ex_live_user_online_data_snapshot
	    WHERE
		    dt = '${day}'
	    AND app_package_name = 'com.meelive.ingkee'
	    GROUP BY
		    app_package_name
    ) AS t2 ON t1.app_package_name = t2.app_package_name;
    "

    echo '################平台直播时长分布比例####################'
    tmp_app_user_live_interval_percent_p2="
    CREATE TEMPORARY TABLE default.tmp_app_user_live_interval_percent_p2 AS
    SELECT
	    app_package_name,
	    category,
	    num,
	    num/user_all_count as live_length_percent
    FROM
	    default.tmp_app_user_live_interval_p2;
    "


    echo '################平台全量用户分类比例统计合并####################'
    app_user_all_category_save="insert overwrite local directory '${tmp_dir}/app/userall/category' row format delimited fields terminated by ',' \
    SELECT
	    app_package_name,
	    cast(gender as string) as category,
	    num,
	    gender_percent as num_percent
    FROM
    	default.tmp_app_user_all_gender_percent_p2
    UNION ALL
	    SELECT
		    app_package_name,
		    hometown as category,
		    num,
		    hometown_percent as num_percent
	    FROM
		    default.tmp_app_user_all_hometown_percent_p2
	    UNION ALL
		    SELECT
			    app_package_name,
			    constellation as category,
			    num,
			    constellation_percent as num_percent
		    FROM
			    default.tmp_app_user_all_constellation_percent_p2;
    "

    echo '################平台每日开播主播分类比例统计合并####################'
    app_user_live_category_save="insert overwrite local directory '${tmp_dir}/app/userlive/category' row format delimited fields terminated by ',' \
    SELECT
	    app_package_name,
	    cast(gender as string) as category,
	    num,
	    gender_percent as num_percent
    FROM
	    default.tmp_app_user_live_gender_percent_p2
    UNION ALL
	    SELECT
		    app_package_name,
		    hometown as category,
		    num,
		    hometown_percent as num_percent
	    FROM
		    default.tmp_app_user_live_hometown_percent_p2
	    UNION ALL
		    SELECT
			    app_package_name,
			    constellation as category,
			    num,
			    constellation_percent as num_percent
		    FROM
			    default.tmp_app_user_live_constellation_percent_p2;
    "

    echo '################平台直播时长分布比例统计合并####################'
    app_user_live_interval_save="insert overwrite local directory '${tmp_dir}/app/userlive/live-interval' row format delimited fields terminated by ',' \
    SELECT
        *
    FROM
        default.tmp_app_user_live_interval_percent_p2;
    "

    executeHiveCommand "
    ${tmp_app_user_info_all_p2}
    ${tmp_app_user_all_gender_1_p2}
    ${tmp_app_user_all_gender_2_p2}
    ${tmp_app_user_all_gender_percent_p2}
    ${tmp_app_user_all_hometown_1_p2}
    ${tmp_app_user_all_hometown_2_p2}
    ${tmp_app_user_all_hometown_percent_p2}
    ${tmp_app_user_all_constellation_1_p2}
    ${tmp_app_user_all_constellation_2_p2}
    ${tmp_app_user_all_constellation_percent_p2}
    ${tmp_app_user_live_gender_1_p2}
    ${tmp_app_user_live_gender_2_p2}
    ${tmp_app_user_live_gender_percent_p2}
    ${tmp_app_user_live_hometown_1_p2}
    ${tmp_app_user_live_hometown_2_p2}
    ${tmp_app_user_live_hometown_percent_p2}
    ${tmp_app_user_live_constellation_1_p2}
    ${tmp_app_user_live_constellation_2_p2}
    ${tmp_app_user_live_constellation_percent_p2}
    ${tmp_app_user_live_interval_p2}
    ${tmp_app_user_live_interval_percent_p2}
    ${app_user_all_category_save}
    ${app_user_live_category_save}
    ${app_user_live_interval_save}
    "

echo '################平台全量用户分类比例结果输出####################'
sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userall/category/1/000000_0
iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userall/category/1/000000_0 -o ${tmp_dir}/全量用户性别比例_${1}.csv

sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userall/category/2/000000_0
iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userall/category/2/000000_0 -o ${tmp_dir}/全量用户地区比例_${1}.csv

sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userall/category/3/000000_0
iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userall/category/3/000000_0 -o ${tmp_dir}/全量用户地区比例_${1}.csv

echo '################平台每日开播主播分类比例结果输出####################'
sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userlive/category/1/000000_0
iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userlive/category/1/000000_0 -o ${tmp_dir}/每日开播主播性别比例_${1}.csv

sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userlive/category/2/000000_0
iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userlive/category/2/000000_0 -o ${tmp_dir}/每日开播主播地区比例_${1}.csv

sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userlive/category/3/000000_0
iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userlive/category/3/000000_0 -o ${tmp_dir}/每日开播主播星座比例_${1}.csv

sed -i "1i 包名,类别,人数,比例" ${tmp_dir}/app/userlive/live-interval/000000_0
iconv -f UTF-8 -t GBK -c ${tmp_dir}/app/userlive/live-interval/000000_0 -o ${tmp_dir}/主播每日直播时长分布比例_${1}.csv

echo '################# 映客相关统计信息 end  ########################'
