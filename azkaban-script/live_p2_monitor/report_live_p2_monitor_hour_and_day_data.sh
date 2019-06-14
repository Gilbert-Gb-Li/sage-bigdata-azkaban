#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
tomorrow=`date -d "+1 day $date" +%Y-%m-%d`
#每月第一天
first_day=`date -d "$date" +%Y-%m`-01
#30天之前
thirty_day=`date -d "-29 day $date" +%Y-%m-%d`
echo 'thirty_day='${thirty_day}
week_stat_day=${date}
week_day_num=`date -d "${date}" +%w`
if [ ${week_day_num} == 0 ];then
   week_stat_day=`date -d "-6 day $date" +%Y-%m-%d`
else
   week_stat_day=`date -d "-$[ ${week_day_num} - 1] day $date" +%Y-%m-%d`
fi
echo 'week_stat_day='${week_stat_day}


echo "################# dump mysql 平台实时数据 start ########################"
tmp_live_app_count_all_file=${localDir}/tbl_live_p2_app_count_all_data_${date}.txt
tmp_live_app_count_all_file_cype=${localDir}/part-123456
    mysql_app_count_all_sql="
    SELECT
	    id,app_package_name,live_count_current,live_count_daily,live_count_all,user_count_current,user_count_daily,user_count_all,
        audience_count_current,audience_count_daily,money_count_current,money_count_daily,money_count_all,
        case_count_daily,case_count_all,time
    FROM
	    sage_bigdata.tbl_live_p2_app_count_all
    WHERE time >= unix_timestamp('${date} 00:00:00')*1000 and time<= unix_timestamp('${date} 23:59:59')*1000+999;
    "
    ${mysql} -h${host} -P${port} -u${user} -p${password} --default-character-set=utf8 -e "${mysql_app_count_all_sql}" > ${tmp_live_app_count_all_file}
    sed '1d' ${tmp_live_app_count_all_file} > ${tmp_live_app_count_all_file_cype}
    load_app_count_all="load data local inpath '${tmp_live_app_count_all_file_cype}' overwrite into table live_p2.tbl_ex_live_p2_app_count_all_data PARTITION(dt='${date}');"
    echo ${load_app_count_all}
    hive -e "${load_app_count_all}"
    rm -rf ${tmp_live_app_count_all_file} ${tmp_live_app_count_all_file_cype}
echo "################# dump mysql 平台实时数据  end ########################"

echo "############### 直播监控  每小时报表统计 start #####################"
mysql_platform_monitor_hour_table="tbl_live_platform_monitor_hour"

hive_sql="
SELECT b.time_string,b.data_source,b.app_package_name,
       sum(b.user_num) as user_num,
       sum(b.audience_gift_num) as audience_gift_num,
       sum(b.gift_money) as gift_money,
       sum(b.violating_num) as violating_num,
       if(sum(b.user_num)>0,sum(b.online_user_num_max),0)as online_user_num_max,
       sum(b.live_count) as live_count
FROM(
    SELECT a1.time_string,
           '${ias_source}' as data_source,
           a1.app_package_name,
           a1.user_num,
           cast(0 as int) as audience_gift_num,
           cast(0 as double) as gift_money,
           cast(0 as int) as violating_num,
           cast(0 as int) as online_user_num_max,
           cast(0 as int) as live_count
    FROM(
         select concat(regexp_replace(dt,'-',''),hour) as time_string,app_package_name,count(distinct user_id) as user_num
         from ias_p2.tbl_ex_live_user_info_data_origin_orc
         where dt='${date}' and is_live=1 group by app_package_name,dt,hour
    ) a1
    UNION ALL
    SELECT a2.time_string,
           '${ias_source}' as data_source,
           a2.app_package_name,
           cast(0 as int) as user_num,
           a2.audience_gift_num,
           cast(0 as double) as gift_money,
           cast(0 as int) as violating_num,
           cast(0 as int) as online_user_num_max,
           cast(0 as int) as live_count
    FROM( select concat(regexp_replace(dt,'-',''),hour) as time_string,app_package_name,count(distinct audience_id) as audience_gift_num
          from ias_p2.tbl_ex_live_gift_info_orc
          where dt='${date}' and audience_id is not null group by app_package_name,dt,hour
    ) a2
    UNION ALL
    SELECT a3.time_string,
           '${ias_source}' as data_source,
           a3.app_package_name,
           cast(0 as int) as user_num,
           cast(0 as int) as audience_gift_num,
           a3.gift_money,
           cast(0 as int) as violating_num,
           cast(0 as int) as online_user_num_max,
           cast(0 as int) as live_count
    FROM( select concat(regexp_replace(dt,'-',''),hour) as time_string,app_package_name,sum(gift_val) as gift_money
          from ias_p2.tbl_ex_live_gift_info_orc
          where dt='${date}' and gift_val is not null and gift_val>0 group by app_package_name,dt,hour
    ) a3
    UNION ALL
    SELECT a4.time_string,
           '${ias_source}' as data_source,
           a4.app_package_name,
           cast(0 as int) as user_num,
           cast(0 as int) as audience_gift_num,
           cast(0 as double) as gift_money,
           a4.violating_num,
           cast(0 as int) as online_user_num_max,
           cast(0 as int) as live_count
    FROM(   SELECT from_unixtime(cast(substr(cast(start_time as string),0,10) as bigint),'yyyyMMddHH') as time_string,app_package_name, count(distinct order_id) AS violating_num
            FROM ias_p2.tbl_ex_live_record_data_origin_orc
            WHERE dt>='${yesterday}' and dt<='${tomorrow}' and result_code=0 and (end_time-start_time)>=${live_record_video_length}
                  and from_unixtime(cast(substr(cast(start_time as string),0,10) as bigint),'yyyy-MM-dd')='${date}'
            GROUP BY app_package_name,from_unixtime(cast(substr(cast(start_time as string),0,10) as bigint),'yyyyMMddHH')
    ) a4
    UNION ALL
    SELECT a5.time_string,
           '${ias_source}' as data_source,
           a5.app_package_name,
           cast(0 as int) as user_num,
           cast(0 as int) as audience_gift_num,
           cast(0 as double) as gift_money,
           cast(0 as int) as violating_num,
           a5.online_user_num_max,
           cast(0 as int) as live_count
    FROM(
         SELECT app_package_name, max(audience_count_current) AS online_user_num_max, from_unixtime( floor(time / 1000), 'yyyyMMddHH') AS time_string
         FROM live_p2.tbl_live_app_audience
         GROUP BY app_package_name,from_unixtime( floor(time / 1000), 'yyyyMMddHH')
    ) a5
    UNION ALL
    SELECT a6.time_string,
           '${ias_source}' as data_source,
           a6.app_package_name,
           cast(0 as int) as user_num,
           cast(0 as int) as audience_gift_num,
           cast(0 as double) as gift_money,
           cast(0 as int) as violating_num,
           cast(0 as int) as online_user_num_max,
           a6.live_count
    FROM(
         SELECT from_unixtime(CAST(substr(CAST(time AS string), 0, 10) AS bigint), 'yyyyMMddHH') AS time_string, app_package_name, SUM(live_count_current) AS live_count
         FROM live_p2.tbl_ex_live_p2_app_count_all_data
         WHERE dt = '${date}'
         GROUP BY app_package_name, from_unixtime(CAST(substr(CAST(time AS string), 0, 10) AS bigint), 'yyyyMMddHH')
    ) a6
) b
WHERE b.app_package_name in (${report_live_app_list})
GROUP BY b.time_string,b.data_source,b.app_package_name
"
echo ${mysql_platform_monitor_hour_table}
hiveSqlToMysql "${hive_sql}" "" "${mysql_platform_monitor_hour_table}" "time_string,data_source,app_package_name,user_num,audience_gift_num,gift_money,violating_num,online_user_num_max,live_count" "time_string"

echo "############### 直播监控 每小时报表统计 end #####################"



echo "############### 直播监控  每天报表统计 start #####################"
mysql_platform_monitor_day_table="tbl_live_platform_monitor_day"

hive_sql_2="
SELECT b.time_string,b.data_source,b.app_package_name,
       sum(active_user_num) as active_user_num,
       sum(live_count) as live_count,
       if(sum(active_user_num)>0,sum(online_user_num_max),0) as online_user_num_max,
       sum(audience_gift_num) as audience_gift_num,
       sum(gift_money) as gift_money,
       sum(violating_num) as violating_num,
       sum(active_user_num_month_natural) as active_user_num_month_natural,
       sum(audience_gift_num_month_natural) as audience_gift_num_month_natural,
       sum(active_user_num_thirty_day) as active_user_num_thirty_day,
       sum(active_user_num_weeks_natural) as active_user_num_weeks_natural
FROM(
    SELECT a1.time_string,
           a1.data_source,
           a1.app_package_name,
           a1.active_user_num,
           cast(0 as int) as live_count,
           cast(0 as int) as online_user_num_max,
           cast(0 as int) as audience_gift_num,
           cast(0 as double) as gift_money,
           cast(0 as int) as violating_num,
           cast(0 as int) as active_user_num_month_natural,
           cast(0 as int) as audience_gift_num_month_natural,
           cast(0 as int) as active_user_num_thirty_day,
           cast(0 as int) as active_user_num_weeks_natural
    FROM(
          select regexp_replace(dt,'-','') as time_string,data_source,app_package_name,count(distinct user_id) as active_user_num
          from live_p2.tbl_ex_live_user_known_daily_snapshot
          where dt='${date}' and from_unixtime(cast(substr(cast(data_generate_time as string),0,10) as bigint),'yyyy-MM-dd')='${date}'
          group by data_source,app_package_name,dt
    ) a1
    UNION ALL
    SELECT a2.time_string,
           a2.data_source,
           a2.app_package_name,
           cast(0 as int) as active_user_num,
           a2.live_count,
           cast(0 as int) as online_user_num_max,
           cast(0 as int) as audience_gift_num,
           cast(0 as double) as gift_money,
           cast(0 as int) as violating_num,
           cast(0 as int) as active_user_num_month_natural,
           cast(0 as int) as audience_gift_num_month_natural,
           cast(0 as int) as active_user_num_thirty_day,
           cast(0 as int) as active_user_num_weeks_natural
    FROM(
          select regexp_replace(dt,'-','') as time_string,data_source,app_package_name,sum(live_online_count) as live_count
          from live_p2.tbl_ex_live_user_online_data_snapshot
          where dt='${date}' and from_unixtime(cast(substr(cast(data_generate_time as string),0,10) as bigint),'yyyy-MM-dd')='${date}'
          group by data_source,app_package_name,dt
    ) a2
    UNION ALL
    SELECT a3.time_string,
           '${ias_source}' as data_source,
           a3.app_package_name,
           cast(0 as int) as active_user_num,
           cast(0 as int) as live_count,
           a3.online_user_num_max,
           cast(0 as int) as audience_gift_num,
           cast(0 as double) as gift_money,
           cast(0 as int) as violating_num,
           cast(0 as int) as active_user_num_month_natural,
           cast(0 as int) as audience_gift_num_month_natural,
           cast(0 as int) as active_user_num_thirty_day,
           cast(0 as int) as active_user_num_weeks_natural
    FROM(
         SELECT app_package_name, max(audience_count_current) AS online_user_num_max, from_unixtime( floor(time / 1000), 'yyyyMMdd') AS time_string
         FROM live_p2.tbl_live_app_audience
         WHERE from_unixtime( floor(time / 1000), 'yyyy-MM-dd')='${date}'
         GROUP BY app_package_name,from_unixtime( floor(time / 1000), 'yyyyMMdd')
    ) a3
    UNION ALL
    SELECT a4.time_string,
           a4.data_source,
           a4.app_package_name,
           cast(0 as int) as active_user_num,
           cast(0 as int) as live_count,
           cast(0 as int) as online_user_num_max,
           a4.audience_gift_num,
           cast(0 as double) as gift_money,
           cast(0 as int) as violating_num,
           cast(0 as int) as active_user_num_month_natural,
           cast(0 as int) as audience_gift_num_month_natural,
           cast(0 as int) as active_user_num_thirty_day,
           cast(0 as int) as active_user_num_weeks_natural
    FROM(
          select regexp_replace(dt,'-','') as time_string,data_source,app_package_name,count(distinct audience_id) as audience_gift_num
          from live_p2.tbl_ex_live_audience_gift_daily_snapshot
          where dt='${date}' and from_unixtime(cast(substr(cast(data_generate_time as string),0,10) as bigint),'yyyy-MM-dd')='${date}'
          group by data_source,app_package_name,dt
    ) a4
    UNION ALL
    SELECT a5.time_string,
           '${ias_source}' as data_source,
           a5.app_package_name,
           cast(0 as int) as active_user_num,
           cast(0 as int) as live_count,
           cast(0 as int) as online_user_num_max,
           cast(0 as int) as audience_gift_num,
           a5.gift_money,
           cast(0 as int) as violating_num,
           cast(0 as int) as active_user_num_month_natural,
           cast(0 as int) as audience_gift_num_month_natural,
           cast(0 as int) as active_user_num_thirty_day,
           cast(0 as int) as active_user_num_weeks_natural
    FROM(
          select regexp_replace(dt,'-','') as time_string,app_package_name,sum(gift_val) as gift_money
          from ias_p2.tbl_ex_live_gift_info_orc
          where dt='${date}' and gift_val is not null and gift_val>0 group by app_package_name,dt
    ) a5
    UNION ALL
    SELECT a6.time_string,
           '${ias_source}' as data_source,
           a6.app_package_name,
           cast(0 as int) as active_user_num,
           cast(0 as int) as live_count,
           cast(0 as int) as online_user_num_max,
           cast(0 as int) as audience_gift_num,
           cast(0 as double) as gift_money,
           a6. violating_num,
           cast(0 as int) as active_user_num_month_natural,
           cast(0 as int) as audience_gift_num_month_natural,
           cast(0 as int) as active_user_num_thirty_day,
           cast(0 as int) as active_user_num_weeks_natural
    FROM(
            SELECT from_unixtime(cast(substr(cast(start_time as string),0,10) as bigint),'yyyyMMdd') as time_string,app_package_name, count(distinct order_id) AS violating_num
            FROM ias_p2.tbl_ex_live_record_data_origin_orc
            WHERE dt>='${yesterday}' and dt<='${tomorrow}' and  result_code=0 and (end_time-start_time)>=${live_record_video_length}
                  and from_unixtime(cast(substr(cast(start_time as string),0,10) as bigint),'yyyy-MM-dd')='${date}'
            GROUP BY app_package_name,from_unixtime(cast(substr(cast(start_time as string),0,10) as bigint),'yyyyMMdd')
    ) a6
    UNION ALL
    SELECT a7.time_string,
           a7.data_source,
           a7.app_package_name,
           cast(0 as int) as active_user_num,
           cast(0 as int) as live_count,
           cast(0 as int) as online_user_num_max,
           cast(0 as int) as audience_gift_num,
           cast(0 as double) as gift_money,
           cast(0 as int) as violating_num,
           a7.active_user_num_month_natural,
           cast(0 as int) as audience_gift_num_month_natural,
           cast(0 as int) as active_user_num_thirty_day,
           cast(0 as int) as active_user_num_weeks_natural
    FROM(
          select regexp_replace(dt,'-','') as time_string,data_source,app_package_name,count(distinct user_id) as active_user_num_month_natural
          from live_p2.tbl_ex_live_user_known_daily_snapshot
          where dt='${date}' and from_unixtime(cast(substr(cast(data_generate_time as string),0,10) as bigint),'yyyy-MM-dd')>='${first_day}'
          and from_unixtime(cast(substr(cast(data_generate_time as string),0,10) as bigint),'yyyy-MM-dd')<='${date}'
          group by data_source,app_package_name,dt
    ) a7
    UNION ALL
    SELECT a8.time_string,
           a8.data_source,
           a8.app_package_name,
           cast(0 as int) as active_user_num,
           cast(0 as int) as live_count,
           cast(0 as int) as online_user_num_max,
           cast(0 as int) as audience_gift_num,
           cast(0 as double) as gift_money,
           cast(0 as int) as violating_num,
           cast(0 as int) as active_user_num_month_natural,
           a8.audience_gift_num_month_natural,
           cast(0 as int) as active_user_num_thirty_day,
           cast(0 as int) as active_user_num_weeks_natural
    FROM(
          select regexp_replace(dt,'-','') as time_string,data_source,app_package_name,count(distinct audience_id) as audience_gift_num_month_natural
          from live_p2.tbl_ex_live_audience_gift_daily_snapshot
          where dt='${date}' and from_unixtime(cast(substr(cast(data_generate_time as string),0,10) as bigint),'yyyy-MM-dd')>='${first_day}'
          and from_unixtime(cast(substr(cast(data_generate_time as string),0,10) as bigint),'yyyy-MM-dd')<='${date}'
          group by data_source,app_package_name,dt
    ) a8
    UNION ALL
    SELECT a9.time_string,
           a9.data_source,
           a9.app_package_name,
           cast(0 as int) as active_user_num,
           cast(0 as int) as live_count,
           cast(0 as int) as online_user_num_max,
           cast(0 as int) as audience_gift_num,
           cast(0 as double) as gift_money,
           cast(0 as int) as violating_num,
           cast(0 as int) as active_user_num_month_natural,
           cast(0 as int) as audience_gift_num_month_natural,
           a9.active_user_num_thirty_day,
           cast(0 as int) as active_user_num_weeks_natural
    FROM(
         select regexp_replace(dt,'-','') as time_string,data_source,app_package_name,count(distinct user_id) as active_user_num_thirty_day
         from live_p2.tbl_ex_live_user_known_daily_snapshot
         where dt='${date}' and from_unixtime(cast(substr(cast(data_generate_time as string),0,10) as bigint),'yyyy-MM-dd')>='${thirty_day}'
         and from_unixtime(cast(substr(cast(data_generate_time as string),0,10) as bigint),'yyyy-MM-dd')<='${date}'
         group by data_source,app_package_name,dt
    ) a9
    UNION ALL
    SELECT a10.time_string,
           a10.data_source,
           a10.app_package_name,
           cast(0 as int) as active_user_num,
           cast(0 as int) as live_count,
           cast(0 as int) as online_user_num_max,
           cast(0 as int) as audience_gift_num,
           cast(0 as double) as gift_money,
           cast(0 as int) as violating_num,
           cast(0 as int) as active_user_num_month_natural,
           cast(0 as int) as audience_gift_num_month_natural,
           cast(0 as int) as active_user_num_thirty_day,
           a10.active_user_num_weeks_natural
    FROM(
         select regexp_replace(dt,'-','') as time_string,data_source,app_package_name,count(distinct user_id) as active_user_num_weeks_natural
         from live_p2.tbl_ex_live_user_known_daily_snapshot
         where dt='${date}' and from_unixtime(cast(substr(cast(data_generate_time as string),0,10) as bigint),'yyyy-MM-dd')>='${week_stat_day}'
         and from_unixtime(cast(substr(cast(data_generate_time as string),0,10) as bigint),'yyyy-MM-dd')<='${date}'
         group by data_source,app_package_name,dt
    ) a10
) b
WHERE b.app_package_name in (${report_live_app_list})
GROUP BY b.time_string,b.data_source,b.app_package_name
"

echo ${mysql_platform_monitor_day_table}
hiveSqlToMysql "${hive_sql_2}" "" "${mysql_platform_monitor_day_table}" "time_string,data_source,app_package_name,active_user_num,live_count,online_user_num_max,audience_gift_num,gift_money,violating_num,active_user_num_month_natural,audience_gift_num_month_natural,active_user_num_thirty_day,active_user_num_weeks_natural" "time_string"


echo "############### 直播监控 每天报表统计 end #####################"





















