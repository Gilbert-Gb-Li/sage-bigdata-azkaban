#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
hour=$2

if [ "$hour" == "00" ]; then
    check_hour=23
    check_day=`date -d "-1 day $day" +%Y-%m-%d`
else
    hour_tmp=`expr $hour - 1`
    if [ ${#hour_tmp} == "1" ]; then
       check_hour="0"${hour_tmp}
    else
       check_hour=`expr $hour - 1`
    fi
    check_day=$day
fi

echo "############### 直播监控 违规用户报表统计 start #####################"

mysql_table="tbl_live_p2_monitor_violation_user"

hive_sql="
SELECT '${day}',
        ${hour},
        x.biz_name,
        x.data_source,

        x.search_id,
        x.order_id,
        x.video_url,
        x.video_length,
        x.start_time,
        x.end_time,
        x.result_code,

        x.gift_val AS income,
        x.gift_count,
        x.message_count,

        x.max_audience_count,
        x.min_audience_count,
        x.avg_audience_count,

        y.name,
        y.version,
        y.version_code,
        y.download_url,
        y.size,
        y.hash,
        y.type,
        y.icon_url,
        y.location,
        y.ip_info,

        x.user_id,
        x.user_name,
        x.age,
        x.sex,
        x.family,
        x.sign,
        x.user_level,
        x.vip_level,
        x.constellation,
        x.hometown,
        x.occupation,
        x.follow_count,
        x.fans_count,
        x.income_app_coin,
        x.cost_app_coin,
        x.location,
        x.user_image as user_avatar_url,

        '' as contact_info,
        1 as is_valid
FROM
(
  SELECT r.biz_name,
         r.data_source,
         r.search_id,
         r.order_id,
         r.video_url,
         r.video_length,
         r.start_time,
         r.end_time,
         r.result_code,
         r.avg_audience_count,
         r.max_audience_count,
         r.min_audience_count,
         r.gift_val,
         r.gift_count,
         r.message_count,
         s.user_id,
         s.user_name,
         s.age,
         s.sex,
         s.family,
         s.sign,
         s.user_level,
         s.vip_level,
         s.constellation,
         s.hometown,
         s.occupation,
         s.follow_count,
         s.fans_count,
         s.income_app_coin,
         s.cost_app_coin,
         s.location
  FROM
  (
    SELECT o.app_package_name AS biz_name,
           '${ias_source}' AS data_source,
           o.search_id,
           o.order_id,
           o.video_url,
           o.video_length,
           o.start_time,
           o.end_time,
           o.result_code,
           o.avg_audience_count,
           o.max_audience_count,
           o.min_audience_count,
           p.gift_val,
           p.gift_count,
           p.message_count
    FROM
    (
      SELECT a.*,
             b.avg_audience_count,
             b.max_audience_count,
             b.min_audience_count
      FROM
      (SELECT * FROM ias_p2.tbl_ex_live_record_data_origin_orc
       WHERE dt='${day}' AND hour='${hour}' AND app_package_name NOT IN (${sex_live_app_list})
      ) a
      LEFT JOIN
      (SELECT search_id,order_id,
               avg(online_num) AS avg_audience_count,
               max(online_num) AS max_audience_count,
               min(online_num) AS min_audience_count
        FROM
        ias_p2.tbl_ex_live_record_audience_count_data_origin_orc
        WHERE dt='${day}' AND hour='${hour}'
        GROUP BY search_id,order_id
      ) b
      ON a.search_id=b.search_id AND a.order_id=b.order_id
    ) o
    LEFT JOIN
    (
      SELECT search_id,gift_val,gift_count,count(1) as message_count
      FROM
      (
        SELECT search_id,start_time,end_time,sum(b.gift_val) as gift_val,count(1) as gift_count
        FROM
        (
          SELECT search_id,start_time,end_time FROM ias_p2.tbl_ex_live_record_data_origin_orc
          WHERE dt='${day}' AND hour='${hour}'
        ) a
        LEFT JOIN
        (
          SELECT search_id,data_generate_time,gift_val FROM live_p2.tbl_ex_gift_info_snapshot
          WHERE (dt='${day}' AND hour='${hour}') OR (dt='${check_day}' AND hour='${check_hour}')
        ) b
        ON a.search_id=b.search_id
        WHERE (b.data_generate_time >= a.start_time) AND (b.data_generate_time <= a.end_time)
        GROUP BY a.search_id,a.start_time,a.end_time
      ) c
      LEFT JOIN
      (
        SELECT search_id,data_generate_time FROM live_p2.tbl_ex_message_info_snapshot
        WHERE (dt='${day}' AND hour='${hour}') OR (dt='${check_day}' AND hour='${check_hour}')
      ) d
      ON c.search_id=d.search_id
      WHERE (d.data_generate_time >= c.start_time) AND (d.data_generate_time <= c.end_time)
      GROUP BY c.search_id,c.gift_val,c.gift_count
    ) p
    ON o.search_id=p.search_id
  ) r

  LEFT JOIN
  (
    SELECT a.search_id,b.* FROM
    (
      SELECT search_id,user_id
      FROM live_p2.tbl_ex_live_info_snapshot
      WHERE dt='${day}' AND hour='${hour}'
      GROUP BY search_id,user_id
    ) a
    LEFT JOIN
    (
      SELECT *
      FROM live_p2.tbl_ex_user_snapshot
      WHERE dt='${day}' AND hour='${hour}'
    ) b
    ON a.user_id=b.user_id
  ) s
  ON r.search_id=s.search_id
) x
LEFT JOIN
live_p2.tbl_live_p2_app_info AS y
ON x.biz_name=y.biz_name
"

liveHiveSqlToMysqlNoConvert "${hive_sql}" "${day}" "${hour}" "${mysql_table}" "dt,hour,biz_name,data_source,search_id,order_id,video_url,video_length,start_time,end_time,result_code,income,gift_count,message_count,max_audience_count,min_audience_count,avg_audience_count,app_name,version,version_code,apk_url,apk_size,apk_hash,app_type,app_icon_url,app_location,app_ip_info,user_id,user_name,age,sex,family,sign,user_level,vip_level,constellation,hometown,occupation,follow_count,fans_count,income_app_coin,cost_app_coin,location,user_avatar_url,contact_info,is_valid" "dt" "hour"

echo "############### 直播监控 违规用户统计 end #####################"
