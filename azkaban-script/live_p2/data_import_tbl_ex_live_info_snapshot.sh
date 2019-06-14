#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
hour=$2

time_stamp_start=`date -d "$day $hour:00:00" +%s`
time_stamp_end=`date -d "$day $hour:59:59" +%s`
number=`date "+%N"`
temp_start_time_stamp=${time_stamp_start}${number:0:3}
temp_end_time_stamp=${time_stamp_end}${number:0:3}

echo ${day}  ${hour}  ${temp_start_time_stamp}  ${temp_end_time_stamp}

echo '################# 直播间快照表 start   ########################'

for app in ${live_app_list};
  do
    #echo '############# 删除当天快照 start #############'
    #deleteLivePartiton4Orc "live_p2" "tbl_ex_live_info_snapshot" "${day}" "${hour}" "${app}" "${p2_location_live_snapshot}"
    #echo '############# 删除当天快照 end #############'

    echo '############# 导入当天快照 start ###########'

    insert_sql="INSERT INTO TABLE live_p2.tbl_ex_live_info_snapshot PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
      SELECT c.record_time,'${app}','${ias_source}',c.search_id,c.user_id,c.live_id,
               if (c.start_time IS NULL AND c.end_time=0,0,
                  if (c.start_time IS NULL AND c.end_time>0,${temp_start_time_stamp},
                      if (c.start_time>c.end_time,${temp_start_time_stamp},c.start_time)
                  )
               ),
               if (c.start_time IS NULL AND c.end_time=0,0,
                  if (c.start_time IS NULL AND c.end_time>0,c.end_time,
                      if (c.start_time>c.end_time,${temp_end_time_stamp},c.end_time)
                  )
               ),
               if (c.start_time IS NULL AND c.end_time=0,0,
                  if (c.start_time IS NULL AND c.end_time>0,c.end_time-${temp_start_time_stamp},
                      if (c.start_time>c.end_time,${temp_end_time_stamp}-${temp_start_time_stamp},c.end_time-c.start_time)
                  )
               ) AS live_time,
               c.audience_count,
               c.gift_count,
               c.income,
               c.message_count,
               c.is_live
      FROM (
        SELECT concat(unix_timestamp(),substr(cast(rand() as string),3,3)) as record_time,
               b.search_id,b.user_id,live_id,min(b.start_time) as start_time,max(b.end_time) as end_time,max(b.audience_count) as audience_count,max(b.gift_count) as gift_count,max(b.income) as income,max(b.message_count) as message_count,max(b.is_live) as is_live
        FROM (

          SELECT a1.search_id,a1.user_id,a1.live_id,a1.start_time,cast(0 as bigint) as end_time,if(cast(a1.audience_count as bigint)>=0,cast(a1.audience_count as bigint),cast(0 as bigint)) as audience_count,cast(0 as bigint) as gift_count,cast(0 as double) as income,cast(0 as bigint) as message_count,cast(is_live as bigint) as is_live
          FROM (
            SELECT search_id,user_id,live_id,
                  min(record_time) AS start_time,
                  max(online_user_num) AS audience_count,
                  max(is_live) as is_live
            FROM ias_p2.tbl_ex_live_user_info_data_origin_orc
            WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}' AND is_live=1 and user_id is not null and live_id is not null
            GROUP BY user_id,live_id,search_id
          ) a1
          UNION ALL
          SELECT a2.search_id,a2.user_id,a2.live_id,null as start_time,a2.end_time,cast(0 as bigint) as audience_count,cast(0 as bigint) as gift_count,cast(0 as double) as income,cast(0 as bigint) as message_count,cast(0 as bigint) as is_live
          FROM (
            SELECT search_id,user_id,live_id,
                  max(record_time) AS end_time
            FROM ias_p2.tbl_ex_live_user_info_data_origin_orc
            WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}' AND is_live=0 and user_id is not null and live_id is not null
            GROUP BY user_id,live_id,search_id
          ) a2
          UNION ALL
          SELECT a3.search_id,a3.user_id,a3.live_id,null as start_time,cast(0 as bigint) as end_time,cast(0 as bigint) as audience_count,if(cast(a3.gift_count as bigint)>=0,cast(a3.gift_count as bigint),cast(0 as bigint)) as gift_count,if(cast(a3.income as double)>=0,cast(a3.income as double),cast(0 as double)) as income,cast(0 as bigint) as message_count,cast(0 as bigint) as is_live
          FROM (
            SELECT search_id,user_id,live_id,count(1) AS gift_count,sum(gift_val) AS income
            FROM live_p2.tbl_ex_gift_info_snapshot
            WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}' and user_id is not null and live_id is not null and gift_count>0
            GROUP BY user_id,live_id,search_id
          ) a3
          UNION ALL
          SELECT a4.search_id,a4.user_id,a4.live_id,null as start_time,cast(0 as bigint) as end_time,cast(0 as bigint) as audience_count,cast(0 as bigint) as gift_count,cast(0 as double) as income,if(cast(a4.message_count as bigint)>=0,cast(a4.message_count as bigint),cast(0 as bigint)) as message_count,cast(0 as bigint) as is_live
          FROM (
            SELECT search_id,user_id,live_id,count(1) AS message_count
            FROM live_p2.tbl_ex_message_info_snapshot
            WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}' and user_id is not null and live_id is not null
            GROUP BY user_id,live_id,search_id
          ) a4
        ) b
        GROUP BY b.user_id,b.live_id,b.search_id
      ) c
    "
    executeHiveCommand "${insert_sql}"

    echo '############# 导入当天快照 end ###########'

done


echo '################# 直播间快照表 end  ########################'
