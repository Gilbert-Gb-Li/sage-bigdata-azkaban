#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
hour=$2

echo '#################  直播间消息快照表 start  ########################'

for app in ${all_live_app_list};
  do
    echo '################# 直播间消息快照表 start   ########################'

      tmp_gift_p2="CREATE TEMPORARY TABLE default.tmp_gift_p4 AS
        SELECT f.record_time,f.trace_id,f.app_package_name,f.data_generate_time,f.search_id,f.user_id,f.live_id,
               if(f.gift[0] == '\\\\N',null,f.gift[0]) AS audience_id,
               if(f.gift[1] == '\\\\N',null,f.gift[1]) AS audience_name,
               if(f.gift[2] == '\\\\N',null,f.gift[2]) AS gift_id,
               if(f.gift[3] == '\\\\N',null,f.gift[3]) AS gift_type,
               if(f.gift[4] == '\\\\N',null,f.gift[4]) AS gift_name,
               if(f.gift[5] == '\\\\N',null,f.gift[5]) AS gift_image_url,
               if(f.gift[6] == '\\\\N',null,f.gift[6]) AS gift_count,
               if(f.gift[7] == '\\\\N',null,f.gift[7]) AS gift_content,
               if(f.gift[8] == '\\\\N',null,f.gift[8]) AS gift_unit_price,
               if(f.gift[9] == '\\\\N',null,f.gift[9]) AS type,
               if(gift[3] == '\\\\N' and gift[2] == '\\\\N',null,
                  if(gift[3] == '\\\\N' and gift[2] <> '\\\\N',gift[2],
                     if(gift[3] <> '\\\\N' and gift[2] == '\\\\N',gift[3],
                        if(gift[3] <> '\\\\N' and gift[2] <> '\\\\N',
                           concat(gift[3], gift[2]),
                           null
                        )
                     )
                  )
               ) AS gift_type_id,row_number() over(order by f.record_time) as tmp_number
        FROM (
            SELECT record_time,trace_id,app_package_name,data_generate_time,
                   search_id,user_id,live_id,split(r1.gift,'${gift_info_sep}') AS gift
            FROM ias_p2.tbl_ex_live_danmu_data_origin_orc AS t
            LATERAL VIEW explode(gift_info) r1 AS gift WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}' AND gift_info IS NOT NULL
        ) as f ;
        "


    insert_sql_1="INSERT INTO TABLE live_p2.tbl_ex_message_info_snapshot PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
        SELECT f.record_time,f.trace_id,f.app_package_name,'${ias_source}',f.data_generate_time,f.search_id,f.user_id,f.live_id,
               if(f.message[0] == '\\\\N',
                  if(f.message[1] == '\\\\N',null,f.message[1]),
                  if(f.message[0] == '\\\\N',null,f.message[0])
               ) as audience_id,
               if(f.message[1] == '\\\\N',null,f.message[1]) as audience_name,
               if(f.message[7] == '\\\\N',null,f.message[7]) as content,
               if(f.message[9] == '\\\\N',null,f.message[9]) as type
        FROM (
            SELECT record_time,trace_id,app_package_name,data_generate_time,search_id,user_id,live_id,split(r1.message,'${message_info_sep}') AS message
            FROM ias_p2.tbl_ex_live_danmu_data_origin_orc as t
            LATERAL VIEW explode(message_info) r1 AS message WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}' AND message_info IS NOT NULL
        ) as f ; "

    insert_sql_2="INSERT INTO TABLE live_p2.tbl_ex_message_info_snapshot PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
        select record_time,trace_id,app_package_name,'${ias_source}',data_generate_time,search_id,user_id,live_id,
               if(audience_id is null,audience_name,audience_id) as audience_id,
               audience_name,gift_content as content,type
        from default.tmp_gift_p4 where gift_count<=0 or gift_count is null; "

    executeHiveCommand "${tmp_gift_p2} ${insert_sql_1} ${insert_sql_2}"

    echo '############# 直播间消息快照表 end################'

done

echo '#################  直播间消息快照表 end  ########################'

