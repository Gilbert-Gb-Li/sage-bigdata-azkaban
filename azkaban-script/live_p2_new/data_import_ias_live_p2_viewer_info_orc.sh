#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
hour=$2

echo '#################  直播间消息快照表 start  ########################'
    insert_sql_1="INSERT INTO TABLE ias_p2.tbl_ex_live_viewer_info_data_origin_orc PARTITION(dt='${day}',hour='${hour}')
        SELECT f.record_time,f.trace_id,f.schema,f.client_time,f.protocol_version,f.spider_version,f.app_package_name,'${ias_source}' as data_source,f.app_version,
               f.ias_client_hsn_id,f.template_version,
               f.search_id,f.live_id,f.user_id,f.data_generate_time,f.online_user_num,
               if(f.audience[0] == '\\\\N',null,f.audience[0]) as audience_user_id,
               if(f.audience[1] == '\\\\N',null,f.audience[1]) as audience_user_name,
               if(f.audience[2] == '\\\\N',null,f.audience[2]) as audience_user_sex,
               if(f.audience[3] == '\\\\N',null,f.audience[3]) as audience_user_hometown,
               if(f.audience[4] == '\\\\N',null,f.audience[4]) as audience_location,
               if(f.audience[5] == '\\\\N',null,f.audience[5]) as audience_current_page
        FROM (
            SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
                              ias_client_hsn_id,template_version,
                              search_id,live_id,user_id,data_generate_time,online_user_num,
                              split(r1.audience,'${audience_info_sep}') AS audience
            FROM ias_p2.tbl_ex_live_viewer_list_data_origin_orc
            LATERAL VIEW explode(audience_list) r1 AS audience WHERE dt='${day}' AND hour='${hour}' AND audience_list IS NOT NULL
        ) as f ; "


    insert_sql_2="INSERT INTO TABLE ias_p2.tbl_ex_live_guard_info_data_origin_orc PARTITION(dt='${day}',hour='${hour}')
        SELECT f2.record_time,f2.trace_id,f2.schema,f2.client_time,f2.protocol_version,f2.spider_version,f2.app_package_name,'${ias_source}' as data_source,f2.app_version,
               f2.ias_client_hsn_id,f2.template_version,
               f2.search_id,f2.live_id,f2.user_id,f2.data_generate_time,f2.is_live,
               if(f2.guardian[0] == '\\\\N',null,f2.guardian[0]) as guarder_id,
               if(f2.guardian[1] == '\\\\N',null,f2.guardian[1]) as guarder_name,
               if(f2.guardian[2] == '\\\\N',null,f2.guardian[2]) as guarder_contribute
        FROM (
            SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
                              ias_client_hsn_id,template_version,
                              search_id,live_id,user_id,data_generate_time,is_live,
                              split(r1.guardian,'${guardian_info_sep}') AS guardian
            FROM ias_p2.tbl_ex_live_guard_list_data_origin_orc
            LATERAL VIEW explode(guard_list) r1 AS guardian WHERE dt='${day}' AND hour='${hour}' AND guard_list IS NOT NULL
        ) as f2 ; "



    insert_sql_3="INSERT INTO TABLE ias_p2.tbl_ex_live_gift_contributor_info_data_origin_orc PARTITION(dt='${day}',hour='${hour}')
        SELECT f3.record_time,f3.trace_id,f3.schema,f3.client_time,f3.protocol_version,f3.spider_version,f3.app_package_name,'${ias_source}' as data_source,f3.app_version,
               f3.ias_client_hsn_id,f3.template_version,
               f3.search_id,f3.live_id,f3.user_id,f3.data_generate_time,
               if(f3.contributor[0] == '\\\\N',null,f3.contributor[0]) as contributor_user_id,
               if(f3.contributor[1] == '\\\\N',null,f3.contributor[1]) as contributor_user_name,
               if(f3.contributor[2] == '\\\\N',null,f3.contributor[2]) as contributor_user_sex,
               if(f3.contributor[3] == '\\\\N',null,f3.contributor[3]) as contributor_gift_num,
               if(f3.contributor[4] == '\\\\N',null,f3.contributor[4]) as contributor_location,
               if(f3.contributor[5] == '\\\\N',null,f3.contributor[5]) as contributor_user_level,
               if(f3.contributor[6] == '\\\\N',null,f3.contributor[6]) as contributor_current_page
        FROM (
            SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
                              ias_client_hsn_id,template_version,
                              search_id,live_id,user_id,data_generate_time,
                              split(r1.contributor,'${contributor_info_sep}') AS contributor
            FROM ias_p2.tbl_ex_live_gift_contributor_list_data_origin_orc
            LATERAL VIEW explode(gift_contributor_list) r1 AS contributor WHERE dt='${day}' AND hour='${hour}' AND gift_contributor_list IS NOT NULL
        ) as f3 ; "


    executeHiveCommand "${insert_sql_1} ${insert_sql_2} ${insert_sql_3}"

echo '#################  直播间消息快照表 end  ########################'

