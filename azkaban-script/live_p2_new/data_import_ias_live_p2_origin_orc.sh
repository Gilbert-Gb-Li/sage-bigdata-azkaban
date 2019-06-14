#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
hour=$2

echo ${day} ${hour}

echo "##############   导入${all_live_app_list} orc 表 开始   ##################"

for app in ${all_live_app_list};
  do
    echo "##############   导入 ${app} orc表开始   ##################"

    hive_sql1="INSERT INTO TABLE ias_p2.tbl_ex_live_id_list_data_origin_orc PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
           ias_client_hsn_id,template_version,search_id,live_id,user_id,user_name,live_desc,room_id,
           online_user_num,current_page,hsn_location,user_image,user_level,share_url
    FROM ias_p2.tbl_ex_live_id_list_data_origin
    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}';"
    echo "$hive_sql1"

    hive_sql2="INSERT INTO TABLE ias_p2.tbl_ex_live_danmu_data_origin_orc PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
           ias_client_hsn_id,template_version,search_id,live_id,user_id,data_generate_time,message_info,gift_info,crash_ip,normal_ip,task_create_time,task_status
    FROM ias_p2.tbl_ex_live_danmu_data_origin
    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}';"
    echo "${hive_sql2}"

    hive_sql3="INSERT INTO TABLE ias_p2.tbl_ex_live_user_info_data_origin_orc PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
           ias_client_hsn_id,template_version,search_id,live_id,user_id,data_generate_time,user_name,age,sex,
           user_level,vip_level,family,sign,constellation,hometown,occupation,room_id,live_desc,follow_count,fans_count,
           income,cost,location,start_time,join_time,online_user_num,is_live,guard_num,authentication,user_label,user_hobby
    FROM ias_p2.tbl_ex_live_user_info_data_origin
    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}';"

    echo "${hive_sql3}"

    hive_sql4="INSERT INTO TABLE ias_p2.tbl_ex_live_stream_url_data_origin_orc PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
           ias_client_hsn_id,template_version,search_id,live_id,user_id,data_generate_time,stream_url
    FROM ias_p2.tbl_ex_live_stream_url_data_origin
    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}';"

    echo "${hive_sql4}"

###数据暂时没有上报，不需要运行次任
#    hive_sql5="INSERT INTO TABLE ias_p2.tbl_ex_live_ip_list_data_origin_orc PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
#    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
#           ias_client_hsn_id,template_version,uri_list
#    FROM ias_p2.tbl_ex_live_ip_list_data_origin
#    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}';"

#    echo "${hive_sql5}"


###数据暂时没有上报，不需要运行次任务
#    hive_sql6="INSERT INTO TABLE ias_p2.tbl_ex_live_share_link_data_origin_orc PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
#    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
#           ias_client_hsn_id,template_version,search_id,live_id,user_id,data_generate_time,share_link
#    FROM ias_p2.tbl_ex_live_share_link_data_origin
#    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}';"

#    echo "${hive_sql6}"


    hive_sql7="INSERT INTO TABLE ias_p2.tbl_ex_live_record_data_origin_orc PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
           ias_client_hsn_id,template_version,
           search_id,order_id,video_url,video_length,start_time,end_time,result_code
    FROM ias_p2.tbl_ex_live_record_data_origin
    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}';"

    echo "${hive_sql7}"

    hive_sql8="INSERT INTO TABLE ias_p2.tbl_ex_live_record_audience_count_data_origin_orc PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
           ias_client_hsn_id,template_version,
           search_id,order_id,online_num,live_id,user_id,data_generate_time,is_live
    FROM ias_p2.tbl_ex_live_record_audience_count_data_origin
    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}';"

    echo "${hive_sql8}"

    hive_sql9="INSERT INTO TABLE ias_p2.tbl_ex_live_viewer_list_data_origin_orc PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
           ias_client_hsn_id,template_version,
           search_id,live_id,user_id,data_generate_time,online_user_num,audience_list
    FROM ias_p2.tbl_ex_live_viewer_list_data_origin
    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}';"

    echo "${hive_sql9}"

    hive_sql10="INSERT INTO TABLE ias_p2.tbl_ex_live_weibo_url_data_origin_orc PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
           ias_client_hsn_id,template_version,
           search_id,live_id,user_id,data_generate_time,weibo_url
    FROM ias_p2.tbl_ex_live_weibo_url_data_origin
    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}';"

    echo "${hive_sql10}"

    hive_sql11="INSERT INTO TABLE ias_p2.tbl_ex_live_guard_list_data_origin_orc PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
           ias_client_hsn_id,template_version,
           search_id,live_id,user_id,data_generate_time,is_live,guard_list
    FROM ias_p2.tbl_ex_live_guard_list_data_origin
    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}';"

    echo "${hive_sql11}"

    hive_sql12="INSERT INTO TABLE ias_p2.tbl_ex_live_gift_contributor_list_data_origin_orc PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
           ias_client_hsn_id,template_version,
            search_id,live_id,user_id,data_generate_time,gift_contributor_list
    FROM ias_p2.tbl_ex_live_gift_contributor_list_data_origin
    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}';"

    echo "${hive_sql12}"


    executeHiveCommand "${hive_sql1} ${hive_sql2} ${hive_sql3} ${hive_sql4} ${hive_sql7} ${hive_sql8} ${hive_sql9} ${hive_sql10} ${hive_sql11} ${hive_sql12}"

    echo "##############   导入 ${app} orc表结束   ##################"
done



echo "##############   导入${all_live_app_list} orc 表结束   ##################"


echo "##############   导入 live_gift_info orc表开始   ##################"
    hive_sql13="INSERT INTO TABLE ias_p2.tbl_ex_live_gift_info_data_origin_orc PARTITION(dt='${day}',hour='${hour}')
    SELECT record_time,trace_id,schema,client_time,protocol_version,spider_version,app_package_name,app_version,
           ias_client_hsn_id,template_version,crash_ip,normal_ip,task_create_time,task_status,
           search_id,live_id,user_id,data_generate_time,gift_id,gift_name,gift_currency_type,gift_image,gift_gold,gift_unit_val
    FROM ias_p2.tbl_ex_live_gift_info_data_origin
    WHERE dt='${day}' AND hour='${hour}';"

    echo "${hive_sql13}"

    executeHiveCommand "${hive_sql13}"
echo "##############   导入 live_gift_info orc表结束   ##################"