#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1

for hour in {00..23}
do
echo "##############   导出orc表开始   ##################"
hive_sql1="insert into table bigdata.douyin_user_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,douyin_id,dynamic_count,short_video_count,like_video_count,school,location,sex,nick_name,user_id,signature,follower_count,like_count,certificate_type,certificate_info,following_count,shop_window,age,province,city,music_count,parent_resource_key,task_data_id,flow_id,source 
from bigdata.douyin_user_data_origin
where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql1}"

echo "##############   导出 douyin_user_data_origin_orc 结束 ##################"

hive_sql2="insert into bigdata.douyin_video_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,music_id,challenge_id,location,comments_count,description,commodity_id,like_count,is_advert,play_url_list,user_birthday,download_url_list,short_video_id,share_count,play_count,video_create_time,user_share_url,challenge_name,video_share_url,location_count,author_id,cover_url_list,avatar_url,hot_id,gender,age,music_name,parent_resource_key,task_data_id,flow_id,source
from bigdata.douyin_video_data_origin 
where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql2}"

echo "##############   导出 douyin_video_data_origin_orc 结束  ##################"

hive_sql3="insert into table bigdata.douyin_video_comment_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,created_time,like_count,comment_total,short_video_id,user_id,comment,comment_type,reply_id,comment_id,parent_resource_key,task_data_id,flow_id,source
from bigdata.douyin_video_comment_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql3}"

echo "##############   导出 douyin_video_comment_data_origin_orc 结束  ##################"

hive_sql4="insert into table bigdata.douyin_hot_music_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,music_use_count,music_name,music_id,music_play_url,music_author,parent_resource_key,task_data_id,flow_id,source
from bigdata.douyin_hot_music_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql4}"

echo "##############   导出 douyin_hot_music_data_origin_orc 结束  ##################"

hive_sql5="insert into table bigdata.douyin_hot_challenge_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,challenge_look_count,challenge_name,challenge_id,challenge_play_count,challenge_desc,challenge_author,parent_resource_key,task_data_id,flow_id,source
from bigdata.douyin_hot_challenge_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql5}"

echo "##############   导出 douyin_hot_challenge_data_origin_orc 结束  ##################"

hive_sql6="insert into table bigdata.douyin_shop_window_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,store_name,store_id,shop_total,parent_resource_key,task_data_id,flow_id,source
from bigdata.douyin_shop_window_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql6}"

echo "##############   导出 douyin_shop_window_data_origin_orc 结束  ##################"

hive_sql7="insert into table bigdata.douyin_shop_window_goods_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,goods_name,good_list_url,goods_price,transaction_record,goods_url,commodity_label,goods_coupon,browse_count,commodity_status,goods_id,store_id,video_id,goods_url_type,parent_resource_key,task_data_id,flow_id,source
from bigdata.douyin_shop_window_goods_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql7}"

echo "##############   导出 douyin_shop_window_goods_data_origin_orc 结束  ##################"

hive_sql8="insert into table bigdata.douyin_hot_recommend_rank_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,hot_name,location,rank_type,price,rank_num,hot_id,rank_type_id,grade,address,type,parent_resource_key,task_data_id,flow_id,source
from bigdata.douyin_hot_recommend_rank_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql8}"

echo "##############   导出 douyin_hot_recommend_rank_data_origin_orc 结束  ##################"

hive_sql9="insert into table bigdata.douyin_hot_recommend_details_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,hot_name,source,tags,price,share_url,average_consumption,open_time,hot_id,user_count,grade,web_url,address,telephone,type,parent_resource_key,task_data_id,flow_id,source_type
from bigdata.douyin_hot_recommend_details_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql9}"

echo "##############   导出 douyin_hot_recommend_details_data_origin_orc 结束  ##################"

hive_sql10="insert into table bigdata.douyin_music_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,music_use_count,music_name,music_id,music_play_url,music_author,parent_resource_key,task_data_id,flow_id,source
from bigdata.douyin_music_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql10}"

echo "##############   导出 douyin_music_data_origin_orc 结束  ##################"

hive_sql11="insert into table bigdata.douyin_challenge_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,challenge_look_count,challenge_name,challenge_id,challenge_play_count,challenge_desc,challenge_author,parent_resource_key,task_data_id,flow_id,source
from bigdata.douyin_challenge_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql11}"

echo "##############   导出 douyin_challenge_data_origin_orc 结束  ##################"

hive_sql12="insert into table bigdata.douyin_attention_follower_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,douyin_id,sex,nick_name,user_id,signature,user_birthday,avatar_url,from_user,object_type,parent_resource_key,task_data_id,flow_id,source 
from bigdata.douyin_attention_follower_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql12}"

echo "##############   导出 douyin_attention_follower_data_origin_orc 结束  ##################"

hive_sql13="insert into table bigdata.douyin_hot_search_list_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,challenge_id,commodity_id,author_id,short_video_id,parent_resource_key,task_data_id,flow_id,source 
from bigdata.douyin_hot_search_list_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql13}"

echo "##############   导出 douyin_hot_search_list_data_origin_orc 结束  ##################"

hive_sql14="insert into table bigdata.douyin_user_video_count_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,user_id,dynamic_count,short_video_count,like_video_count,nick_name,parent_resource_key,task_data_id,flow_id,source 
from bigdata.douyin_user_video_count_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql14}"

echo "##############   导出 douyin_user_video_count_data_origin_orc 结束  ##################"

hive_sql15="insert into table bigdata.douyin_hot_recommend_video_and_user_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select cloud_service_id,client_time,spider_version,app_version,data_generate_time,app_package_name,container_id,resource_key,data_type,data_source,schema,record_time,trace_id,douyin_id,dynamic_count,short_video_count,like_video_count,school,location,sex,nick_name,user_id,signature,follower_count,like_count,certificate_type,certificate_info,following_count,shop_window,age,provice,city,commodity,video_desc,video_location,video_effects,parent_resource_key,task_data_id,flow_id,source 
from bigdata.douyin_hot_recommend_video_and_user_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql15}"

echo "##############   导出douyin_hot_recommend_video_and_user_data_origin_orc 结束  ##################"
done
