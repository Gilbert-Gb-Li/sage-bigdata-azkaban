#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1

echo "##############   导出orc表开始   ##################"
hive_sql1="insert into table ias.tbl_ex_short_video_user_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,
ias_client_hsn_id,job_id,task_id,location,mobile,ias_client_ip,user_id,nick_name,user_name,avatar_url,short_video_count,following_count,
follower_count,birthday,user_location,sex,like_count,signature,weibo_url,following_id_list,follower_id_list,mplatform_followers_count,
custom_verify,internal_uid,favoriting_video_count,enterprise_verify_reason,verification_type,weibo_verify,constellation,pre_uid,relationship,template_name
from ias.tbl_ex_short_video_user_origin
where dt='${date}' "

executeHiveCommand "${hive_sql1}"

echo "##############   导出 tbl_ex_short_video_user_origin_orc 结束 ##################"



echo "##############   导出orc表开始   ##################"
hive_sql1="insert into table ias.tbl_ex_short_video_data_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,
ias_client_hsn_id,job_id,task_id,location,mobile,ias_client_ip,short_video_id,author_id,video_create_time,comment_count,share_count,play_count,
like_count,description,share_url,cover_url_list,play_url_list,download_url_list,music_id, music_play_url,music_is_original,music_author,music_name,
shallenge_id,challenge_name
from ias.tbl_ex_short_video_data_origin
where dt='${date}' "

executeHiveCommand "${hive_sql1}"

echo "##############   导出 tbl_ex_short_video_data_origin 结束  ##################"



echo "##############   导出orc表开始   ##################"
hive_sql1="insert into table ias.tbl_ex_short_video_comment_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,
ias_client_hsn_id,job_id,task_id,location,mobile,ias_client_ip,short_video_id,comment_id,comment,reply_id,created_time,like_count,
user_id
from ias.tbl_ex_short_video_comment_origin
where dt='${date}' "

executeHiveCommand "${hive_sql1}"

echo "##############   导出 tbl_ex_short_video_comment_origin_orc 结束 ##################"


echo "##############   导出orc表开始   ##################"
hive_sql1="insert into table ias.tbl_ex_short_video_music_data_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,
ias_client_hsn_id,job_id,task_id,location,mobile,ias_client_ip,music_is_original,music_user_count,music_author,music_id,music_name,music_play_url,
music_template_type
from ias.tbl_ex_short_video_music_data_origin
where dt='${date}' "

executeHiveCommand "${hive_sql1}"

echo "##############   导出 tbl_ex_short_video_music_data_origin_orc 结束 ##################"




echo "##############   导出orc表开始   ##################"
hive_sql1="insert into table ias.tbl_ex_short_video_user_like_link_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,
ias_client_hsn_id,job_id,task_id,location,mobile,ias_client_ip,user_id,nick_name,favorite_id_list,music_author
from ias.tbl_ex_short_video_user_like_link_origin
where dt='${date}' "

executeHiveCommand "${hive_sql1}"

echo "##############   导出 tbl_ex_short_video_user_like_link_origin_orc 结束 ##################"




echo "##############   导出orc表开始   ##################"
hive_sql1="insert into table ias.tbl_ex_short_video_challenge_data_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,
ias_client_hsn_id,job_id,task_id,location,mobile,ias_client_ip,challenge_id,challenge_name,challenge_user_count,challenge_description
from ias.tbl_ex_short_video_challenge_data_origin
where dt='${date}' "

executeHiveCommand "${hive_sql1}"

echo "##############   导出 tbl_ex_short_video_challenge_data_origin_orc 结束 ##################"


echo "##############   导出orc表开始   ##################"
hive_sql1="insert into table ias.tbl_ex_short_video_user_detail_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,
ias_client_hsn_id,job_id,task_id,location,mobile,ias_client_ip,
user_id,following_count,follower_count,like_count,weibo_url,mplatform_followers_count,internal_uid,favoriting_video_count,short_video_count
from ias.tbl_ex_short_video_user_detail_origin
where dt='${date}' "

executeHiveCommand "${hive_sql1}"

echo "##############   导出 tbl_ex_short_video_user_detail_origin_orc 结束 ##################"


