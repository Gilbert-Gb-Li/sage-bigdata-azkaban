#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
hour=$2
echo "##############   导出orc表开始   ##################"
hive_sql1="insert into table ias.tbl_ex_weibo_user_topic_data_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,user_id,user_name,screen_name,avatar_url,cover_url,article_count,followers_count,fans_count,user_location,sex,
verified,verified_info,verified_type,email,qq,msn,rank,vip_rank,description,credit_score,readed_count_yesterday,action_count_yesterday,followers_id_list,fans_id_list
from ias.tbl_ex_weibo_user_topic_data_origin
where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql1}"

echo "##############   导出 tbl_ex_weibo_user_topic_data_origin_orc 结束 ##################"

hive_sql2="insert into table ias.tbl_ex_weibo_article_topic_data_origin_orc  PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,article_id,article_content,user_id,created_time,like_count,comments_count,forwards_count,reads_count,source,
share_url,pic_url_list,is_ad
from ias.tbl_ex_weibo_article_topic_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql2}"

echo "##############   导出 tbl_ex_weibo_article_topic_data_origin_orc 结束  ##################"

hive_sql3="insert into table ias.tbl_ex_weibo_comment_topic_data_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,comment_id,root_id,article_id,comment,created_time,like_count,reply_count,user_id,pic_url_list
from ias.tbl_ex_weibo_comment_topic_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql3}"

echo "##############   导出 tbl_ex_weibo_comment_topic_data_origin_orc 结束  ##################"

hive_sql4="insert into table ias.tbl_ex_weibo_forward_topic_data_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,forward_id,article_id,comment,created_time,forward_count,user_id
from ias.tbl_ex_weibo_forward_topic_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql4}"

echo "##############   导出 tbl_ex_weibo_forward_topic_data_origin_orc 结束  ##################"
