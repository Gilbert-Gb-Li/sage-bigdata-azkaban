#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
hour=$2
echo "##############   导出orc表开始   ##################"
hive_sql1="insert into table ias.tbl_ex_pengpai_news_comment_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,news_id,comment,page_name,'','','','',''
from ias.tbl_ex_pengpai_news_comment_data_origin
where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql1}"

echo "##############   导出 tbl_ex_pengpai_news_comment_data_origin_orc 结束 ##################"

hive_sql2="insert into table ias.tbl_ex_pengpai_topic_comment_data_origin_orc  PARTITION(dt='${date}',hour='${hour}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,topic_id,issue,reply,page_name,'','','','','','','','','','',''
from ias.tbl_ex_pengpai_topic_comment_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql2}"

echo "##############   导出 tbl_ex_pengpai_topic_comment_data_origin_orc 结束  ##################"

hive_sql3="insert into table ias.tbl_ex_pengpai_news_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,news_id,news_title,news_publish_time,news_source,author_name,news_content,page_name
from ias.tbl_ex_pengpai_news_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql3}"

echo "##############   导出 tbl_ex_pengpai_news_data_origin_orc 结束  ##################"

hive_sql4="insert into table ias.tbl_ex_pengpai_topic_data_origin_orc PARTITION(dt='${date}',hour='${hour}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,topic_id,topic_title,topic_pic_url,topic_description,topic_publish_time,author_name,author_fans_count,author_description,page_name
from ias.tbl_ex_pengpai_topic_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql4}"

echo "##############   导出 tbl_ex_pengpai_topic_data_origin_orc 结束  ##################"