#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1


echo "##############   导出orc表开始   ##################"
hive_sql1="insert into table ias.tbl_ex_takeout_shop_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,shop_id,shop_name,shop_logo,open_time,address,phone,province,city,longitude,latitude,take_out_type,geo_hash,org_code 
from ias.tbl_ex_takeout_shop_origin
where dt='${date}'"

executeHiveCommand "${hive_sql1}"

echo "##############   导出 tbl_ex_takeout_shop_origin_orc 结束 ##################"

hive_sql2="insert into table ias.tbl_ex_takeout_shop_info_origin_orc  PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,shop_id,shop_name,company_name,company_owner,company_address,business_scope,business_type,expire_time,license_num,
shop_cert_photo1,shop_cert_photo2,take_out_type,geo_hash,local_logo,last_check_grade,place_grade,manage_grade,check_date 
from ias.tbl_ex_takeout_shop_info_origin where dt='${date}'"

executeHiveCommand "${hive_sql2}"

echo "##############   导出 tbl_ex_takeout_shop_info_origin_orc 结束  ##################"

hive_sql3="insert into table ias.tbl_ex_takeout_shop_comment_total_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,shop_id,take_out_type,comment_score,comment_total_num,comment_level_1,comment_level_2,comment_level_3,comment_with_pic
from ias.tbl_ex_takeout_shop_comment_total_origin where dt='${date}'"

executeHiveCommand "${hive_sql3}"

echo "##############   导出 tbl_ex_takeout_shop_comment_total_origin 结束  ##################"

hive_sql4="insert into table ias.tbl_ex_takeout_shop_comment_detail_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,shop_id,take_out_type,comment_id,user_id,user_name,score,comment,comment_time
from ias.tbl_ex_takeout_shop_comment_detail_origin where dt='${date}'"

executeHiveCommand "${hive_sql4}"

echo "##############   导出 tbl_ex_takeout_shop_comment_detail_origin_orc 结束  ##################"

hive_sql5="insert into table ias.tbl_ex_takeout_shop_license_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,protocol_version,biz_type,shop_id,take_out_type,
case when expire_time!='null'  and expire_time is not null  
then concat(substr(expire_time,1,4),'-',substr(expire_time,6,2),'-',substr(expire_time,9,2))
else '' end as expire_time
,license_num,has_business_license,has_license,shop_name
from ias.tbl_ex_takeout_shop_license_origin where dt='${date}'"

executeHiveCommand "${hive_sql5}"

echo "##############   导出 tbl_ex_takeout_shop_license_origin 结束  ##################"