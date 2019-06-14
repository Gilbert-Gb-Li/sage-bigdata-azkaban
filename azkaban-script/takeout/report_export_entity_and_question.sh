#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1

mysql_table1="waimai_shop_geohash"
# mysql_table2="waimai_entity_score"
mysql_table3="waimai_shop"
# mysql_table4="waimai_entity_shop"
mysql_table5="waimai_entity"
mysql_table6="waimai_question"
mysql_table7="waimai_shop_question"

echo "############### 导出shop geoshop start #####################"
truncateTakeoutMysqlData "${mysql_table1}"

hive_sql1="select distinct shop_id,geo_hash,org_code,take_out_type from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}';"

hiveSqlToTakeoutMysqlNoDelete "${hive_sql1}" "${mysql_table1}" "shop_id,geohash,orgcode,waimai_type"
echo "############### 导出shop geoshop end #####################"

# echo "############### 导出entity score start #####################"
# truncateTakeoutMysqlData "${mysql_table2}"

# hive_sql2="select t1.entity_id,case when t2.score is null then '-1' else t2.score end score,t1.negative_comment_rate,t1.comment_num,t1.negative_comment_num from
# (select entity_id,case when (sum(comment_total_num) = '0.0' or sum(comment_total_num) is null) then '0.0' else round(sum(negative_comment_num)/sum(comment_total_num),2) end negative_comment_rate,case when sum(comment_total_num) is null then '0.0' else sum(comment_total_num) end comment_num,case when sum(negative_comment_num) is null then '0.0' else sum(negative_comment_num) end negative_comment_num from (
# select entity_id,comment_total_num,negative_comment_num 
# from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}'
# ) a group by entity_id) t1
# left join
# (select entity_id,sum(comprehensive_score)/sum(comment_total_num) as score from (
# select entity_id,comment_score*comment_total_num as comprehensive_score,comment_total_num 
# from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}' and comment_score != '0.0'
# ) a group by entity_id) t2
# on t1.entity_id = t2.entity_id;"

# hiveSqlToTakeoutMysqlNoDelete "${hive_sql2}" "${mysql_table2}" "entity_id,score,negative_comment_rate,comment_num,negative_comment_num"
# echo "############### 导出entity score end #####################"


echo "############### 导出全量商户 start #####################"
truncateTakeoutMysqlData "${mysql_table3}"

hive_sql3="select a.*,b.license_num,b.expire_time,b.has_business_license,b.has_license,c.entity_id from
 (select shop_id, shop_name, shop_logo, open_time, address, phone, province, city, longitude, latitude, geo_hash,
 org_code, comment_score, case when (comment_total_num = '0.0' or comment_total_num is null or comment_total_num = '' or negative_comment_num = '0.0' or negative_comment_num is null or negative_comment_num = '') then '0.0' else ceil(negative_comment_num*100/comment_total_num)/100 end negative_comment_rate,comment_total_num, negative_comment_num, take_out_type, local_logo, last_check_grade, 
 place_grade, manage_grade, check_date, company_name, company_owner, company_address,business_scope, business_type,
  expire_time,license_num,shop_cert_photo1, shop_cert_photo2
 from takeout.tbl_ex_takeout_all_shop where dt = '${yesterday}') a
 left join 
 (select shop_id,take_out_type,license_num,expire_time,has_business_license,has_license from takeout.tbl_ex_takeout_shop_all_license where dt = '${yesterday}') b
 on a.shop_id=b.shop_id and a.take_out_type=b.take_out_type
 left join 
 (select distinct entity_id, shop_id from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}') c
 on a.shop_id = c.shop_id;"

hiveSqlToTakeoutMysqlNoDelete "${hive_sql3}" "${mysql_table3}" "shop_id, shop_name, shop_logo, open_time, address, phone, province, city, longitude, latitude, geo_hash, org_code, score, negative_comment_rate, comment_num, negative_comment_num, waimai_type, local_logo, last_check_grade, place_grade, manage_grade, check_date, company_name, company_owner, company_address, business_scope, business_type,expire_time,license_num, shop_cert_photo1, shop_cert_photo2,ocr_lincense,ocr_expire_data,has_business,has_license,entity_id"

echo "############### 导出全量商户 end #####################"

# echo "############### 导出entity shop start #####################"
# truncateTakeoutMysqlData "${mysql_table4}"

# hive_sql4="select distinct entity_id, shop_id from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}';"

# hiveSqlToTakeoutMysqlNoDelete "${hive_sql4}" "${mysql_table4}" "entity_id, shop_id"
# echo "############### 导出entity shop end #####################"

echo "############### 导出实体 start #####################"
truncateTakeoutMysqlData "${mysql_table5}"

hive_sql5="select a.entity_id,a.shop_name,a.province,a.city,a.org_code,b.score,b.negative_comment_rate,b.comment_num,b.negative_comment_num from
(SELECT entity_id,shop_name,province,city,org_code from(
SELECT t.*, row_number() OVER (partition by entity_id,province ORDER BY shop_name,province,city desc) num from 
(SELECT entity_id,shop_name,province,city,org_code FROM takeout.tbl_ex_entity_info_snapshot WHERE dt = '${yesterday}') t
) r where r.num = 1) a
left join
(select t1.entity_id,case when t2.score is null then '0' else t2.score end score,t1.negative_comment_rate,t1.comment_num,t1.negative_comment_num from
(select entity_id,case when (sum(comment_total_num) = '0.0' or sum(comment_total_num) is null) then '0.0' else ceil(sum(negative_comment_num)*100/sum(comment_total_num))/100 end negative_comment_rate,case when sum(comment_total_num) is null then '0.0' else sum(comment_total_num) end comment_num,case when sum(negative_comment_num) is null then '0.0' else sum(negative_comment_num) end negative_comment_num from (
select entity_id,comment_total_num,negative_comment_num 
from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}'
) a group by entity_id) t1
left join
(select entity_id,sum(comprehensive_score)/sum(comment_total_num) as score from (
select entity_id,comment_score*comment_total_num as comprehensive_score,comment_total_num 
from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}' and comment_score != '0.0'
) a group by entity_id) t2
on t1.entity_id = t2.entity_id) b
on a.entity_id = b.entity_id;"

hiveSqlToTakeoutMysqlNoDelete "${hive_sql5}" "${mysql_table5}" "entity_id, name, province, city, orgcode, score, negative_comment_rate, comment_num, negative_comment_num"
echo "############### 导出实体 end #####################"

echo "############### 导出waimai question start #####################"
truncateTakeoutMysqlData "${mysql_table6}"

hive_sql6="SELECT distinct entity_id, question_type from takeout.tbl_ex_entity_question_info_snapshot WHERE dt = '${yesterday}' and question_type != '0';"

hiveSqlToTakeoutMysqlNoDelete "${hive_sql6}" "${mysql_table6}" "entity_id, question_type_id"
echo "############### 导出waimai question end #####################"

echo "############### 导出waimai shop question start #####################"
truncateTakeoutMysqlData "${mysql_table7}"

hive_sql7="SELECT distinct entity_id, shop_id, take_out_type, question_type, org_code from takeout.tbl_ex_entity_question_info_snapshot WHERE dt = '${yesterday}' and question_type != '0';"

hiveSqlToTakeoutMysqlNoDelete "${hive_sql7}" "${mysql_table7}" "entity_id, shop_id, waimai_type, question_type_id ,orgcode"
echo "############### 导出waimai shop question end #####################"
