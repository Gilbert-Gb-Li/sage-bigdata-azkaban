#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1

echo '###计算无证 无许可证'
hive_sql1="insert into table takeout.tbl_ex_entity_question_info_snapshot PARTITION(dt='${yesterday}')
select * from (
select entity_id,shop_id,take_out_type,org_code,
case when (ocr_license_no is null  or ocr_license_no='' or ocr_license_no='null') 
and (is_has_business_license='0' or is_has_business_license is null)
then '8' 
when ((ocr_license_no is null  or ocr_license_no='' or ocr_license_no='null')  and is_has_business_license='1') 
then '6'
end question_type,province,city
from takeout.tbl_ex_entity_info_snapshot
where dt='${yesterday}') as t where t.question_type is not null "

executeHiveCommand "${hive_sql1}"

echo '####//无营业证'
hive_sql2="insert into table takeout.tbl_ex_entity_question_info_snapshot PARTITION(dt='${yesterday}')
select * from (
select t1.entity_id,t2.shop_id,t2.take_out_type,t2.org_code,
case when t1.license_no='0' then '7' end question_type,t2.province,t2.city from
(select entity_id,sum(is_has_business_license) as license_no
from  takeout.tbl_ex_entity_info_snapshot 
where ocr_license_no is not null and ocr_license_no !='' and dt='${yesterday}'
group by entity_id) as t1
left join 
(select * from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' )as t2
 on t1.entity_id=t2.entity_id) as t 
where t.question_type is not null"

executeHiveCommand "${hive_sql2}"

echo '######//证件不一致'
hive_sql3="insert into table takeout.tbl_ex_entity_question_info_snapshot PARTITION(dt='${yesterday}')
select * from (
select t1.entity_id,t2.shop_id,t2.take_out_type,t2.org_code,'9' as question_type,t2.province,t2.city
from(
select t1.entity_id from (
select entity_id from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' and ocr_license_no is not null and ocr_license_no!='') as t1
left join
(select licno from takeout.tbl_ex_license_info) as t2
on t1.entity_id =t2.licno
where t2.licno is null
)  as t1 left join 
(select * from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' )as t2
 on t1.entity_id=t2.entity_id) as t where t.question_type is not null"

executeHiveCommand "${hive_sql3}"

echo '##### //  日期过期'
hive_sql4="insert into table takeout.tbl_ex_entity_question_info_snapshot PARTITION(dt='${yesterday}')
select * from (
select t1.entity_id,t2.shop_id,t2.take_out_type,t2.org_code,t1.question_type as question_type,t2.province,t2.city
from(
select t1.entity_id,case when (t1.license_expire_date = t2.licexpiry and t1.license_expire_date is not null and t2.licexpiry is not null) and t1.license_expire_date < '${yesterday}' then '13' end as question_type
 from ( select entity_id,license_expire_date from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' and ocr_license_no is not null) as t1
left join
(select licno,licexpiry from takeout.tbl_ex_license_info) as t2
on t1.entity_id =t2.licno
where t2.licno is not null
) as t1 left join 
(select * from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' )as t2
 on t1.entity_id=t2.entity_id) as t  where t.question_type is not null"

executeHiveCommand "${hive_sql4}"

echo '##### //问题类型为10 识别的日期跟药监局所有shop_id的日期匹配不上'
hive_sql4_1="insert into table takeout.tbl_ex_entity_question_info_snapshot PARTITION(dt='${yesterday}')
select * from (
select t1.entity_id,t2.shop_id,t2.take_out_type,t2.org_code,'10' as question_type ,t2.province,t2.city
from (

select distinct(t1.entity_id) as entity_id from (select a.entity_id,a.license_expire_date,b.licno
from (select entity_id,license_expire_date from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' and ocr_license_no is not null) as a
left join (select licno from takeout.tbl_ex_license_info) as b
on a.entity_id=b.licno where b.licno is not null ) t1
left join
(select distinct entity_id,question_type from (select a.entity_id, case when(a.license_expire_date=b.licexpiry) then 'yes'
when (a.license_expire_date!=b.licexpiry) then 'not' end question_type
from (select entity_id,license_expire_date from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' and ocr_license_no is not null) as a
left join (select licno,licexpiry from takeout.tbl_ex_license_info) as b
on a.entity_id=b.licno where b.licno is not null ) t where question_type = 'yes') t2 on t1.entity_id=t2.entity_id
where t2.entity_id is null

) as t1 left join
 (select * from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' )as t2
 on t1.entity_id=t2.entity_id ) as t  where t.question_type is not null;"

executeHiveCommand "${hive_sql4_1}"



echo '###### 一证多用'
hive_sql8="insert into table takeout.tbl_ex_entity_question_info_snapshot PARTITION(dt='${yesterday}')
select * from (
select t1.entity_id,t2.shop_id,t2.take_out_type,t2.org_code,t1.question_type as question_type,t2.province,t2.city
from(
select t.entity_id, case when t.count_num > 1 then '14' end as question_type
 from (
select entity_id,take_out_type,count(distinct shop_id) as count_num from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' 
group by entity_id,take_out_type
) as t ) as t1  left join 
(select * from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' )as t2
 on t1.entity_id=t2.entity_id) as t where t.question_type is not null "

executeHiveCommand "${hive_sql8}"


echo '##### //缺证'
hive_sql5="insert into table takeout.tbl_ex_entity_question_info_snapshot PARTITION(dt='${yesterday}')
select * from (
select t2.entity_id,t1.shop_id,t2.take_out_type,t2.org_code,t1.question_type as question_type,t2.province,t2.city
from(
select t1.shop_id,question_type from
(select shop_id,
case when (is_has_business_license='0' or is_has_business_license is null)  then '11' end question_type
from  takeout.tbl_ex_entity_info_snapshot 
where ocr_license_no is not null and dt='${yesterday}'
) as t1
left join(
select t1.shop_id from(
select distinct shop_id from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}') as t1
left join
(select distinct shop_id from takeout.tbl_ex_entity_question_info_snapshot where dt='${yesterday}') as t2
on t1.shop_id=t2.shop_id
where t2.shop_id is not null
)as t2 on t1.shop_id=t2.shop_id
where t2.shop_id is null ) as t1
left join 
(select * from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' )as t2
 on t1.shop_id=t2.shop_id) as t where t.question_type is not null"

executeHiveCommand "${hive_sql5}"

echo '##### //不匹配'
hive_sql6="insert into table takeout.tbl_ex_entity_question_info_snapshot PARTITION(dt='${yesterday}')
select * from (
select t2.entity_id,t1.shop_id,t2.take_out_type,t2.org_code,t1.question_type as question_type,t2.province,t2.city
from(
select t1.shop_id,question_type from
(select shop_id,
case when shop_license_no!=ocr_license_no then '12' end question_type
from  takeout.tbl_ex_entity_info_snapshot 
where ocr_license_no is not null and dt='${yesterday}' and take_out_type='2'
) as t1
left join(
select t1.shop_id from(
select distinct shop_id from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}') as t1
left join
(select distinct shop_id from takeout.tbl_ex_entity_question_info_snapshot where dt='${yesterday}' and question_type!='11') as t2
on t1.shop_id=t2.shop_id
where t2.shop_id is not null
)as t2 on t1.shop_id=t2.shop_id
where t2.shop_id is null ) as t1
left join 
(select * from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' )as t2
 on t1.shop_id=t2.shop_id) as t where t.question_type is not null"

executeHiveCommand "${hive_sql6}"

#echo "#########问题类型为12， 同一个实体，既有日期对应上的（药监局过期日期跟图片识别上报过期日期）shop_id，也有日期对应不上的shop_id#########"

#hive_sql6_1="insert into table takeout.tbl_ex_entity_question_info_snapshot PARTITION(dt='${yesterday}')
#select t.entity_id,t.shop_id,t.take_out_type,t.org_code,t.question_type ,t.province,t.city 
#from (
#select t1.entity_id,t1.shop_id,t1.take_out_type,t1.org_code, case when(t1.license_expire_date=t2.licexpiry) then '0'
#when (t1.license_expire_date!=t2.licexpiry) then '12' end question_type,t1.province,t1.city from (
#select t2.* from(
#select t1.entity_id from (
#select distinct entity_id,question_type from (select a.entity_id,a.license_expire_date,b.licno,b.licexpiry,case when(a.license_expire_date=b.licexpiry) then 'yes'
#when (a.license_expire_date!=b.licexpiry) then 'not' end question_type 
#from (select entity_id,license_expire_date from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' and ocr_license_no is not null) as a 
#left join (select licno,licexpiry from takeout.tbl_ex_license_info) as b 
#on a.entity_id=b.licno where b.licno is not null ) t where question_type = 'not') t1 

#inner join  
#(select distinct entity_id,question_type from (select a.entity_id,a.license_expire_date,b.licno,b.licexpiry,case when(a.license_expire_date=b.licexpiry) then 'yes'
#when (a.license_expire_date!=b.licexpiry) then 'not' end question_type 
#from (select entity_id,license_expire_date from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' and ocr_license_no is not null) as a 
#left join (select licno,licexpiry from takeout.tbl_ex_license_info) as b 
#on a.entity_id=b.licno where b.licno is not null ) t where question_type = 'yes') t2 on t1.entity_id=t2.entity_id 

#) t1 left join
 #(select * from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' )as t2
 #on t1.entity_id=t2.entity_id   ) as t1
 #left join (select licno,licexpiry from takeout.tbl_ex_license_info) as t2 
 #on t1.entity_id=t2.licno where t2.licno is not null
 #) as t  where t.question_type = '12' and t.question_type is not null;"
 
#executeHiveCommand "${hive_sql6_1}"


echo '##### //没有问题的主体'
hive_sql7="insert into table takeout.tbl_ex_entity_question_info_snapshot PARTITION(dt='${yesterday}')
select * from (select t2.entity_id,t1.shop_id,t2.take_out_type,t2.org_code,'0' as question_type,t2.province,t2.city
from(
select t1.shop_id from(
select distinct shop_id from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}') as t1
left join
(select distinct shop_id from takeout.tbl_ex_entity_question_info_snapshot where dt='${yesterday}') as t2
on t1.shop_id=t2.shop_id
where t2.shop_id is null
)as t1
left join 
(select * from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}' )as t2
 on t1.shop_id=t2.shop_id) as t where t.question_type is not null"

executeHiveCommand "${hive_sql7}"
