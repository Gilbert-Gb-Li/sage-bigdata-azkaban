#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 计算活跃商户 #####################"
yesterday=$1

hive_sql="insert into table takeout.tbl_ex_takeout_active_shop PARTITION(dt='${yesterday}')
select a.shop_id,a.shop_name,a.shop_logo,a.open_time,a.address,a.phone,a.province,a.city,a.longitude,
a.latitude,a.geo_hash,a.org_code,b.comment_score,(b.comment_level_3/b.comment_total_num) as negative_comment_rate,
b.comment_total_num,b.comment_level_3,a.take_out_type,a.local_logo,a.last_check_grade,a.place_grade,a.manage_grade,
a.check_date,a.company_name,a.company_owner,a.company_address,a.business_scope,a.business_type,a.expire_time,a.license_num,
a.shop_cert_photo1,a.shop_cert_photo2 from
(select a.shop_id,a.shop_name,a.shop_logo,a.open_time,a.address,a.phone,a.province,a.city,
a.longitude,a.latitude,a.geo_hash,case when (c.regaddrzl is not null and c.regaddrzl !='' and c.regaddrzl != a.org_code) then c.regaddrzl else a.org_code end org_code,a.take_out_type,b.local_logo,b.last_check_grade,b.place_grade,
b.manage_grade,b.check_date,b.company_name,b.company_owner,b.company_address,b.business_scope,b.business_type,
b.expire_time,b.license_num,b.shop_cert_photo1,b.shop_cert_photo2 from
(select * from 
(select t.*, row_number() over (partition by shop_id order by record_time desc) num from(
select record_time,shop_id,shop_name,shop_logo,open_time,address,phone,province,city,longitude,latitude,take_out_type,geo_hash,org_code 
from web.tbl_ex_takeout_shop_origin_orc where dt = '${yesterday}' and org_code!='' and org_code is not null  
) as t) r
where r.num = 1) a
left join
(select * from
(select t.*, row_number() over (partition by shop_id order by record_time desc) num from(
select record_time,shop_id,company_name,company_owner,company_address,business_scope,business_type,
expire_time,license_num,shop_cert_photo1,shop_cert_photo2,local_logo,last_check_grade,place_grade,manage_grade,check_date 
from web.tbl_ex_takeout_shop_info_origin_orc where dt = '${yesterday}') as t) r
where r.num = 1) b
on a.shop_id = b.shop_id

left join
(select distinct(licno), regaddrzl from takeout.tbl_ex_license_info) c
on b.license_num = c.licno

) a
left join
(select * from
(select t.*, row_number() over (partition by shop_id order by record_time desc) num from(
select record_time,shop_id,comment_score,comment_total_num,comment_level_3 
from web.tbl_ex_takeout_shop_comment_total_origin_orc where dt = '${yesterday}') as t) r
where r.num = 1) b
on a.shop_id = b.shop_id

union all

select a.shop_id,a.shop_name,a.shop_logo,a.open_time,a.address,a.phone,a.province,a.city,a.longitude,a.latitude,a.geo_hash,
a.org_code,b.comment_score,(b.comment_level_3/b.comment_total_num) as negative_comment_rate,b.comment_total_num,b.comment_level_3,
a.take_out_type,a.local_logo,a.last_check_grade,a.place_grade,a.manage_grade,a.check_date,a.company_name,a.company_owner,a.company_address,
a.business_scope,a.business_type,a.expire_time,a.license_num,a.shop_cert_photo1,a.shop_cert_photo2 from
(select a.shop_id,a.shop_name,a.shop_logo,a.open_time,a.address,a.phone,a.province,a.city,a.longitude,a.latitude,a.geo_hash,case when (c.regaddrzl is not null and c.regaddrzl !='' and c.regaddrzl != a.org_code) then c.regaddrzl else a.org_code end org_code,
a.take_out_type,b.local_logo,b.last_check_grade,b.place_grade,b.manage_grade,b.check_date,b.company_name,b.company_owner,b.company_address,
b.business_scope,b.business_type,b.expire_time,b.license_num,b.shop_cert_photo1,b.shop_cert_photo2 from
(select * from 
(select t.*, row_number() over (partition by shop_id order by record_time desc) num from(
select record_time,shop_id,shop_name,shop_logo,open_time,address,phone,province,city,longitude,latitude,take_out_type,geo_hash,org_code 
from ias.tbl_ex_takeout_shop_origin_orc where dt = '${yesterday}' and org_code!='' and org_code is not null and take_out_type!= 5 and take_out_type!=11 and take_out_type!=21 and take_out_type!=31) as t) r
where r.num = 1) a
left join
(select * from
(select t.*, row_number() over (partition by shop_id order by record_time desc) num from(
select record_time,shop_id,company_name,company_owner,company_address,business_scope,business_type,expire_time,license_num,
shop_cert_photo1,shop_cert_photo2,local_logo,last_check_grade,place_grade,manage_grade,check_date 
from ias.tbl_ex_takeout_shop_info_origin_orc where dt = '${yesterday}' and shop_cert_photo1 is not null and shop_cert_photo1 !='' ) as t) r
where r.num = 1) b
on a.shop_id = b.shop_id

left join
(select distinct(licno), regaddrzl from takeout.tbl_ex_license_info) c
on b.license_num = c.licno

) a
left join
(select * from
(select t.*, row_number() over (partition by shop_id order by record_time desc) num from(
select record_time,shop_id,comment_score,comment_total_num,comment_level_3 
from ias.tbl_ex_takeout_shop_comment_total_origin_orc where dt = '${yesterday}') as t) r
where r.num = 1) b
on a.shop_id = b.shop_id;"

executeHiveCommand "${hive_sql}"

echo "############### 计算活跃商户 end #####################"
