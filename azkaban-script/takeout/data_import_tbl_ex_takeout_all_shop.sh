#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 计算全量商户 start #####################"
yesterday=$1
echo $yesterday
the_day_before_yesterday=`date -d "-1 day ${yesterday}" +%Y-%m-%d`
echo $the_day_before_yesterday

hive_sql="insert into table takeout.tbl_ex_takeout_all_shop PARTITION(dt='${yesterday}')
select shop_id, shop_name, shop_logo, open_time, address, phone, province, city, longitude, latitude, geo_hash, org_code, 
comment_score, negative_comment_rate, 
comment_total_num, negative_comment_num, take_out_type, local_logo, last_check_grade, place_grade, manage_grade, 
check_date, company_name, company_owner, company_address, 
business_scope, business_type, expire_time, license_num, shop_cert_photo1, shop_cert_photo2 from (
select t.*,row_number() over (partition by shop_id order by dt desc) num from(
select shop_id, shop_name, shop_logo, open_time, address, phone, province, city, longitude, latitude, geo_hash, org_code, 
comment_score, negative_comment_rate, 
comment_total_num, negative_comment_num, take_out_type, local_logo, last_check_grade, place_grade, manage_grade, 
check_date, company_name, company_owner, company_address, 
business_scope, business_type, expire_time, license_num, shop_cert_photo1, shop_cert_photo2,dt
 from takeout.tbl_ex_takeout_active_shop where dt = '${yesterday}' and (shop_cert_photo1 is not null and shop_cert_photo1 !='' 
or shop_cert_photo2 is not null and shop_cert_photo2 !='' or phone is not null and phone!='')
union all
select shop_id, shop_name, shop_logo, open_time, address, phone, province, city, longitude, latitude, geo_hash, 
case when (t2.regaddrzl is not null and t2.regaddrzl !='' and t2.regaddrzl !='null' and t2.regaddrzl != t1.org_code) then t2.regaddrzl else t1.org_code end org_code, 
comment_score, negative_comment_rate, 
comment_total_num, negative_comment_num, take_out_type, local_logo, last_check_grade, place_grade, manage_grade, 
check_date, company_name, company_owner, company_address, 
business_scope, business_type, expire_time, license_num, shop_cert_photo1, shop_cert_photo2,dt from (select * from takeout.tbl_ex_takeout_all_shop where dt = '${the_day_before_yesterday}' and (shop_cert_photo1 is not null and shop_cert_photo1 !='' 
or shop_cert_photo2 is not null and shop_cert_photo2 !='' or phone is not null and phone!='')) t1 left join 
(select  distinct licno,regaddrzl from takeout.tbl_ex_license_info) t2 on t1.license_num=t2.licno 
) as t
) r
where r.num = 1 and r.org_code!='' and r.org_code is not null;"

executeHiveCommand "${hive_sql}"
echo "############### 计算全量商户 end #####################"
