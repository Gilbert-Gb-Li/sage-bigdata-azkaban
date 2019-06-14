#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 拷贝用户评论信息 start #####################"
yesterday=$1
mysql_table="waimai_comment"

hive_sql="select '${yesterday}' as dt,a.shop_id,a.take_out_type,a.user_id,a.user_name,a.score,a.comment,a.comment_time from
(select shop_id,take_out_type,comment_id,user_id,user_name,score,comment,comment_time from
(select t.*,row_number() over (partition by shop_id,comment_id order by record_time desc) num from(
select record_time,shop_id,take_out_type,comment_id,user_id,user_name,score,comment,comment_time from ias.tbl_ex_takeout_shop_comment_detail_origin_orc where dt = '${yesterday}'
union all
select record_time,shop_id,take_out_type,comment_id,user_id,user_name,score,comment,comment_time from web.tbl_ex_takeout_shop_comment_detail_origin_orc where dt = '${yesterday}'
) t) r
where r.num = 1) a
left join
(select shop_id,take_out_type,comment_id,user_id,user_name,score,comment,comment_time from ias.tbl_ex_takeout_shop_comment_detail_origin_orc where dt < '${yesterday}'
union all
select shop_id,take_out_type,comment_id,user_id,user_name,score,comment,comment_time from web.tbl_ex_takeout_shop_comment_detail_origin_orc where dt < '${yesterday}') b
on a.shop_id = b.shop_id and a.comment_id = b.comment_id
where b.comment_id is null ;"

hiveSqlToTakeoutMysql "${hive_sql}" "${yesterday}" "${mysql_table}" "date,shop_id,waimai_id,user_id,user_name,score,comment,comment_time" "date"

echo "############### 拷贝用户评论信息 end #####################"
