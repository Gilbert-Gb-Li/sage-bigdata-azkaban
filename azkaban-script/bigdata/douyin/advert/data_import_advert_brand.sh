#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

#创建临时表
tmp_table_name_suffix="tmp_brand_table"
hive_sql1="select g1.bid as id,g1.name,g1.pid,g1.sub_id1,g1.sub_name1,g1.sub_pid1,g1.sub_id2,g1.sub_name2,g1.sub_pid2,g1.sub_id3,g1.sub_name3,g1.sub_pid3,g1.sub_id4,g1.sub_name4,g1.sub_pid4,g1.sub_id5,g1.sub_name5,g1.sub_pid5,g1.sub_id6,g1.sub_name6,g1.sub_pid6,g1.sub_id7,g1.sub_name7,g1.sub_pid7,g1.sub_id8,g1.sub_name8,g1.sub_pid8,g1.sub_id9,g1.sub_name9,g1.sub_pid9,g2.bid as sub_id10,g2.name as sub_name10,g2.pid as sub_pid10 from
(select f1.bid,f1.name,f1.pid,f1.sub_id1,f1.sub_name1,f1.sub_pid1,f1.sub_id2,f1.sub_name2,f1.sub_pid2,f1.sub_id3,f1.sub_name3,f1.sub_pid3,f1.sub_id4,f1.sub_name4,f1.sub_pid4,f1.sub_id5,f1.sub_name5,f1.sub_pid5,f1.sub_id6,f1.sub_name6,f1.sub_pid6,f1.sub_id7,f1.sub_name7,f1.sub_pid7,f1.sub_id8,f1.sub_name8,f1.sub_pid8,f2.bid as sub_id9,f2.name as sub_name9,f2.pid as sub_pid9 from
(select e1.bid,e1.name,e1.pid,e1.sub_id1,e1.sub_name1,e1.sub_pid1,e1.sub_id2,e1.sub_name2,e1.sub_pid2,e1.sub_id3,e1.sub_name3,e1.sub_pid3,e1.sub_id4,e1.sub_name4,e1.sub_pid4,e1.sub_id5,e1.sub_name5,e1.sub_pid5,e1.sub_id6,e1.sub_name6,e1.sub_pid6,e1.sub_id7,e1.sub_name7,e1.sub_pid7,e2.bid as sub_id8,e2.name as sub_name8,e2.pid as sub_pid8 from
(select d1.bid,d1.name,d1.pid,d1.sub_id1,d1.sub_name1,d1.sub_pid1,d1.sub_id2,d1.sub_name2,d1.sub_pid2,d1.sub_id3,d1.sub_name3,d1.sub_pid3,d1.sub_id4,d1.sub_name4,d1.sub_pid4,d1.sub_id5,d1.sub_name5,d1.sub_pid5,d1.sub_id6,d1.sub_name6,d1.sub_pid6,d2.bid as sub_id7,d2.name as sub_name7,d2.pid as sub_pid7 from
(select b1.bid,b1.name,b1.pid,b1.sub_id1,b1.sub_name1,b1.sub_pid1,b1.sub_id2,b1.sub_name2,b1.sub_pid2,b1.sub_id3,b1.sub_name3,b1.sub_pid3,b1.sub_id4,b1.sub_name4,b1.sub_pid4,b1.sub_id5,b1.sub_name5,b1.sub_pid5,b2.bid as sub_id6,b2.name as sub_name6,b2.pid as sub_pid6 from
(select c1.bid,c1.name,c1.pid,c1.sub_id1,c1.sub_name1,c1.sub_pid1,c1.sub_id2,c1.sub_name2,c1.sub_pid2,c1.sub_id3,c1.sub_name3,c1.sub_pid3,c1.sub_id4,c1.sub_name4,c1.sub_pid4,c2.bid as sub_id5,c2.name as sub_name5,c2.pid as sub_pid5 from
(select m1.bid,m1.name,m1.pid,m1.sub_id1,m1.sub_name1,m1.sub_pid1,m1.sub_id2,m1.sub_name2,m1.sub_pid2,m1.sub_id3,m1.sub_name3,m1.sub_pid3,m2.bid as sub_id4,m2.name as sub_name4,m2.pid as sub_pid4 from
(select s1.bid,s1.name,s1.pid,s1.sub_id1,s1.sub_name1,s1.sub_pid1,s1.sub_id2,s1.sub_name2,s1.sub_pid2,s2.bid as sub_id3,s2.name as sub_name3,s2.pid as sub_pid3 from
(select t1.bid,t1.name,t1.pid,t1.sub_id1,t1.sub_name1,t1.sub_pid1,t2.bid as sub_id2,t2.name as sub_name2,t2.pid as sub_pid2 from
(select a.bid,a.name,a.pid,b.bid as sub_id1,b.name as sub_name1,b.pid as sub_pid1 from
(select * from bigdata.douyin_advert_brand_daily_data where dt = '${yesterday}' and pid = '0') a
left join
(select * from bigdata.douyin_advert_brand_daily_data where dt = '${yesterday}' and pid != '0') b
on a.bid = b.pid) t1
left join
(select * from bigdata.douyin_advert_brand_daily_data where dt = '${yesterday}' and pid != '0') t2
on t1.sub_id1 = t2.pid) s1
left join
(select * from bigdata.douyin_advert_brand_daily_data where dt = '${yesterday}' and pid != '0') s2
on s1.sub_id2 = s2.pid) m1
left join
(select * from bigdata.douyin_advert_brand_daily_data where dt = '${yesterday}' and pid != '0') m2
on m1.sub_id3 = m2.pid) c1
left join
(select * from bigdata.douyin_advert_brand_daily_data where dt = '${yesterday}' and pid != '0') c2
on c1.sub_id4 = c2.pid) b1
left join
(select * from bigdata.douyin_advert_brand_daily_data where dt = '${yesterday}' and pid != '0') b2
on b1.sub_id5 = b2.pid) d1
left join
(select * from bigdata.douyin_advert_brand_daily_data where dt = '${yesterday}' and pid != '0') d2
on d1.sub_id6 = d2.pid) e1
left join
(select * from bigdata.douyin_advert_brand_daily_data where dt = '${yesterday}' and pid != '0') e2
on e1.sub_id7 = e2.pid) f1
left join
(select * from bigdata.douyin_advert_brand_daily_data where dt = '${yesterday}' and pid != '0') f2
on f1.sub_id8 = f2.pid) g1
left join
(select * from bigdata.douyin_advert_brand_daily_data where dt = '${yesterday}' and pid != '0') g2
on g1.sub_id9 = g2.pid;"

tmp_brand_table=`hiveSqlToTmpHive "${hive_sql1}" "${tmp_table_name_suffix}"`
#拆分表生成品类表结构
hive_sql2="INSERT overwrite table bigdata.advert_brand PARTITION(dt='${yesterday}')
select distinct id,name,pid,depth,path,brand1,brand2,brand3,brand4,brand5,brand6,brand7,brand8,brand9,brand10 from
(select id,name,pid,depth,path,case when brand1 is null then '' else brand1 end brand1,case when brand2 is null then '' else brand2 end brand2,
case when brand3 is null then '' else brand3 end brand3,case when brand4 is null then '' else brand4 end brand4,
case when brand5 is null then '' else brand5 end brand5,case when brand6 is null then '' else brand6 end brand6,
case when brand7 is null then '' else brand7 end brand7,case when brand8 is null then '' else brand8 end brand8,
case when brand9 is null then '' else brand9 end brand9,case when brand10 is null then '' else brand10 end brand10 from
(select id,name,pid,size(split(path,',')) as depth,path,split(path,',')[0] as brand1,split(path,',')[1] as brand2,split(path,',')[2] as brand3,split(path,',')[3] as brand4,split(path,',')[4] as brand5,split(path,',')[4] as brand6,split(path,',')[4] as brand7,split(path,',')[4] as brand8,split(path,',')[4] as brand9,split(path,',')[4] as brand10 from
(select id,name,pid,concat_ws(',',id) as path from
$tmp_brand_table t
union all 
select sub_id1 as id,sub_name1 as name,sub_pid1 as pid,concat_ws(',',id,sub_id1) as path from
$tmp_brand_table t
union all
select sub_id2 as id,sub_name2 as name,sub_pid2 as pid,concat_ws(',',id,sub_id1,sub_id2) as path from
$tmp_brand_table t
union all
select sub_id3 as id,sub_name3 as name,sub_pid3 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3) as path from
$tmp_brand_table t
union all
select sub_id4 as id,sub_name4 as name,sub_pid4 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid4) as path from
$tmp_brand_table t
union all
select sub_id5 as id,sub_name5 as name,sub_pid5 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid5,sub_pid5) as path from
$tmp_brand_table t
union all
select sub_id6 as id,sub_name6 as name,sub_pid6 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid4,sub_pid5,sub_pid6) as path from
$tmp_brand_table t
union all
select sub_id7 as id,sub_name7 as name,sub_pid7 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid4,sub_pid5,sub_pid6,sub_pid7) as path from
$tmp_brand_table t
union all
select sub_id8 as id,sub_name8 as name,sub_pid8 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid4,sub_pid5,sub_pid6,sub_pid7,sub_pid8) as path from
$tmp_brand_table t
union all
select sub_id9 as id,sub_name9 as name,sub_pid9 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid4,sub_pid5,sub_pid6,sub_pid7,sub_pid8,sub_pid9) as path from
$tmp_brand_table t
union all
select sub_id10 as id,sub_name10 as name,sub_pid10 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid4,sub_pid5,sub_pid6,sub_pid7,sub_pid8,sub_pid9,sub_pid10) as path from
$tmp_brand_table t) m
where m.id is not null) t) s;"

executeHiveCommand "${COMMON_VAR}${hive_sql2}"
#删除临时表
drop_table_sql="DROP TABLE $tmp_brand_table"

executeHiveCommand "${COMMON_VAR}${drop_table_sql}"

hive_sql3="INSERT overwrite table bigdata.douyin_advert_brand_keywords PARTITION(dt='${yesterday}')
select a.keyword_id,a.keyword_name,a.bid,b.brand_1,b.brand_2,b.brand_3,b.brand_4,b.brand_5,b.brand_6,b.brand_7,b.brand_8,b.brand_9,b.brand_10,a.cid,c.pl_1,c.pl_2,c.pl_3,c.pl_4,c.pl_5,c.pl_6,c.pl_7,c.pl_8,c.pl_9,c.pl_10 from
(select sha1(keywords1) as keyword_id,keywords1 as keyword_name,bid,cid from bigdata.douyin_advert_brand_daily_data LATERAL VIEW explode(split(keywords,',')) table1 as keywords1 where dt = '${yesterday}') a
left join
(select id,brand_1,brand_2,brand_3,brand_4,brand_5,brand_6,brand_7,brand_8,brand_9,brand_10 from bigdata.advert_brand where dt = '${yesterday}') b
on a.bid = b.id
left join
(select id,pl_1,pl_2,pl_3,pl_4,pl_5,pl_6,pl_7,pl_8,pl_9,pl_10 from bigdata.advert_category where dt = '${yesterday}') c
on a.cid = c.id;"

executeHiveCommand "${COMMON_VAR}${hive_sql3}"