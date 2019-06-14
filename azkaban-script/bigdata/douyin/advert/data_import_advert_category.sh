#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

#创建临时表
tmp_table_name_suffix="tmp_category_table"
hive_sql1="select g1.cid as id,g1.name,g1.pid,g1.sub_id1,g1.sub_name1,g1.sub_pid1,g1.sub_id2,g1.sub_name2,g1.sub_pid2,g1.sub_id3,g1.sub_name3,g1.sub_pid3,g1.sub_id4,g1.sub_name4,g1.sub_pid4,g1.sub_id5,g1.sub_name5,g1.sub_pid5,g1.sub_id6,g1.sub_name6,g1.sub_pid6,g1.sub_id7,g1.sub_name7,g1.sub_pid7,g1.sub_id8,g1.sub_name8,g1.sub_pid8,g1.sub_id9,g1.sub_name9,g1.sub_pid9,g2.cid as sub_id10,g2.name as sub_name10,g2.pid as sub_pid10 from
(select f1.cid,f1.name,f1.pid,f1.sub_id1,f1.sub_name1,f1.sub_pid1,f1.sub_id2,f1.sub_name2,f1.sub_pid2,f1.sub_id3,f1.sub_name3,f1.sub_pid3,f1.sub_id4,f1.sub_name4,f1.sub_pid4,f1.sub_id5,f1.sub_name5,f1.sub_pid5,f1.sub_id6,f1.sub_name6,f1.sub_pid6,f1.sub_id7,f1.sub_name7,f1.sub_pid7,f1.sub_id8,f1.sub_name8,f1.sub_pid8,f2.cid as sub_id9,f2.name as sub_name9,f2.pid as sub_pid9 from
(select e1.cid,e1.name,e1.pid,e1.sub_id1,e1.sub_name1,e1.sub_pid1,e1.sub_id2,e1.sub_name2,e1.sub_pid2,e1.sub_id3,e1.sub_name3,e1.sub_pid3,e1.sub_id4,e1.sub_name4,e1.sub_pid4,e1.sub_id5,e1.sub_name5,e1.sub_pid5,e1.sub_id6,e1.sub_name6,e1.sub_pid6,e1.sub_id7,e1.sub_name7,e1.sub_pid7,e2.cid as sub_id8,e2.name as sub_name8,e2.pid as sub_pid8 from
(select d1.cid,d1.name,d1.pid,d1.sub_id1,d1.sub_name1,d1.sub_pid1,d1.sub_id2,d1.sub_name2,d1.sub_pid2,d1.sub_id3,d1.sub_name3,d1.sub_pid3,d1.sub_id4,d1.sub_name4,d1.sub_pid4,d1.sub_id5,d1.sub_name5,d1.sub_pid5,d1.sub_id6,d1.sub_name6,d1.sub_pid6,d2.cid as sub_id7,d2.name as sub_name7,d2.pid as sub_pid7 from
(select b1.cid,b1.name,b1.pid,b1.sub_id1,b1.sub_name1,b1.sub_pid1,b1.sub_id2,b1.sub_name2,b1.sub_pid2,b1.sub_id3,b1.sub_name3,b1.sub_pid3,b1.sub_id4,b1.sub_name4,b1.sub_pid4,b1.sub_id5,b1.sub_name5,b1.sub_pid5,b2.cid as sub_id6,b2.name as sub_name6,b2.pid as sub_pid6 from
(select c1.cid,c1.name,c1.pid,c1.sub_id1,c1.sub_name1,c1.sub_pid1,c1.sub_id2,c1.sub_name2,c1.sub_pid2,c1.sub_id3,c1.sub_name3,c1.sub_pid3,c1.sub_id4,c1.sub_name4,c1.sub_pid4,c2.cid as sub_id5,c2.name as sub_name5,c2.pid as sub_pid5 from
(select m1.cid,m1.name,m1.pid,m1.sub_id1,m1.sub_name1,m1.sub_pid1,m1.sub_id2,m1.sub_name2,m1.sub_pid2,m1.sub_id3,m1.sub_name3,m1.sub_pid3,m2.cid as sub_id4,m2.name as sub_name4,m2.pid as sub_pid4 from
(select s1.cid,s1.name,s1.pid,s1.sub_id1,s1.sub_name1,s1.sub_pid1,s1.sub_id2,s1.sub_name2,s1.sub_pid2,s2.cid as sub_id3,s2.name as sub_name3,s2.pid as sub_pid3 from
(select t1.cid,t1.name,t1.pid,t1.sub_id1,t1.sub_name1,t1.sub_pid1,t2.cid as sub_id2,t2.name as sub_name2,t2.pid as sub_pid2 from
(select a.cid,a.name,a.pid,b.cid as sub_id1,b.name as sub_name1,b.pid as sub_pid1 from
(select * from bigdata.douyin_advert_category_daily_data where dt = '${yesterday}' and pid = '0') a
left join
(select * from bigdata.douyin_advert_category_daily_data where dt = '${yesterday}' and pid != '0') b
on a.cid = b.pid) t1
left join
(select * from bigdata.douyin_advert_category_daily_data where dt = '${yesterday}' and pid != '0') t2
on t1.sub_id1 = t2.pid) s1
left join
(select * from bigdata.douyin_advert_category_daily_data where dt = '${yesterday}' and pid != '0') s2
on s1.sub_id2 = s2.pid) m1
left join
(select * from bigdata.douyin_advert_category_daily_data where dt = '${yesterday}' and pid != '0') m2
on m1.sub_id3 = m2.pid) c1
left join
(select * from bigdata.douyin_advert_category_daily_data where dt = '${yesterday}' and pid != '0') c2
on c1.sub_id4 = c2.pid) b1
left join
(select * from bigdata.douyin_advert_category_daily_data where dt = '${yesterday}' and pid != '0') b2
on b1.sub_id5 = b2.pid) d1
left join
(select * from bigdata.douyin_advert_category_daily_data where dt = '${yesterday}' and pid != '0') d2
on d1.sub_id6 = d2.pid) e1
left join
(select * from bigdata.douyin_advert_category_daily_data where dt = '${yesterday}' and pid != '0') e2
on e1.sub_id7 = e2.pid) f1
left join
(select * from bigdata.douyin_advert_category_daily_data where dt = '${yesterday}' and pid != '0') f2
on f1.sub_id8 = f2.pid) g1
left join
(select * from bigdata.douyin_advert_category_daily_data where dt = '${yesterday}' and pid != '0') g2
on g1.sub_id9 = g2.pid;"

tmp_category_table=`hiveSqlToTmpHive "${hive_sql1}" "${tmp_table_name_suffix}"`
#拆分表生成品类表结构
hive_sql2="INSERT overwrite table bigdata.advert_category PARTITION(dt='${yesterday}')
select distinct id,name,pid,depth,path,pl_1,pl_2,pl_3,pl_4,pl_5,pl_6,pl_7,pl_8,pl_9,pl_10 from
(select id,name,pid,depth,path,case when pl_1 is null then '' else pl_1 end pl_1,case when pl_2 is null then '' else pl_2 end pl_2,
case when pl_3 is null then '' else pl_3 end pl_3,case when pl_4 is null then '' else pl_4 end pl_4,
case when pl_5 is null then '' else pl_5 end pl_5,case when pl_6 is null then '' else pl_6 end pl_6,
case when pl_7 is null then '' else pl_7 end pl_7,case when pl_8 is null then '' else pl_8 end pl_8,
case when pl_9 is null then '' else pl_9 end pl_9,case when pl_10 is null then '' else pl_10 end pl_10 from
(select id,name,pid,size(split(path,',')) as depth,path,split(path,',')[0] as pl_1,split(path,',')[1] as pl_2,split(path,',')[2] as pl_3,split(path,',')[3] as pl_4,split(path,',')[4] as pl_5,split(path,',')[4] as pl_6,split(path,',')[4] as pl_7,split(path,',')[4] as pl_8,split(path,',')[4] as pl_9,split(path,',')[4] as pl_10 from
(select id,name,pid,concat_ws(',',id) as path from
$tmp_category_table t
union all 
select sub_id1 as id,sub_name1 as name,sub_pid1 as pid,concat_ws(',',id,sub_id1) as path from
$tmp_category_table t
union all
select sub_id2 as id,sub_name2 as name,sub_pid2 as pid,concat_ws(',',id,sub_id1,sub_id2) as path from
$tmp_category_table t
union all
select sub_id3 as id,sub_name3 as name,sub_pid3 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3) as path from
$tmp_category_table t
union all
select sub_id4 as id,sub_name4 as name,sub_pid4 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid4) as path from
$tmp_category_table t
union all
select sub_id5 as id,sub_name5 as name,sub_pid5 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid5,sub_pid5) as path from
$tmp_category_table t
union all
select sub_id6 as id,sub_name6 as name,sub_pid6 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid4,sub_pid5,sub_pid6) as path from
$tmp_category_table t
union all
select sub_id7 as id,sub_name7 as name,sub_pid7 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid4,sub_pid5,sub_pid6,sub_pid7) as path from
$tmp_category_table t
union all
select sub_id8 as id,sub_name8 as name,sub_pid8 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid4,sub_pid5,sub_pid6,sub_pid7,sub_pid8) as path from
$tmp_category_table t
union all
select sub_id9 as id,sub_name9 as name,sub_pid9 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid4,sub_pid5,sub_pid6,sub_pid7,sub_pid8,sub_pid9) as path from
$tmp_category_table t
union all
select sub_id10 as id,sub_name10 as name,sub_pid10 as pid,concat_ws(',',id,sub_id1,sub_id2,sub_pid3,sub_pid4,sub_pid5,sub_pid6,sub_pid7,sub_pid8,sub_pid9,sub_pid10) as path from
$tmp_category_table t) m
where m.id is not null) t) s;"

executeHiveCommand "${COMMON_VAR}${hive_sql2}"
#删除临时表
drop_table_sql="DROP TABLE $tmp_category_table"

executeHiveCommand "${COMMON_VAR}${drop_table_sql}"

hive_sql3="INSERT overwrite table bigdata.douyin_advert_category_keywords PARTITION(dt='${yesterday}')
select a.keyword_id,a.keyword_name,a.cid,b.pl_1,b.pl_2,b.pl_3,b.pl_4,b.pl_5,b.pl_6,b.pl_7,b.pl_8,b.pl_9,b.pl_10 from
(select sha1(keywords1) as keyword_id,keywords1 as keyword_name,cid from bigdata.douyin_advert_category_daily_data LATERAL VIEW explode(split(keywords,',')) table1 as keywords1 where dt = '${yesterday}') a
left join
(select id,pl_1,pl_2,pl_3,pl_4,pl_5,pl_6,pl_7,pl_8,pl_9,pl_10 from bigdata.advert_category where dt = '${yesterday}') b
on a.cid = b.id;"

executeHiveCommand "${COMMON_VAR}${hive_sql3}"