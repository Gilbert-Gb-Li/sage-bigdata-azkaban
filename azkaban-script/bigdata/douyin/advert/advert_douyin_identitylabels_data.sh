#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
#昨天的日期
date=$1
#昨天的日期
yesterday=`date -d "-0 day $date" +%Y-%m-%d`

# 自动配置KOL身份认证 匹配到到输出到结果表   其他输出到人工表导出

hive_sql="
use bigdata;
-- 临时表创建
create temporary table bigdata.temporary_identitylabels_origin (
  labels_code string ,
  labels string ,
  array_key array<string> ,
  id_range int
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n' STORED AS TEXTFILE;

-- 临时表插入数据 temporary_identitylabels_origin
insert into table temporary_identitylabels_origin
select labels_code,labels,split(key,'\\\\+') as array_key,id_range
from (select labels_code,labels,key,id_range
      from bigdata.identitylabels_origin
      LATERAL VIEW explode(split(keyword,'/')) t2 as key) t
where id_range!='-1';
---------------------------------------------------------------------------
insert into bigdata.douyin_advert_identify_code_data partition(dt='${yesterday}')
-- 认证信息
select user_id,b.array_key,labels_code,b.labels,b.id_range
from
     (select user_id,certificate_type,identify_array(certificate_info) as ia
      from douyin_advert_kol_data_snapshot
      where dt='${yesterday}'
        and identify_array(certificate_info) is not NULL)a
join temporary_identitylabels_origin b
on a.ia=b.array_key
union all
-- 用户签名
select user_id,b.array_key,labels_code,b.labels,b.id_range
from
     (select user_id,certificate_type,identify_array(signature) as ia
      from douyin_advert_kol_data_snapshot
      where dt='${yesterday}'
        and identify_array(certificate_info) is NULL
        and identify_array(signature) is not NULL
        and certificate_type='-1')a
join temporary_identitylabels_origin b
on a.ia=b.array_key
union all
-- 用户昵称
select user_id,b.array_key,labels_code,b.labels,b.id_range
from
     (select user_id,certificate_type,identify_array(nick_name) as ia
      from douyin_advert_kol_data_snapshot
      where dt='${yesterday}'
        and identify_array(certificate_info) is NULL
        and identify_array(signature) is NULL
        and identify_array(nick_name) is not NULL
        and certificate_type='-1')a
join temporary_identitylabels_origin b
on a.ia=b.array_key;
---------------------------------------------------------------------------
-- 删除临时表
drop table if exists bigdata.temporary_identitylabels_origin;
------------------------------------------------------------------------------------------
-- 临时表创建
create temporary table bigdata.temporary_identify_code (
  user_id string ,
  num bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n' STORED AS TEXTFILE;
-- 临时表插入数据
insert into table bigdata.temporary_identify_code
select a.user_id, count(*) num
from douyin_advert_identify_code_data a
where dt='${yesterday}' group by user_id;
------------------------------------------------------------------------------------------
-- 系统标记 +  人工标记 = 全量标记（冲突以人工为准）
insert into bigdata.douyin_advert_identify_code_result_data partition(dt='${yesterday}')
select t1.user_id,t1.array_key,t1.labels_code,t1.labels,t1.id_range
      from bigdata.douyin_advert_identify_artifact_result_data_snapshot t1 where t1.dt = '${yesterday}'
union all
select b.user_id,b.array_key,b.labels_code,b.labels,b.id_range
from bigdata.douyin_advert_identify_code_data b
where  b.dt='${yesterday}' and
    b.user_id in
     (select t1.user_id
        from (select user_id from temporary_identify_code where num = 1) t1
            left join
            (select t1.user_id from bigdata.douyin_advert_identify_artifact_result_data_snapshot t1
                where t1.dt = '${yesterday}') t2
            on t1.user_id = t2.user_id
        where t2.user_id is null);
------------------------------------------------------------------------------------------
-- （系统标注为多条 && 没在人工标注中） + （机器标注为一条 && 与人工标注不一致的）
insert into bigdata.douyin_advert_identify_code_artificial_data partition(dt='${yesterday}')
-- 系统标注为多条 && 没在人工标注中
select b.user_id,b.array_key,b.labels_code,b.labels,b.id_range,0
from bigdata.douyin_advert_identify_code_data b
where  b.dt='${yesterday}' and
    b.user_id in
     (select t1.user_id
        from (select user_id from temporary_identify_code where num > 1) t1
            left join
            (select t1.user_id from bigdata.douyin_advert_identify_artifact_result_data_snapshot t1
                where t1.dt = '${yesterday}') t2
            on t1.user_id = t2.user_id
        where t2.user_id is null)
union all
-- 机器标注为一条 && 与人工标注不一致的
select t1.user_id,t1.array_key,t1.labels_code,t1.labels,t1.id_range,1
from (select b.user_id,b.array_key,b.labels_code,b.labels,b.id_range
    from bigdata.douyin_advert_identify_code_data b
    where  b.dt='${yesterday}' and
      b.user_id in (select user_id from temporary_identify_code where num = 1)) t1
  left join
  (select t1.user_id,t1.labels_code from bigdata.douyin_advert_identify_artifact_result_data_snapshot t1
                where t1.dt = '${yesterday}') t2
  on t1.user_id = t2.user_id
where t1.labels_code != t2.labels_code;
------------------------------------------------------------------------------------------
-- 删除临时表
drop table if exists bigdata.temporary_identify_code;
"

 executeHiveCommand "${hive_sql}"