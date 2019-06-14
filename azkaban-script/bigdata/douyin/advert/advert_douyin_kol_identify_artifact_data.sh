#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 认证脚本: 1. 同步认证特征词库; 2. 同步认证层级; 3. 数据关联同步到人工标记表 douyin_advert_identify_artifact_result_data

#昨天的日期
date=$1
#昨天的日期
yesterday=`date -d "-0 day $date" +%Y-%m-%d`
#前天的日期
beforyesterday=`date -d "-1 day $date" +%Y-%m-%d`
# 获取最近的认证标签库时间
RECENT_DAY_ID=$(hdfs dfs -ls /data/douyin/advert_snapshot/douyin_kol_mark_data/cert_labels/ | awk -F '/' '{print $7}' | sort | tail -n 1)
# 获取最近的认证标签层级
RECENT_DAY_ID1=$(hdfs dfs -ls /data/douyin/advert_snapshot/douyin_kol_mark_data/cert_tier/ | awk -F '/' '{print $7}' | sort | tail -n 1)
# 获取最近的人工认证信息
RECENT_DAY_ID2=$(hdfs dfs -ls /data/douyin/advert_snapshot/douyin_kol_mark_data/work_order_cert/ | awk -F '/' '{print $7}' | sort | tail -n 1)

# 如果是昨天上传的标签库则覆盖原来的特征词库
if [ ${yesterday} == ${RECENT_DAY_ID} ];then
hive -e "
use bigdata;
load data inpath '/data/douyin/advert_snapshot/douyin_kol_mark_data/cert_labels/${yesterday}/'
into table bigdata.identitylabels_origin_orc partition (dt='${yesterday}');

insert overwrite table bigdata.identitylabels_origin
select labels_code,labels,keyword,id_range
from bigdata.identitylabels_origin_orc
where dt='${yesterday}' and labels_code != '' and labels_code is not null
group by labels_code,labels,keyword,id_range;
"
fi


# 如果是昨天上传的标签层级则覆盖原来的认证标签层级
if [ ${yesterday} == ${RECENT_DAY_ID1} ];then
hive -e "
use bigdata;
load data inpath '/data/douyin/advert_snapshot/douyin_kol_mark_data/cert_tier/${yesterday}/'
into table bigdata.advert_cert_orc partition (dt='${yesterday}');

insert overwrite table advert_cert_v1
select id,name,depth,pid
from bigdata.advert_cert_orc
where dt = '${yesterday}' and id != '' and id is not null
group by id,name,depth,pid;
"
fi

# 通过人工导入的表 关联 标签库生成 人工认证
if [ ${yesterday} == ${RECENT_DAY_ID2} ];then
hive -e "
use bigdata;
load data inpath '/data/douyin/advert_snapshot/douyin_kol_mark_data/work_order_cert/${yesterday}/'
into table bigdata.douyin_advert_identify_code_data_origin partition (dt='${yesterday}');

insert into table douyin_advert_identify_artifact_result_data partition (dt='${yesterday}')
select distinct t1.kol_id,'',t1.labels_code,t2.labels,t2.id_range
from (select kol_id,labels_code
    from douyin_advert_identify_code_data_origin
    where dt = '${yesterday}' and kol_id != '' and kol_id is not null) t1
  left join identitylabels_origin t2
  on t1.labels_code = t2.labels_code;
"
fi

# 人工认证 每日全量快照
hive -e "
use bigdata;
insert into table bigdata.douyin_advert_identify_artifact_result_data_snapshot partition (dt='${yesterday}')
select user_id,array_key,labels_code,labels,id_range
from (
    select user_id,array_key,labels_code,labels,id_range,row_number() over (partition by user_id order by dt desc) rank
    from (
        select t1.user_id,t1.array_key,t1.labels_code,t1.labels,t1.id_range,t1.dt
            from bigdata.douyin_advert_identify_artifact_result_data t1 where t1.dt = '${yesterday}'
        union all
        select t2.user_id,t2.array_key,t2.labels_code,t2.labels,t2.id_range,t2.dt
            from bigdata.douyin_advert_identify_artifact_result_data t2 where t2.dt = '${beforyesterday}'
        ) t1
    ) t1
where rank = 1;"