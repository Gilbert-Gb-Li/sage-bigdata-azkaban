#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 认证脚本:  数据关联同步到 douyin_kol_mark_data_orc

#昨天的日期
date=$1
#昨天的日期
yesterday=`date -d "-0 day $date" +%Y-%m-%d`
#前天的日期
beforyesterday=`date -d "-1 day $date" +%Y-%m-%d`
# 获取最近的人工兴趣信息
RECENT_DAY_ID=$(hdfs dfs -ls /data/douyin/advert_snapshot/douyin_kol_mark_data/work_order_interest/ | awk -F '/' '{print $7}' | sort | tail -n 1)



# 通过人工导入的表 关联 标签库生成 原始表
if [ ${yesterday} == ${RECENT_DAY_ID} ];then
hive -e "
use bigdata;
load data inpath '/data/douyin/advert_snapshot/douyin_kol_mark_data/work_order_interest/${yesterday}/'
into table bigdata.douyin_kol_mark_data_artifact_orc partition (dt='${yesterday}');

insert into table douyin_kol_mark_data_orc partition (dt='${yesterday}')
select distinct platform,kol_id,labels_code
from douyin_kol_mark_data_artifact_orc
where dt = '${yesterday}' and kol_id != '' and kol_id is not null;
"
fi