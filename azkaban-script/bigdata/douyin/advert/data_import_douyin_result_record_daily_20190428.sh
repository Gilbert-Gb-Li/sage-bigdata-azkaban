#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
#source ${base_path}/util.sh
today=$1
yesterday=`date -d "-0 day $today" +%Y-%m-%d`
table_path='/data/douyin/advert/snapshot/advert_douyin_record_count_result_daily_snapshot'
hdfs dfs -rm ${table_path}/dt=$yesterday/*
hdfs dfs -rmdir ${table_path}/dt=$yesterday
hive -e "use bigdata;
ALTER TABLE bigdata.advert_douyin_record_count_result_daily_snapshot DROP IF EXISTS PARTITION(dt='${yesterday}');
insert overwrite table bigdata.advert_douyin_record_count_result_daily_snapshot partition(dt='${yesterday}')
select
split(record_count,'\t')[7] as user_cn,
split(record_count,'\t')[18] as valid_user_cn,
split(record_count,'\t')[0] as day_valid_user_cn,
split(record_count,'\t')[1] as vedio_cn,
split(record_count,'\t')[2] as valid_vedio_cn,
split(record_count,'\t')[3] as day_valid_vedio_cn,
split(record_count,'\t')[4] as topic_cn,
split(record_count,'\t')[5] as valid_topic_cn,
split(record_count,'\t')[6] as day_valid_topic_cn,
split(record_count,'\t')[8] as comment_cn,
split(record_count,'\t')[9] as valid_comment_cn,
split(record_count,'\t')[10] as day_valid_comment_cn,
split(record_count,'\t')[11] as day_kol_cn,
split(record_count,'\t')[12] as kol_cn,
split(record_count,'\t')[13] as valid_kol_cn,
split(record_count,'\t')[14] as day_add_valid_kol_cn,
split(record_count,'\t')[15] as day_valid_kol_vedio_cn,
split(record_count,'\t')[16] as valid_kol_vedio_cn,
split(record_count,'\t')[17] as valid_kol_valid_vedio_cn,
split(record_count,'\t')[19] as day_add_valid_kol_valid_vedio_cn,
split(record_count,'\t')[20] as valid_kol_topic_cn,
split(record_count,'\t')[21] as day_valid_kol_topic_cn,
split(record_count,'\t')[22] as valid_kol_valid_topic_cn,
split(record_count,'\t')[23] as day_add_valid_kol_valid_topic_cn,
split(record_count,'\t')[24] as valid_kol_comment_cn,
split(record_count,'\t')[25] as day_valid__kol_comment_cn,
split(record_count,'\t')[26] as valid_kol_valid_comment_cn,
split(record_count,'\t')[27] as day_add_valid_kol_valid_comment_cn
from(
select dt,concat_ws('\t',collect_list(cn)) as record_count 
from bigdata.advert_douyin_user_count_record_daily_second_snapshot
where dt='${yesterday}'
group by dt) a
where a.dt='${yesterday}';
"
echo "执行完成"
