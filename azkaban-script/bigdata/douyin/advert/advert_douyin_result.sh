#!/bin/sh

today=$1
dayBeforeYesterday=`date -d "-2 day $today" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive -e "use bigdata;${COMMON_VAR}
insert into bigdata.advert_douyin_count_result_daily_snapshot partition(dt='${dayBeforeYesterday}')
select split(record_count,'\t')[0] as user_cn,split(record_count,'\t')[1] as valid_user_cn,split(record_count,'\t')[2] as add_valid_user_cn,
split(record_count,'\t')[3] as vedio_cn,split(record_count,'\t')[4] as valid_vedio_cn,
split(record_count,'\t')[5] as add_valid_vedio_cn,split(record_count,'\t')[6] as topic_cn,
split(record_count,'\t')[7] as valid_topic_cn,split(record_count,'\t')[8] as add_valid_topic_cn,
split(record_count,'\t')[9] as comment_cn,split(record_count,'\t')[10] as valid_comment_cn,
split(record_count,'\t')[11] as kol_cn,split(record_count,'\t')[12] as valid_kol_cn,
split(record_count,'\t')[13] as add_valid_kol_cn,split(record_count,'\t')[14] as kol_vedio_cn,
split(record_count,'\t')[15] as kol_valid_vedio_cn,split(record_count,'\t')[16] as add_kol_valid_vedio_cn,
split(record_count,'\t')[17] as kol_topic_cn,split(record_count,'\t')[18] as kol_valid_topic_cn,
split(record_count,'\t')[19] as add_kol_valid_topic_cn,split(record_count,'\t')[20] as kol_comment_cn,
split(record_count,'\t')[21] as kol_valid_comment_cn
from(
select dt,concat_ws('\t',collect_list(cn)) as record_count 
from bigdata.advert_douyin_user_count_record_daily_snapshot
where dt='${dayBeforeYesterday}'
group by dt) a
where a.dt='${dayBeforeYesterday}';
"
echo "执行完成"
