#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

today=$1
yesterday=`date -d "-0 day $today" +%Y-%m-%d`
beforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
RECENT_DAY_ID=$(hive -e "show partitions bigdata.advert_douyin_kol_mark_cert_data;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -u|tail -n 1)

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"
# KOL兴趣认证  （历史+今天）去重 -> 全量
hive -e "${COMMON_VAR}insert into bigdata.advert_douyin_kol_mark_daily_snapshot partition(dt='${yesterday}')
select b.kol_id,b.interest_id,b.cert_label_id
from
   (select kol_id,interest_id,cert_label_id,
           row_number() over (partition by kol_id order by dt desc) rank
    from
        (select t1.kol_id,t1.interest_id,t1.cert_label_id,t1.dt
         from bigdata.advert_douyin_kol_mark_daily_snapshot t1
         where t1.dt='${beforeYesterday}' and t1.kol_id is not null
         union all
         select t2.kol_id,t2.interest_id,t2.cert_label_id,t2.dt
         from bigdata.advert_douyin_kol_mark_cert_data t2
         where t2.dt='${RECENT_DAY_ID}' and t2.kol_id is not null) a
    ) b
where b.rank=1;"
