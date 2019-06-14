#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# kol兴趣  （历史+今天）去重 -> 当日全量快照

yesterday=`date -d "-0 day $1" +%Y-%m-%d`
RECENT_DAY_ID=$(hive -e "show partitions bigdata.douyin_kol_mark_data_orc;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -u|tail -n 1)
RECENT_DAY_ID2=$(hive -e "show partitions bigdata.douyin_kol_mark_data;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -u|tail -n 1)


if [ ${RECENT_DAY_ID} == ${yesterday} ];then
hive -e "
insert into bigdata.douyin_kol_mark_data partition(dt='${yesterday}')
select b.platform,b.kol_id,b.cert_label_id
from
   (select platform,kol_id,cert_label_id,
           dt,row_number() over (partition by kol_id order by dt desc) rank
    from
        (select t1.platform, t1.kol_id,t1.cert_label_id,t1.dt
         from bigdata.douyin_kol_mark_data_orc t1
         where t1.dt='${RECENT_DAY_ID}' and t1.kol_id is not null
         union all
         select t2.platform,t2.kol_id,t2.cert_label_id,t2.dt
         from bigdata.douyin_kol_mark_data t2
         where t2.dt='${RECENT_DAY_ID2}' and t2.kol_id is not null) a
    ) b
where b.rank=1;"
else
echo "无需更新"
fi