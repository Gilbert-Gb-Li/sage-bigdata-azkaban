#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

today=$1
yesterday=`date -d "-0 day $today" +%Y-%m-%d`
RECENT_DAY_ID1=$(hive -e "show partitions bigdata.douyin_advert_identify_code_result_data;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -u|tail -n 1)
RECENT_DAY_ID2=$(hive -e "show partitions bigdata.douyin_kol_mark_data;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -u|tail -n 1)

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"
# KOL兴趣认证    kol兴趣+kol认证 -> kol当日全量
if [ ${RECENT_DAY_ID1} == ${yesterday} ] || [ ${RECENT_DAY_ID2} == ${yesterday} ];then
hive -e "${COMMON_VAR}insert into bigdata.advert_douyin_kol_mark_cert_data partition(dt='${yesterday}')
        select a.user_id,a.labels_code,b.cert_label_id
        from (select user_id,labels_code
              from bigdata.douyin_advert_identify_code_result_data
              where dt='${RECENT_DAY_ID1}') a
        join (select kol_id,cert_label_id
              from bigdata.douyin_kol_mark_data
              where dt='${RECENT_DAY_ID2}') b
        on a.user_id=b.kol_id;

        insert into bigdata.advert_douyin_kol_mark_cert_data partition(dt='${yesterday}')
        select c.user_id,c.labels_code,null
        from (select * from bigdata.douyin_advert_identify_code_result_data where dt='${RECENT_DAY_ID1}') c
        left outer join (select * from bigdata.advert_douyin_kol_mark_cert_data where dt='${yesterday}') b
        on c.user_id=b.kol_id
        where  b.kol_id is null

        union all

        select e.kol_id,null,e.cert_label_id
        from (select * from bigdata.douyin_kol_mark_data where dt='${RECENT_DAY_ID2}') e
        left outer join (select * from bigdata.advert_douyin_kol_mark_cert_data where dt='${yesterday}') f
        on e.kol_id=f.kol_id
        where f.kol_id is null;
        "
else
echo "无需更新"
fi