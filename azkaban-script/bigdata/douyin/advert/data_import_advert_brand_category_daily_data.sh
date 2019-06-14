#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
#对原始的品牌表先进行去重（两表数据为昨天的和前天的），得到douyin_advert_brand_daily_data表
today=$1
date=`date -d "-1 day $today" +%Y-%m-%d`
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

RECENT_DAY_ID1=$(hive -e "show partitions bigdata.douyin_advert_category_daily_data;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -ur|head -n 1)
RECENT_DAY_ID2=$(hive -e "show partitions bigdata.douyin_advert_brand_daily_data;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -ur|head -n 1)
# RECENT_DAY_ID11=$(hive -e "show partitions bigdata.advert_category_keywords;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -u|head -n 1)
# RECENT_DAY_ID22=$(hive -e "show partitions bigdata.advert_brand_keywords;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -u|head -n 1)

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive_sql="insert overwrite table bigdata.douyin_advert_brand_daily_data partition(dt='${date}')
select bid,name,pid,keywords,cid
from
    (select *,row_number() over (partition by bid order by dt desc) as order_num
     from (select dt,bid,name,pid,keywords,cid
           from bigdata.douyin_advert_brand_data_origin
           where dt = '${date}'
           union all
           select dt,bid,name,pid,keywords,cid
           from bigdata.douyin_advert_brand_daily_data
           where dt = '${RECENT_DAY_ID2}'
           )as p
    )as t
where t.order_num =1;"

executeHiveCommand "${COMMON_VAR}${hive_sql}"

hive_sq2="insert overwrite table bigdata.douyin_advert_category_daily_data partition(dt='${date}')
select cid, name, pid, keywords
  from (select *,
               row_number() over(partition by cid order by dt desc) as order_num
          from (select dt, cid, name, pid, keywords
                  from bigdata.douyin_advert_category_data_origin
                 where dt = '${date}'
                union all
                select dt, cid, name, pid, keywords
                  from bigdata.douyin_advert_category_daily_data
                 where dt = '${RECENT_DAY_ID1}') as p) as t
 where t.order_num = 1;"

executeHiveCommand "${COMMON_VAR}${hive_sq2}"