#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 根据zeta清洗的百度分词查询结果 生成 品牌原始表数据

today=$1

yesterday=`date -d "-1 day $today" +%Y-%m-%d`
dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_1=`date -d "-1 day $dayBeforeYesterday" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive_sql="${COMMON_VAR}insert overwrite table bigdata.douyin_advert_brand_data_origin partition(dt = '${yesterday}')
            select md5(brand), brand, 0, kw, md5(pl)
              from (select kw, brand, category, sum(score) total_score
                      from (select kw,
                                   brand,
                                   category,
                                   title_count,
                                   content_count,
                                   case type
                                     when 'rs' then
                                      title_count * 1 + content_count * 1
                                     when 'result' then
                                      title_count * 1 + content_count * 1
                                     else
                                      0
                                   end score
                              from bigdata.advert_douyin_bd_brand_origin
                             where dt = '${yesterday}'
                               and (title_count > 0 or content_count > 0)
                            ) t1
                     group by kw, brand, category
                    having sum(score) >= 10) t2 lateral VIEW explode(split(t2.category, ',')) pls as pl;"

executeHiveCommand "${COMMON_VAR}${hive_sql}"