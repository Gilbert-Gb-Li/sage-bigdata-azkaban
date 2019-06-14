#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖于分词中间表
# 提及品牌 KOL-营销分析-互动趋势中的弹窗
# 依赖于 douyin_advert_content_calc_data.sh

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

for cycle in 1 7 30 60
do
    echo "++++++++++++++++++++++++++++++++导出 品牌提及 统计周期'${cycle}' ES++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
        insert into bigdata.advert_douyin_brand_content_es partition(dt='${dayBeforeYesterday}',cycle=${cycle})
        SELECT '${stat_date}' AS stat_month,
               unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd')*1000,
               kol_id,
               'douyin' platform,
               '0' platform_kol_id,
               t1.pl_1,
               t3. NAME,
               t1.brand_id,
               t1.brand_name,
               '${dayBeforeYesterday}',
               count(t1.content_id),
               COALESCE(sum(new_interact_num), 0),
               if(COALESCE(count(t1.content_id), 0) = 0,
                  0,
                  COALESCE(sum(new_interact_num), 0) / count(t1.content_id)),
               '${dayBeforeYesterday}',
               ${cycle}
          FROM (SELECT DISTINCT kol_id, pl_1, brand_id, brand_name, content_id
                  FROM bigdata.douyin_advert_source_keywords
                 WHERE dt <= '${dayBeforeYesterday}'
                   and dt >  '${date_reduce}'
                   AND to_date(source_date) > '${date_reduce}'
                   and kol_id is not null) t1
          LEFT JOIN (select * from bigdata.advert_category where dt='${dayBeforeYesterday}') t3
            ON t1.pl_1 = t3.id
          LEFT JOIN (SELECT content_id, new_interact_num
                       FROM bigdata.douyin_advert_content_calc_data
                      WHERE dt = '${dayBeforeYesterday}'
                        AND cycle = ${cycle}
                        AND source_time <= '${dayBeforeYesterday}'
                        AND source_time > '${date_reduce}') t2
            ON t1.content_id = t2.content_id
         GROUP BY kol_id, t1.pl_1, t3. NAME, t1.brand_id, t1.brand_name;"

    executeHiveCommand "${COMMON_VAR}${es_sql}"
done
