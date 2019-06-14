#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖advert_douyin_category_volume
# 导出到ES 品牌品类分析-> 品类的声量指数
# 声量指数改为从此处获取，因应重新计算时，只有声量指数需要重新计算

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;"

for cycle in 1 7 30 60
do
echo "++++++++++++++++++++++++++++++++导出品牌品类分析中 品类的声量指数 到ES 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
es_sql="${es_sql}
    insert into bigdata.advert_douyin_category_volume_es partition(dt='${dayBeforeYesterday}',cycle=${cycle})
    SELECT '${stat_date}' AS stat_month,
           unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd')*1000,
           t.category_id,
           t.category_deep,
           t.mention_content_interact_min,
           t.mention_content_interact_max,
           t.mention_content_interact_avg,
           t.volume,
           '${dayBeforeYesterday}',
           ${cycle},
           'douyin' platform
      FROM bigdata.douyin_advert_category_volume t
     where dt = '${dayBeforeYesterday}'
       and cycle = ${cycle};"
done
executeHiveCommand "${COMMON_VAR}${es_sql}"