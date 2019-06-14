#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_kol_property_brand_industry.sh,douyin_advert_kol_calc_data.sh,douyin_advert_content_calc_data.sh
# 导出到ES KOL统计信息->营销分析->互动趋势

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

cycle=1
echo "++++++++++++++++++++++++++++++++导出KOL统计信息 营销分析中互动趋势到ES 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
    insert into bigdata.advert_douyin_user_brand_es partition(dt='${dayBeforeYesterday}',cycle=${cycle})
    SELECT '${stat_date}' AS stat_month,
           unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd')*1000,
           d1.kol_id,
           'douyin' platform,
           '0' platform_kol_id,
           count(1) mention_content_num,
           sum(new_interact_num) mention_content_interact_num,
           '${dayBeforeYesterday}',
           ${cycle}
      FROM (SELECT kol_id, content_id
              FROM bigdata.douyin_advert_source_keywords
             WHERE dt > '${date_reduce}'
               AND dt <= '${dayBeforeYesterday}'
               AND to_date(source_date) > '${date_reduce}'
             GROUP BY kol_id, content_id) d1
      LEFT JOIN bigdata.douyin_advert_content_calc_data d2
        ON d1.content_id = d2.content_id
       AND d2.dt = '${dayBeforeYesterday}'
       AND d2.cycle = ${cycle}
     GROUP BY d1.kol_id;"

executeHiveCommand "${COMMON_VAR}${es_sql}"