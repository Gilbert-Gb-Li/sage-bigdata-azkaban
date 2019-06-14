#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_kol_calc_data.sh,douyin_advert_content_calc_data.sh
# 导出到ES 粉丝分析 兴趣分布 interest

yesterday=$1

dayBeforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

cycle=30
    echo "++++++++++++++++++++++++++++++++导出KOL粉丝分析 兴趣分布到ES 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
            insert into bigdata.advert_douyin_user_analyze_es partition(dt='${dayBeforeYesterday}',cycle=${cycle})
            SELECT '${stat_date}' AS stat_month,
       unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd') * 1000,
       t.kol_id,
       'douyin' platform,
       '0' platform_kol_id,
       t8.cert_1,
       'interest',
       t10. NAME label,
       count(1) val,
       IF(t8.cert_1 IS NULL,
          0,
          avg(count(1)) over(PARTITION BY t8.cert_1, t10. NAME)),
       '${dayBeforeYesterday}',
       ${cycle}
  FROM (SELECT kol_id, fans_id user_id
          FROM bigdata.douyin_advert_fans_data_snapshot) t
  LEFT JOIN (select kol_id, interest_id, cert_label_id
               from bigdata.advert_douyin_kol_mark_daily_snapshot
              where dt = '${yesterday}') t7
    ON t7.kol_id = t.user_id
  LEFT JOIN bigdata.advert_cert t8
    ON t7.cert_label_id = t8.id
  LEFT JOIN bigdata.advert_interest t9
    ON t7.interest_id = t9.id
  LEFT JOIN bigdata.advert_interest t10
    ON t9.interest_1 = t10.id
 GROUP BY t.kol_id, t8.cert_1, t10. NAME;
"
    executeHiveCommand "${COMMON_VAR}${es_sql}"
