#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_kol_calc_data.sh,douyin_advert_content_calc_data.sh
# 导出到ES 粉丝分析 活跃度 activity_rate  1：点赞    2：评论    3：潜水

yesterday=$1
dayBeforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

cycle=30
    echo "++++++++++++++++++++++++++++++++导出KOL粉丝分析 活跃度到ES 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
              insert into bigdata.advert_douyin_user_analyze_es partition(dt='${dayBeforeYesterday}',cycle=${cycle})
              SELECT '${stat_date}' AS stat_month,
                     unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd')*1000,
                     t.kol_id,
                     'douyin' platform,
                     '0' platform_kol_id,
                     t.cert_1,
                     'activity_rate',
                     CASE type
                       WHEN 1 THEN
                        '点赞率'
                       WHEN 2 THEN
                        '评论率'
                       ELSE
                        '潜水率'
                     END col,
                     CASE type
                       WHEN 1 THEN
                        like_rate
                       WHEN 2 THEN
                        fans_rate
                       ELSE
                        cover_rate
                     END rate,
                     CASE type
                       WHEN 1 THEN
                        avg_like_rage
                       WHEN 2 THEN
                        avg_fans_rate
                       ELSE
                        avg_cover_rate
                     END avg_rate,
                     '${dayBeforeYesterday}',
                     ${cycle}
                FROM (SELECT t1.kol_id,
                             t3.cert_1,
                             if(total_val = 0, 0, round(sum_like / total_val, 5)) like_rate,
                             if(total_val = 0, 0, round(sum_fans / total_val, 5)) fans_rate,
                             if(total_val = 0, 0, round(cover_fans_num / total_val, 5)) cover_rate,
                             if(sum(total_val) over(partition by t3.cert_1) = 0,
                                0,
                                round(sum(sum_like)
                                      over(partition by t3.cert_1) / sum(total_val)
                                      over(partition by t3.cert_1),
                                      5)) avg_like_rage,
                             if(sum(total_val) over(partition by t3.cert_1) = 0,
                                0,
                                round(sum(sum_fans)
                                      over(partition by t3.cert_1) / sum(total_val)
                                      over(partition by t3.cert_1),
                                      5)) avg_fans_rate,
                             if(sum(total_val) over(partition by t3.cert_1) = 0,
                                0,
                                round(sum(cover_fans_num)
                                      over(partition by t3.cert_1) / sum(total_val)
                                      over(partition by t3.cert_1),
                                      5)) avg_cover_rate
                        FROM (SELECT kol_id,
                                     cover_fans_num,
                                     sum(new_like_count) sum_like,
                                     sum(comment_fans_num) sum_fans,
                                     cover_fans_num + sum(new_like_count) +
                                     sum(comment_fans_num) total_val
                                FROM bigdata.douyin_advert_content_calc_data
                               WHERE dt = '${dayBeforeYesterday}'
                                 AND cycle = ${cycle}
                               GROUP BY kol_id, cover_fans_num) t1
                        LEFT JOIN (select kol_id,interest_id,cert_label_id from bigdata.advert_douyin_kol_mark_daily_snapshot where dt='${yesterday}') t2
                          ON t1.kol_id = t2.kol_id
                        LEFT JOIN bigdata.advert_cert t3
                          ON t2.cert_label_id = t3.id) t lateral VIEW explode(array(1, 2, 3)) addtable AS type;"
    executeHiveCommand "${COMMON_VAR}${es_sql}"
