#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_challenge_daily_snapshot,douyin_advert_content_calc_data.sh,douyin_advert_kol_property_brand_industry.sh
# 导出到ES 话题统计信息

yesterday=$1
dayBeforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

for cycle in 1 7 30 60
do
    echo "++++++++++++++++++++++++++++++++导出话题统计信息到ES 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
                    insert into bigdata.advert_douyin_challenge_calc_es partition(dt='${dayBeforeYesterday}',cycle=${cycle})
                    SELECT '${stat_date}' AS stat_month,
                           unix_timestamp(t1.dt, 'yyyy-MM-dd')*1000,
                           t5.kol_id,
                           'douyin' platform,
                           '0' platform_kol_id,
                           bi.pls,
                           bi.brands,
                           ic.interest_class,
                           ic.cert_label,
                           u.follower_count,
                           u.age,
                           u.sex,
                           u.province,
                           u.city,
                           '${dayBeforeYesterday}' source_time,
                           t1.challenge_id,
                           t1.challenge_name,
                           t1.challenge_desc,
                           '' challenge_imag_url,
                           t1.challenge_author,
                           t1.challenge_play_count play_count,
                           t1.challenge_play_count - COALESCE(t2.challenge_play_count, 0) new_play_count,
                           t4.content_count,
                           t4.kol_count participant_cout,
                           t4.kol_count kol_count,
                           t4.cover_fans_num,
                           t4.interact_fans_num,
                           IF(t4.cover_fans_num = 0,
                              0,
                              round(t4.interact_fans_num / t4.cover_fans_num, 5)) fans_interact_rate,
                           t4.content_count,
                           t4.interact_sum,
                           IF(t4.content_count = 0,
                              0,
                              round(t4.interact_sum / t4.content_count, 5)) content_rate_avg,
                           t6.cover_fans_num kol_cover_fans_num,
                           t6.content_interact_num kol_content_interact_num,
                           t4.challenge_brands,
                           t4.challenge_categorys,
                           '${dayBeforeYesterday}',
                           ${cycle}
                      FROM (SELECT *
                              FROM bigdata.douyin_challenge_daily_snapshot
                             WHERE dt = '${dayBeforeYesterday}') t1
                      LEFT JOIN (SELECT challenge_id, challenge_play_count
                                   FROM bigdata.douyin_challenge_daily_snapshot
                                  WHERE dt = '${date_reduce}') t2
                        ON t1.challenge_id = t2.challenge_id
                      LEFT JOIN (SELECT challenge_id,
                                        count(DISTINCT kol_id) kol_count,
                                        sum(t3.cover_fans_num) cover_fans_num,
                                        sum(t3.interact_fans_num) interact_fans_num,
                                        count(content_id) content_count,
                                        sum(new_interact_num) interact_sum,
                                        split(bigdata.string_distinct(concat_ws(',',
                                                                                collect_set(content_brands))),
                                              ',') challenge_brands,
                                        split(bigdata.string_distinct(concat_ws(',',
                                                                                collect_set(content_categorys))),
                                              ',') challenge_categorys
                                   FROM (SELECT t31.*, sk.content_brands, sk.content_categorys
                                           FROM (SELECT kol_id,
                                                        content_id,
                                                        challenge_ids,
                                                        cover_fans_num,
                                                        interact_fans_num,
                                                        fans_interact_rate,
                                                        new_interact_num
                                                   FROM bigdata.douyin_advert_content_calc_data
                                                  WHERE dt = '${dayBeforeYesterday}'
                                                    AND size(challenge_ids) >= 1
                                                    AND cycle = ${cycle}) t31
                                           LEFT JOIN (SELECT content_id,
                                                            bigdata.string_distinct(concat_ws(',',
                                                                                              collect_set(sk2.path))) content_brands,
                                                            bigdata.string_distinct(concat_ws(',',
                                                                                              collect_set(sk3.path))) content_categorys
                                                       FROM bigdata.douyin_advert_source_keywords sk1
                                                      INNER JOIN (select * from bigdata.advert_brand where dt='${dayBeforeYesterday}') sk2
                                                         ON sk1.brand_id = sk2.id
                                                      left JOIN (select * from bigdata.advert_category where dt='${dayBeforeYesterday}') sk3
                                                         ON sk1.category_id = sk3.id
                                                      WHERE sk1.dt <= '${dayBeforeYesterday}'
                                                        AND sk1.dt > '${date_reduce}'
                                                        AND to_date(sk1.source_date) > '${date_reduce}'
                                                      GROUP BY sk1.content_id) sk
                                             ON t31.content_id = sk.content_id) t3 LATERAL VIEW explode(challenge_ids) vir_content AS challenge_id
                                  GROUP BY challenge_id) t4
                        ON t2.challenge_id = t4.challenge_id
                      LEFT JOIN (SELECT kol_id, challenge_id
                                   FROM (SELECT challenge_ids, kol_id
                                           FROM bigdata.douyin_advert_content_calc_data
                                          WHERE dt = '${dayBeforeYesterday}'
                                            AND size(challenge_ids) >= 1
                                            AND cycle = ${cycle}) t51 LATERAL VIEW explode(challenge_ids) vir_content2 AS challenge_id
                                  GROUP BY challenge_id, kol_id) t5
                        ON t2.challenge_id = t5.challenge_id
                      LEFT JOIN (SELECT *
                                   FROM bigdata.douyin_advert_kol_calc_data
                                  WHERE dt = '${dayBeforeYesterday}'
                                    AND cycle = ${cycle}) t6
                        ON t5.kol_id = t6.kol_id
                      LEFT JOIN bigdata.douyin_advert_kol_property_brand_industry bi
                        ON t5.kol_id = bi.kol_id
                       AND bi.dt = '${dayBeforeYesterday}'
                       AND bi.cycle = ${cycle}
                      LEFT JOIN (SELECT ic1.kol_id,
                                        split(ic2.path, ',') interest_class,
                                        split(ic3.path, ',') cert_label,
                                        ic3.cert_1 f_cert_id,
                                        ic4. NAME f_cert_name
                                   FROM (select kol_id,interest_id,cert_label_id
                                         from bigdata.advert_douyin_kol_mark_daily_snapshot
                                         where dt='${yesterday}') ic1,
                                        bigdata.advert_interest            ic2,
                                        bigdata.advert_cert                ic3,
                                        bigdata.advert_cert                ic4
                                  WHERE ic1.interest_id = ic2.id
                                    AND ic1.cert_label_id = ic3.id
                                    AND ic3.cert_1 = ic4.id) ic
                        ON t5.kol_id = ic.kol_id
                      inner JOIN (select * from bigdata.douyin_advert_kol_snapshot where dt = '${dayBeforeYesterday}') u
                        ON t5.kol_id = u.user_id;"

    executeHiveCommand "${COMMON_VAR}${es_sql}"
done
