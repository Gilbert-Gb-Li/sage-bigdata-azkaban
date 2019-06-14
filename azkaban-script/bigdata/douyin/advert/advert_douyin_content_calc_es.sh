#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_kol_property_brand_industry.sh,douyin_advert_content_calc_data.sh
# 导出到ES 内容统计信息

yesterday=$1
dayBeforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

for cycle in 1 7 30 60
do
    echo "++++++++++++++++++++++++++++++++导出内容统计信息列表到ES 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
        insert into bigdata.advert_douyin_content_calc_es partition(dt='${dayBeforeYesterday}',cycle=${cycle})
        SELECT '${stat_date}' AS stat_month,
               unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd')*1000,
               t.content_id,
               t.description,
               t.cover_url,
               t.video_url,
               t.avatar_url,
               case when size(t.challenge_ids)>=1 then t.challenge_ids else array() end challenge_ids,
               t.like_count,
               t.comments_count,
               t.share_count,
               t.interact_num,
               t.new_like_count,
               t.new_comments_count,
               t.new_share_count,
               t.new_interact_num,
               t.kol_id,
               'douyin' platform,
               '0' platform_kol_id,
               u.nick_name kol_name,
               bi.pls,
               bi.brands,
               ic.interest_class,
               ic.cert_label,
               u.follower_count,
               u.age,
               u.sex,
               u.province,
               u.city,
               t.source_time,
               t.cover_fans_num,
               t.interact_fans_num,
               t.fans_interact_rate,
               sk.content_brands,
               sk.content_categorys,
               '${dayBeforeYesterday}',
               ${cycle}
          FROM (SELECT content_id,
                       description,
                       cover_url,
                       video_url,
                       avatar_url,
                       challenge_ids,
                       like_count,
                       comments_count,
                       share_count,
                       interact_num,
                       new_like_count,
                       new_comments_count,
                       new_share_count,
                       new_interact_num,
                       kol_id,
                       cycle,
                       cover_fans_num,
                       interact_fans_num,
                       fans_interact_rate,
                       source_time
                  FROM bigdata.douyin_advert_content_calc_data
                 WHERE dt = '${dayBeforeYesterday}'
                   AND cycle = ${cycle}) t
          LEFT JOIN (SELECT content_id,
                            split(bigdata.string_distinct(concat_ws(',',
                                                                    collect_set(sk2.path))),
                                  ',') content_brands,
                            split(bigdata.string_distinct(concat_ws(',',
                                                                    collect_set(sk3.path))),
                                  ',') content_categorys
                       FROM bigdata.douyin_advert_source_keywords sk1
                      INNER JOIN (select * from bigdata.advert_brand where dt='${dayBeforeYesterday}') sk2
                         ON sk1.brand_id = sk2.id
                      left JOIN (select * from bigdata.advert_category where dt='${dayBeforeYesterday}') sk3
                         ON sk1.category_id = sk3.id
                      WHERE sk1.dt <= '${dayBeforeYesterday}'
                        AND sk1.dt > '${date_reduce}'
                        AND to_date(sk1.source_date) > '${date_reduce}'
                      GROUP BY sk1.content_id) sk
            ON t.content_id = sk.content_id
          LEFT JOIN bigdata.douyin_advert_kol_property_brand_industry bi
            ON t.kol_id = bi.kol_id
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
            ON t.kol_id = ic.kol_id
          inner JOIN (select * from bigdata.douyin_advert_kol_snapshot where dt = '${dayBeforeYesterday}') u
            ON t.kol_id = u.user_id;"

    executeHiveCommand "${COMMON_VAR}${es_sql}"
done