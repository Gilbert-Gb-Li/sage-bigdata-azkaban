#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖于douyin_advert_content_calc_data.sh
# 内容评论表

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
insert into bigdata.advert_douyin_content_coment_es partition(dt='${dayBeforeYesterday}')
SELECT '${stat_date}' AS stat_month,
       unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd')*1000,
       t2.comment_id,
       t1.kol_id,
       'douyin' platform,
       '0' platform_kol_id,
       t1.content_id,
       t1.cover_url,
       t1.description,
       t1.source_time,
       t1.like_count,
       t1.comments_count,
       t1.share_count,
       t1.interact_num,
       t1.challenge_ids,
       t3.nick_name,
       t2. COMMENT,
       t2.created_time created_time,
       '0' comment_prefer,
       t2.like_count,
       sk.content_brands,
       sk.content_categorys,
       '${dayBeforeYesterday}'
  FROM (SELECT kol_id,
               content_id,
               cover_url,
               source_time,
               like_count,
               comments_count,
               share_count,
               interact_num,
               challenge_ids,
               description
          FROM bigdata.douyin_advert_content_calc_data
         WHERE dt = '${dayBeforeYesterday}'
           AND cycle = 1
           AND to_date(source_time) > '${date_reduce_60}') t1
 INNER JOIN (SELECT comment_id,
                    user_id,
                    short_video_id content_id,
                    created_time,
                    like_count,
                    COMMENT
               FROM bigdata.douyin_video_comment_daily_snapshot
              WHERE dt = '${dayBeforeYesterday}'
                AND to_date(created_time) > '${date_reduce_60}') t2
    ON t1.content_id = t2.content_id
  LEFT JOIN (SELECT content_id,
                    split(bigdata.string_distinct(concat_ws(',',
                                                            collect_set(sk2.path))),
                          ',') content_brands,
                    split(bigdata.string_distinct(concat_ws(',',
                                                            collect_set(sk3.path))),
                          ',') content_categorys
               FROM (select * from bigdata.douyin_advert_source_keywords
                   WHERE dt <= '${dayBeforeYesterday}'
                    AND dt > '${date_reduce_60}'
                    AND to_date(source_date) > '${date_reduce_60}') sk1
              INNER JOIN (select * from bigdata.advert_brand where dt='${dayBeforeYesterday}') sk2
                 ON sk1.brand_id = sk2.id
              left JOIN (select * from bigdata.advert_category where dt='${dayBeforeYesterday}') sk3
                 ON sk1.category_id = sk3.id
              GROUP BY sk1.content_id) sk
    ON t2.content_id = sk.content_id
  inner JOIN (SELECT user_id, nick_name
               FROM bigdata.douyin_advert_kol_snapshot
              WHERE dt = '${dayBeforeYesterday}') t3
    ON t2.user_id = t3.user_id;"


echo "++++++++++++++++++++++++++++++++导出 内容评论表 ES++++++++++++++++++++++++++++++++++++++"
executeHiveCommand "${COMMON_VAR}${es_sql}"

