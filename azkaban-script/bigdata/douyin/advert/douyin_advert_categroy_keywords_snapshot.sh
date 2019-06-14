#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

today=$1
yesterday=`date -d "-1 day $today" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive_sql1="
 INSERT INTO TABLE bigdata.douyin_advert_categroy_keywords_snapshot PARTITION(dt='${yesterday}')
 select b.category_id,b.pl_1,b.pl_2,b.pl_3,b.pl_4,b.pl_5,b.pl_6,b.pl_7,b.pl_8,b.pl_9,b.pl_10,a.keyword_id,a.keyword_name,'3' as type, a.short_video_id as resource_id
 from
      (select short_video_id,video_create_time,author_id,split(keyword,',')[0] as keyword_name,sha1(split(keyword,',')[0]) as keyword_id,split(keyword,',')[1] as keyword_num
       from
          (select short_video_id,video_create_time,author_id,content_categroy_keywords
           from bigdata.douyin_advert_new_video_data
           where dt = '${yesterday}') t1
          LATERAL VIEW explode(split(content_categroy_keywords,',')) t2 as keyword) a
left join
     (select keyword_id,keyword_name,category_id,pl_1,pl_2,pl_3,pl_4,pl_5,pl_6,pl_7,pl_8,pl_9,pl_10
     from bigdata.douyin_advert_category_keywords
     where dt = '${yesterday}') b
on a.keyword_id = b.keyword_id;"

executeHiveCommand "${COMMON_VAR}${hive_sql1}"

hive_sql2="
INSERT INTO TABLE bigdata.douyin_advert_categroy_keywords_snapshot PARTITION(dt='${yesterday}')
select b.category_id,b.pl_1,b.pl_2,b.pl_3,b.pl_4,b.pl_5,b.pl_6,b.pl_7,b.pl_8,b.pl_9,b.pl_10,a.keyword_id,a.keyword_name,'1' as type,a.short_video_id as resource_id
from
     (select short_video_id,video_create_time,author_id,split(keyword,'-')[0] as keyword_name,sha1(split(keyword,'-')[0]) as keyword_id,split(keyword,'-')[1] as keyword_num
      from
          (select short_video_id,video_create_time,author_id,description_categroy_keywords
           from bigdata.douyin_advert_new_video_data
           where dt = '${yesterday}') t1
      LATERAL VIEW explode(split(description_categroy_keywords,',')) t2 as keyword) a
left join
     (select keyword_id,keyword_name,category_id,pl_1,pl_2,pl_3,pl_4,pl_5,pl_6,pl_7,pl_8,pl_9,pl_10
     from bigdata.douyin_advert_category_keywords
     where dt = '${yesterday}') b
on a.keyword_id = b.keyword_id;"

executeHiveCommand "${COMMON_VAR}${hive_sql2}"

hive_sql3="INSERT INTO TABLE bigdata.douyin_advert_categroy_keywords_snapshot PARTITION(dt='${yesterday}')
select b.category_id,b.pl_1,b.pl_2,b.pl_3,b.pl_4,b.pl_5,b.pl_6,b.pl_7,b.pl_8,b.pl_9,b.pl_10,a.keyword_id,a.keyword_name,'2' as type,a.comment_id as resource_id
from
    (select comment_id,created_time,short_video_id,split(keyword,'-')[0] as keyword_name,sha1(split(keyword,'-')[0]) as keyword_id,split(keyword,'-')[1] as keyword_num
    from
        (select comment_id,created_time,short_video_id,categroy_keywords
        from bigdata.douyin_advert_new_comment_data
        where dt = '${yesterday}') t1
       LATERAL VIEW explode(split(categroy_keywords,',')) t2 as keyword) a
left join
    (select keyword_id,keyword_name,category_id,pl_1,pl_2,pl_3,pl_4,pl_5,pl_6,pl_7,pl_8,pl_9,pl_10
    from bigdata.douyin_advert_category_keywords
    where dt = '${yesterday}') b
on a.keyword_id = b.keyword_id;"

executeHiveCommand "${COMMON_VAR}${hive_sql3}"