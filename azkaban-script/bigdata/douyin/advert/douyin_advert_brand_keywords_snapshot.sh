#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
#分别和三种文字来源于品牌的表匹配
today=$1
yesterday=`date -d "-1 day $today" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive_sql1="INSERT INTO TABLE bigdata.douyin_advert_brand_keywords_snapshot PARTITION(dt='${yesterday}')
select b.brand_id,b.brand_1,b.brand_2,b.brand_3,b.brand_4,b.brand_5,b.brand_6,b.brand_7,b.brand_8,b.brand_9,b.brand_10,
b.category_id,b.pl_1,b.pl_2,b.pl_3,b.pl_4,b.pl_5,b.pl_6,b.pl_7,b.pl_8,b.pl_9,b.pl_10,a.keyword_id,a.keyword_name,'3' as type,
a.short_video_id as resource_id,a.short_video_id,'' as comment_id,a.author_id,a.video_create_time as resource_time,a.video_create_time,
'' as comment_create_time,a.keyword_num
from
   (select short_video_id,video_create_time,author_id,split(keyword,'-')[0] as keyword_name,sha1(split(keyword,'-')[0]) as keyword_id,split(keyword,'-')[1] as keyword_num
    from
        (select short_video_id,video_create_time,author_id,content_brand_keywords
         from bigdata.douyin_advert_new_video_data
         where dt = '${yesterday}') t1
    LATERAL VIEW explode(split(content_brand_keywords,',')) t2 as keyword) a
left join
   (select keyword_id,keyword_name,brand_id,brand_1,brand_2,brand_3,brand_4,brand_5,brand_6,brand_7,brand_8,brand_9,brand_10,category_id,pl_1,pl_2,pl_3,pl_4,pl_5,pl_6,pl_7,pl_8,pl_9,pl_10
    from bigdata.douyin_advert_brand_keywords
    where dt = '${yesterday}') b
on a.keyword_id = b.keyword_id;"

executeHiveCommand "${COMMON_VAR}${hive_sql1}"

hive_sql2="INSERT INTO TABLE bigdata.douyin_advert_brand_keywords_snapshot PARTITION(dt='${yesterday}')
select b.brand_id,b.brand_1,b.brand_2,b.brand_3,b.brand_4,b.brand_5,b.brand_6,b.brand_7,b.brand_8,b.brand_9,b.brand_10,
b.category_id,b.pl_1,b.pl_2,b.pl_3,b.pl_4,b.pl_5,b.pl_6,b.pl_7,b.pl_8,b.pl_9,b.pl_10,a.keyword_id,a.keyword_name,'1' as type,
a.short_video_id as resource_id,a.short_video_id,'' as comment_id,a.author_id,a.video_create_time as resource_time,a.video_create_time,
'' as comment_create_time,a.keyword_num
from
    (select short_video_id,video_create_time,author_id,split(keyword,'-')[0] as keyword_name,sha1(split(keyword,'-')[0]) as keyword_id,split(keyword,'-')[1] as keyword_num
     from
         (select short_video_id,video_create_time,author_id,description_brand_keywords
          from bigdata.douyin_advert_new_video_data
          where dt = '${yesterday}') t1
LATERAL VIEW explode(split(description_brand_keywords,',')) t2 as keyword) a
left join
(select keyword_id,keyword_name,brand_id,brand_1,brand_2,brand_3,brand_4,brand_5,brand_6,brand_7,brand_8,
brand_9,brand_10,category_id,pl_1,pl_2,pl_3,pl_4,pl_5,pl_6,pl_7,pl_8,pl_9,pl_10 from
bigdata.douyin_advert_brand_keywords where dt = '${yesterday}') b
on a.keyword_id = b.keyword_id;"

executeHiveCommand "${COMMON_VAR}${hive_sql2}"

hive_sql3="INSERT INTO TABLE bigdata.douyin_advert_brand_keywords_snapshot PARTITION(dt='${yesterday}')
select t1.brand_id,t1.brand_1,t1.brand_2,t1.brand_3,t1.brand_4,t1.brand_5,t1.brand_6,t1.brand_7,t1.brand_8,t1.brand_9,t1.brand_10,
t1.category_id,t1.pl_1,t1.pl_2,t1.pl_3,t1.pl_4,t1.pl_5,t1.pl_6,t1.pl_7,t1.pl_8,t1.pl_9,t1.pl_10,t1.keyword_id,t1.keyword_name,t1.type,
t1.resource_id,t1.short_video_id,t1.comment_id,t2.author_id,t1.resource_time,t1.video_create_time,t1.created_time,t1.keyword_num from
(select b.brand_id,b.brand_1,b.brand_2,b.brand_3,b.brand_4,b.brand_5,b.brand_6,b.brand_7,b.brand_8,b.brand_9,b.brand_10,
b.category_id,b.pl_1,b.pl_2,b.pl_3,b.pl_4,b.pl_5,b.pl_6,b.pl_7,b.pl_8,b.pl_9,b.pl_10,a.keyword_id,a.keyword_name,'2' as type,
a.comment_id as resource_id,a.short_video_id,a.comment_id,a.created_time as resource_time,'' as video_create_time,
a.created_time,a.keyword_num from
  (select comment_id,created_time,short_video_id,split(keyword,'-')[0] as keyword_name,sha1(split(keyword,'-')[0]) as keyword_id,split(keyword,'-')[1] as keyword_num
   from
        (select comment_id,created_time,short_video_id,brand_keywords
         from bigdata.douyin_advert_new_comment_data
         where dt = '${yesterday}') t1
         LATERAL VIEW explode(split(brand_keywords,',')) t2 as keyword) a
  left join
        (select keyword_id,keyword_name,brand_id,brand_1,brand_2,brand_3,brand_4,brand_5,brand_6,brand_7,brand_8,brand_9,brand_10,category_id,pl_1,pl_2,pl_3,pl_4,pl_5,pl_6,pl_7,pl_8,pl_9,pl_10
         from bigdata.douyin_advert_brand_keywords
         where dt = '${yesterday}') b
   on a.keyword_id = b.keyword_id) t1
inner join
  (select short_video_id,author_id
  from bigdata.douyin_advert_content_snapshot
  where dt = '${yesterday}') t2
on t1.short_video_id = t2.short_video_id;"

executeHiveCommand "${COMMON_VAR}${hive_sql3}"