#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
beforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
echo "############ 微博文章快照表  start   #########"

echo "##################删除当天快照  start  ###############"
deleteHdfsAndPartiton4Orc "sns" "tbl_ex_weibo_article_detail_snapshot" "${yesterday}"
echo "#################删除当天快照  end  ###################"

hive_sql="INSERT INTO TABLE sns.tbl_ex_weibo_article_detail_snapshot PARTITION(dt='${yesterday}')
SELECT record_time,article_id,user_id,like_count,comment_count,forward_count,read_count
FROM (
  SELECT t.*, row_number() over (partition by article_id order by record_time desc) num
  FROM (
    SELECT record_time,article_id,user_id,like_count,comment_count,forward_count,read_count
    FROM sns.tbl_ex_weibo_article_detail_snapshot WHERE dt='${beforeYesterday}'
    UNION ALL
    SELECT record_time,article_id,user_id,like_count,comments_count,forwards_count,reads_count
    FROM ias.tbl_ex_weibo_article_topic_data_origin_orc WHERE dt='${yesterday}'
  ) AS t
) AS r WHERE r.num=1"
executeHiveCommand "${hive_sql}"

echo "############ 微博文章快照表  end #########"
