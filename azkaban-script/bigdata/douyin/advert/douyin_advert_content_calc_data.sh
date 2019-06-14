#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

echo "++++++++++++++++++++++++++++++++计算 内容统计信息 统计周期 中间表++++++++++++++++++++++++++++++++++++++"
for cycle in 1 7 30 60
do
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    hive_sql1="${hive_sql1}INSERT overwrite table bigdata.douyin_advert_content_calc_data PARTITION
  (dt = '${dayBeforeYesterday}', cycle = ${cycle})
  SELECT record_time,
         t.kol_id,
         t.content_id,
         description,
         cover_url,
         video_url,
         avatar_url,
         challenge_ids,
         like_count,
         comments_count,
         share_count,
         like_count + comments_count + share_count interact_num,
         new_like_count,
         new_comments_count,
         new_share_count,
         new_like_count + new_comments_count + new_share_count new_interact_num,
         k.cover_fans_num,
         new_like_count + COALESCE(c.interact_fans_num, 0) interact_fans_num,
         round((new_like_count + COALESCE(c.interact_fans_num, 0)) /
               k.cover_fans_num,
               5) fans_interact_rate,
         source_time,
         COALESCE(c.interact_fans_num, 0) comment_fans_num
    FROM (SELECT t1.record_times [ 0 ] record_time,
                 t1.kol_id,
                 t1.short_video_id content_id,
                 t1.description,
                 t1.cover_url,
                 t1.video_url,
                 t1.avatar_url,
                 t1.challenge_ids,
                 t1.like_count,
                 t1.comments_count,
                 t1.share_count,
                 t1.source_time,
                 CASE
                   WHEN t3.like_count IS NULL OR t3.like_count = '' OR
                        t3.like_count = -1 THEN
                    t1.like_count
                   ELSE
                    t1.like_count - t3.like_count
                 END new_like_count,
                 CASE
                   WHEN t3.comments_count IS NULL OR t3.comments_count = '' OR
                        t3.comments_count = -1 THEN
                    t1.comments_count
                   ELSE
                    t1.comments_count - t3.comments_count
                 END new_comments_count,
                 CASE
                   WHEN t3.share_count IS NULL OR t3.share_count = '' OR
                        t3.share_count = -1 THEN
                    t1.share_count
                   ELSE
                    t1.share_count - t3.share_count
                 END new_share_count
            FROM (SELECT short_video_id,
                         video_create_time source_time,
                         description description,
                         CASE
                           WHEN size(cover_url_list) <= 0 THEN
                            NULL
                           ELSE
                            cover_url_list [ 0 ]
                         END cover_url,
                         CASE
                           WHEN size(play_url_list) <= 0 THEN
                            NULL
                           ELSE
                            play_url_list [ 0 ]
                         END video_url,
                         CASE
                           WHEN size(avatar_url) <= 0 THEN
                            NULL
                           ELSE
                            avatar_url [ 0 ]
                         END avatar_url,
                         like_count,
                         comments_count,
                         share_count,
                         author_id kol_id,
                         collect_set(challenge_id) challenge_ids,
                         collect_set(record_time) record_times
                    FROM bigdata.douyin_advert_content_snapshot
                   WHERE dt = '${dayBeforeYesterday}'
                     AND video_create_time > '${date_reduce_60}'
                   group by short_video_id,
                            video_create_time,
                            description,
                            cover_url_list,
                            play_url_list,
                            avatar_url,
                            like_count,
                            comments_count,
                            share_count,
                            author_id) t1
            LEFT JOIN (SELECT short_video_id,
                             author_id,
                             like_count,
                             comments_count,
                             share_count
                        FROM bigdata.douyin_advert_content_snapshot
                       WHERE dt = '${date_reduce}'
                         AND video_create_time > '${date_reduce_60}'
                       group by short_video_id,
                                author_id,
                                like_count,
                                comments_count,
                                share_count) t3
              ON t1.short_video_id = t3.short_video_id) t
    LEFT JOIN (SELECT user_id kol_id, max(follower_count) cover_fans_num
                 FROM bigdata.douyin_advert_kol_snapshot
                WHERE dt <= '${dayBeforeYesterday}'
                  AND dt > '${date_reduce}'
                GROUP BY user_id) k
      ON t.kol_id = k.kol_id
    LEFT JOIN (SELECT short_video_id content_id,
                      count(DISTINCT user_id) interact_fans_num
                 FROM bigdata.douyin_video_comment_daily_snapshot
                WHERE dt = '${dayBeforeYesterday}'
                  AND created_time > '${date_reduce}'
                GROUP BY short_video_id) c
      ON t.content_id = c.content_id;
"

done
executeHiveCommand "${COMMON_VAR}${hive_sql1}"