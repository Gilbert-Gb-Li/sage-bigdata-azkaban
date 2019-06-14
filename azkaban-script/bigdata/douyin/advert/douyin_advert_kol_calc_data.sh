#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_content_calc_data.sh 先执行

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_1=`date -d "-1 day $dayBeforeYesterday" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`

# 增加参数粉丝数大于等于8000的kol为有效KOL
KOL_FANS_NUM=8000

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

echo "++++++++++++++++++++++++++++++++计算 KOL统计信息 统计周期 中间表++++++++++++++++++++++++++++++++++++++"
for cycle in 1 7 30 60
do
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    hive_sql1="${hive_sql1}WITH t_orgin AS
 (SELECT t2.record_time,
         t2.user_id kol_id,
         t3.interact_avg,
         ${cycle} cycle,
         t2.cover_fans_num,
         t3.interact_fans_num,
         IF(t2.cover_fans_num = 0,
            0,
            round(t3.interact_fans_num / t2.cover_fans_num, 5)) fans_interact_rate,
         t3.content_num,
         t3.content_interact_num,
         t3.content_rate_avg,
         t2.fans_num,
         t3.interact_min,
         t3.interact_max
    FROM (SELECT user_id,
                 max(case
                       when dt = '${dayBeforeYesterday}' then
                        follower_count
                       else
                        0
                     end) fans_num,
                 max(follower_count) cover_fans_num,
                 max(record_time) record_time
            FROM bigdata.douyin_advert_kol_snapshot
           WHERE dt <= '${dayBeforeYesterday}'
             AND dt > '${date_reduce}'
             and follower_count >= ${KOL_FANS_NUM}
           GROUP BY user_id) t2
    LEFT JOIN (SELECT kol_id,
                     count(1) content_num,
                     sum(new_interact_num) content_interact_num,
                     round(avg(interact_fans_num), 5) interact_fans_num,
                     round(avg(new_interact_num), 5) interact_avg,
                     round(avg(fans_interact_rate), 5) content_rate_avg,
                     min(round(avg(new_interact_num), 5)) over(partition by 1) interact_min,
                     max(round(avg(new_interact_num), 5)) over(partition by 1) interact_max
                FROM bigdata.douyin_advert_content_calc_data
               WHERE dt = '${dayBeforeYesterday}'
                 AND cycle = ${cycle}
                 AND to_date(source_time) > '${date_reduce}'
               GROUP BY kol_id) t3
      ON t2.user_id = t3.kol_id)
INSERT overwrite table bigdata.douyin_advert_kol_calc_data PARTITION
 (dt = '${dayBeforeYesterday}', cycle = ${cycle})
SELECT t.record_time,
       t.kol_id,
       t.interact_avg,
       t.interact_min,
       t.interact_max,
       t.impact_index,
       t.cover_fans_num,
       t.interact_fans_num,
       t.fans_interact_rate,
       t.content_num,
       t.content_interact_num,
       t.content_rate_avg,
       t.impact_index - coalesce(tt.impact_index, 0) impact_incr,
       t.fans_num,
       if(tt.fans_add_list is null,
          t.fans_num,
          bigdata.cover(tt.fans_add_list, t.fans_num - tt.fans_num)) fans_add_list,
       if(tt.fans_num is null or tt.fans_num = 0 or
          tt.content_interact_num is null or tt.content_interact_num = 0,
          0,
          case
            when (t.fans_num - tt.fans_num) > 0 and
                 (t.content_interact_num - tt.content_interact_num) > 0 and
                 ((t.fans_num - tt.fans_num) / tt.fans_num) /
                 ((t.content_interact_num - tt.content_interact_num) /
                 tt.content_interact_num) >= 3 or
                 (t.fans_num - tt.fans_num) > 0 and
                 (t.content_interact_num - tt.content_interact_num) < 0 then
             (t.fans_num - tt.fans_num) -
             (bigdata.arraySum(bigdata.cover(tt.fans_add_list,
                                             t.fans_num - tt.fans_num)) /
             size(split(bigdata.cover(tt.fans_add_list,
                                       t.fans_num - tt.fans_num),
                         ',')) +
             round(bigdata.standardDeviation(bigdata.cover(tt.fans_add_list,
                                                            t.fans_num -
                                                            tt.fans_num))))
            else
             0
          end) false_fans_num
  FROM (SELECT to1.record_time,
               to1.kol_id,
               to1.interact_avg,
               to1.interact_min,
               to1.interact_max,
               round(1 + (100 - 1) / (to1.interact_max - to1.interact_min) *
                     (to1.interact_avg - to1.interact_min)) impact_index,
               to1.cover_fans_num,
               to1.interact_fans_num,
               to1.fans_interact_rate,
               to1.content_num,
               to1.content_interact_num,
               to1.content_rate_avg,
               to1.fans_num
          FROM t_orgin to1) t
  LEFT JOIN (select kol_id,
                    max(case dt
                          when '${date_reduce_1}' then
                           fans_num
                          else
                           0
                        end) fans_num,
                    max(case dt
                          when '${date_reduce_1}' then
                           content_interact_num
                          else
                           0
                        end) content_interact_num,
                    max(case dt
                          when '${date_reduce_1}' then
                           0
                          else
                           impact_index
                        end) impact_index,
                    max(case dt
                          when '${date_reduce_1}' then
                           fans_add_list
                          else
                           null
                        end) fans_add_list
               from bigdata.douyin_advert_kol_calc_data
              where dt in ('${date_reduce_1}', '${date_reduce}')
                AND cycle = ${cycle}
              group by kol_id) tt
    ON t.kol_id = tt.kol_id;
"
done
executeHiveCommand "${COMMON_VAR}${hive_sql1}"