#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1

dayBeforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
maxPartitionFan=${dayBeforeYesterday}
# 计算时间是T+2
# 生成douyin_advert_fans_data_snapshot

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

RECENT_DAY_ID1=$(hive -e "show partitions bigdata.douyin_advert_fans_data_snapshot;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -ur|head -n 1)
y1=`date -d "$dayBeforeYesterday" +%s`
r1=`date -d "$RECENT_DAY_ID1" +%s`
if [[ ${y1} -gt ${r1} ]] ;then
    maxPartitionFan=${RECENT_DAY_ID1}
fi


echo "++++++++++++++++++++++++++++++++生成douyin_advert_fans_data_snapshot表++++++++++++++++++++++++++++++++++++++"
datahql="insert overwrite table bigdata.douyin_advert_fans_data_snapshot partition
  (dt = '${yesterday}')
    select kol_id,
         fans_id,
         nick_name,
         record_time,
         app_version,
         app_package_name,
         sex,
         age,
         province,
         city
    from (select kol_id,
                 fans_id,
                 nick_name,
                 record_time,
                 app_version,
                 app_package_name,
                 sex,
                 age,
                 province,
                 city,
                 row_number() over(partition by kol_id, fans_id order by record_time desc) order_seq
            from (select t.user_id kol_id,
                         f.fans_id,
                         u.nick_name,
                         f.record_time,
                         f.app_version,
                         f.app_package_name,
                         u.sex,
                         u.age,
                         u.province,
                         u.city
                    from (select user_id
                            from bigdata.douyin_advert_kol_data_snapshot
                           where dt = '${yesterday}') t
                   inner join (select case object_type when 1 then from_user when 2 then  user_id end  kol_id,
                                   case object_type when 1 then user_id when 2 then  from_user end  fans_id,
                                   record_time,
                                   app_version,
                                   app_package_name
                              from bigdata.douyin_attention_follower_data_origin_orc
                             where dt = '${yesterday}' and object_type in (1,2)) f
                      on t.user_id = f.kol_id
                   inner join (select user_id, sex, age, province, city, nick_name
                                from bigdata.douyin_user_daily_snapshot
                               where dt = '${yesterday}') u
                      on f.fans_id = u.user_id
                  union
                  select kol_id,
                         fans_id,
                         nick_name,
                         record_time,
                         app_version,
                         app_package_name,
                         sex,
                         age,
                         province,
                         city
                    from bigdata.douyin_advert_fans_data_snapshot
                   where dt = '${maxPartitionFan}') t) p
   where order_seq = 1;
"
executeHiveCommand "${COMMON_VAR}${datahql}"