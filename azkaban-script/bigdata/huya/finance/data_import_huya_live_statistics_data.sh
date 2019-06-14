#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
stat_date=`date -d "$date" +%Y%m%d`
stat_month=`date -d "${date}" +%Y%m`
day7=`date -d "-6 day ${date}" +%Y-%m-%d`
day30=`date -d "-29 day ${date}" +%Y-%m-%d`

##############################
########计算平台主播数#########
##############################

hive_sql1="
select
      '${date}' as stat_date,
      count(distinct user_id) as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_all_user_info_snapshot
where dt = '${date}'
"

###################################
########计算直播流水、开播数#########
###################################

hive_sql2="
select 
      '${date}' as stat_date,
      0 as user_count,
      sum(income) as total_money,
      sum(live_num) as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_active_user_info_snapshot
where dt = '${date}'
"

#######################################
########计算平台活跃主播数(当天)#########
#######################################

hive_sql3="
select
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      count(distinct user_id) as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_active_user_info_snapshot
where dt = '${date}'
"

########################################
########计算平台活跃主播数(近7天)#########
########################################

hive_sql10="
select
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      count(distinct user_id) as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_active_user_info_snapshot
where dt >= '${day7}' and dt <= '${date}'
"

########################################
########计算平台活跃主播数(近30天)#########
########################################

hive_sql11="
select
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      count(distinct user_id) as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_active_user_info_snapshot
where dt >= '${day30}' and dt <= '${date}'
"

##################################
########计算平台新增主播数#########
##################################

hive_sql4="
select
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      count(distinct user_id) as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_new_user_info_snapshot
where dt = '${date}'
"

###########################################
########计算平台活跃打赏用户数(当天)#########
###########################################

hive_sql5="
select
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      count(audience_id) as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_active_pay_user_snapshot
where dt = '${date}'
"

###########################################
########计算平台活跃打赏用户数(近7天)#########
###########################################

hive_sql12="
select
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      count(audience_id) as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_active_pay_user_snapshot
where dt >= '${day7}' and dt <= '${date}'
"

###########################################
########计算平台活跃打赏用户数(近30天)#########
###########################################

hive_sql13="
select
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      count(audience_id) as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_active_pay_user_snapshot
where dt >= '${day30}' and dt <= '${date}'
"

#####################################
########计算平台新增打赏用户数#########
#####################################

hive_sql6="
select
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      count(audience_id) as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_new_pay_user_snapshot
where dt = '${date}'
"

#########################################
########计算直播间开通或续费贵族数#########
#########################################

hive_sql7="
select 
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      count(distinct audience_id) as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_danmu_origin_orc 
where 
     dt = '${date}' 
     and danmu_type = 2 
     and is_this = 'true'
     and audience_id != ''
     and audience_id != 'null'
     and audience_id is not null
"

##############################
########计算新增贵族数#########
##############################

hive_sql8="
select 
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      count(audience_id) as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_new_nobility_user_snapshot 
where 
     dt = '${date}'
"

#####################################
########计算付费用户数（当天）#########
#####################################

hive_sql9="
select 
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      count(distinct audience_id) as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_danmu_origin_orc 
where 
     dt = '${date}' 
     and danmu_type in ('0','2')
     and audience_id != ''
     and audience_id != 'null'
     and audience_id is not null
"

######################################
########计算付费用户数（近7天）#########
######################################

hive_sql14="
select 
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      count(distinct audience_id) as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_danmu_origin_orc 
where 
     dt >= '${day7}' 
     and dt <= '${date}' 
     and danmu_type in ('0','2')
     and audience_id != ''
     and audience_id != 'null'
     and audience_id is not null
"

#######################################
########计算付费用户数（近30天）#########
#######################################

hive_sql15="
select 
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      count(distinct audience_id) as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from bigdata.huya_live_danmu_origin_orc 
where 
     dt >= '${day30}' 
     and dt <= '${date}' 
     and danmu_type in ('0','2')
     and audience_id != ''
     and audience_id != 'null'
     and audience_id is not null
"

#####################################
########计算平台最大在线观众数#########
#####################################

hive_sql16="
select 
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      max(online_num) as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from(
    select data_generate_time,sum(online_num) online_num
    from bigdata.huya_live_user_info_origin_orc
    where dt='${date}'
    group by data_generate_time
) a
"

#####################################
########计算平台最大在线观众数#########
#####################################

hive_sql16="
select 
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      max(online_num) as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from(
    select data_generate_time,sum(online_num) online_num
    from bigdata.huya_live_user_info_origin_orc
    where dt='${date}'
    group by data_generate_time
) a
"

#####################################
########计算平台开通贵族流水###########
#####################################

hive_sql19="
select
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      sum(cast(open_noble_count*price as double)) as open_noble_money,
      0 as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from
(
    select
          noble_classify,
          sum(open_noble_count) as open_noble_count,
          case when noble_classify = '帝皇' then 150000/30
               when noble_classify = '君王' then 15000/30
               when noble_classify = '公爵' then 5000/30
               when noble_classify = '领主' then 1000/30
               when noble_classify = '骑士' then 300/30
               when noble_classify = '剑士' then 50/30
          else 0 end price
    from bigdata.huya_live_open_noble_user_snapshot
    where end_time >= '${date}'
         and start_time <= '${date}'
    group by noble_classify
) t
"

#####################################
########计算平台续费贵族流水###########
#####################################

hive_sql20="
select
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      sum(cast(renew_noble_count*price as double)) as renew_noble_money,
      0 as renew_guard_count,
      0 as open_guard_count
from
(
    select
          noble_classify,
          count(distinct audience_id) as renew_noble_count,
          case when noble_classify = '帝皇' then 30000/30
               when noble_classify = '君王' then 3000/30
               when noble_classify = '公爵' then 1200/30
               when noble_classify = '领主' then 200/30
               when noble_classify = '骑士' then 50/30
               when noble_classify = '剑士' then 10/30
          else 0 end price
    from bigdata.huya_live_renew_noble_user_snapshot
    where dt = '${date}'
         and end_time >= '${date}'
    group by noble_classify
) t
"

##############################################################################################
########从当日抓取的弹幕中找存在24小时直播的房间作为非直播间用户续费贵族的依据（不需要去重）#########
##############################################################################################

hive_sql21="
select
      t1.live_user_id,
      t2.data_generate_time
from
(
    select
          a.live_user_id,
          b.user_id
    from
    (   
        select
              distinct live_user_id
        from bigdata.huya_live_danmu_origin_orc
        where dt = '${date}'
              and live_user_id is not null
              and live_user_id != ''
              and (danmu_type = 5 or danmu_type = 6)
    ) a
    join
    (
        select
              user_id,
              target_id
        from
        (
            select
                  user_id,
                  target_id,
                  row_number() over (partition by user_id order by data_generate_time desc) as order_num
            from bigdata.huya_live_id_list_origin_orc
            where dt = '${date}'
                  and user_id is not null
                  and user_id != ''
                  and target_id is not null
                  and target_id != ''
        ) t
        where t.order_num = 1
    ) b
    on a.live_user_id = b.target_id
) t1
join
(  
    select
          user_id,
          data_generate_time
    from
    (
        select
              user_id,
              data_generate_time,
              row_number() over (partition by user_id order by data_generate_time desc) as order_num
        from bigdata.huya_live_user_info_origin_orc
        where dt = '${date}' 
              and live_duration >= 1440
              and user_id is not null
              and user_id != ''
    ) t
    where t.order_num = 1 
) t2
on t1.user_id = t2.user_id
order by t2.data_generate_time desc
limit 1
;"

result=`hive -e "${hive_sql21}"`
echo $result

data=(${result//\t / })

echo ${data[0]}

#############################################
########计算平台开通和续费守护的人数###########
############################################

hive_sql22="
select
      '${date}' as stat_date,
      0 as user_count,
      0 as total_money,
      0 as total_live_num,
      0 as active_count,
      0 as active_count7,
      0 as active_count30,
      0 as new_count,
      0 as active_gift_pay_user,
      0 as active_gift_pay_user7,
      0 as active_gift_pay_user30,
      0 as new_pay_user,
      0 as open_or_renew_nobility,
      0 as new_nobility,
      0 as pay_user_count,
      0 as pay_user_count7,
      0 as pay_user_count30,
      0 as online_num,
      0 as open_noble_money,
      0 as renew_noble_money,
      count(distinct renew_guard) as renew_guard_count,
      count(distinct open_guard) as open_guard_count
from
(
    select
          case when danmu_type = 5 then audience_id end renew_guard,
          case when danmu_type = 6 then audience_id end open_guard
    from bigdata.huya_live_danmu_origin_orc 
            where dt = '${date}'
                 and live_user_id = '${data[0]}'
                 and audience_id is not null
                 and audience_id != ''
                 and (danmu_type = 5 or danmu_type = 6)
) t
"

#######################################
########合并计算结果并导入结果表#########
#######################################

hive_sql17="
insert into bigdata.huya_live_statistics_data partition(dt='${date}')
select 
      cast(MAX(user_count) as int) as platform_user_count,
      cast(MAX(total_money) as double) as live_money,
      cast(MAX(total_live_num) as int) as total_live_num,
      cast(MAX(active_count) as int) as active_live_user,
      cast(MAX(active_count7) as int) as seven_active_live_user,
      cast(MAX(active_count30) as int) as thirty_active_live_user,
      cast(MAX(new_count) as int) as new_live_user,
      cast(MAX(active_gift_pay_user) as int) as active_gift_pay_user,
      cast(MAX(active_gift_pay_user7) as int) as seven_active_gift_pay_user,
      cast(MAX(active_gift_pay_user30) as int) as thirty_active_gift_pay_user,
      cast(MAX(new_pay_user) as int) as new_pay_user,
      cast(MAX(open_or_renew_nobility) as int) as open_or_renew_nobility,
      cast(MAX(new_nobility) as int) as new_nobility,
      cast(MAX(pay_user_count) as int) as active_pay_user,
      cast(MAX(pay_user_count7) as int) as seven_active_pay_user,
      cast(MAX(pay_user_count30) as int) as thirty_active_pay_user,
      cast(MAX(online_num) as int) as max_online_num,
      cast(MAX(open_noble_money) as double) as open_noble_money,
      cast(MAX(renew_noble_money) as double) as renew_noble_money,
      cast(MAX(renew_guard_count) as int) as renew_guard_count,
      cast(MAX(open_guard_count) as int) as open_guard_count
from
(
    ${hive_sql1}
    UNION ALL
    ${hive_sql2}
    UNION ALL
    ${hive_sql3}
    UNION ALL
    ${hive_sql10}
    UNION ALL
    ${hive_sql11}
    UNION ALL
    ${hive_sql4}
    UNION ALL
    ${hive_sql5}
    UNION ALL
    ${hive_sql12}
    UNION ALL
    ${hive_sql13}
    UNION ALL
    ${hive_sql6}
    UNION ALL
    ${hive_sql7}
    UNION ALL
    ${hive_sql8}
    UNION ALL
    ${hive_sql9}
    UNION ALL
    ${hive_sql14}
    UNION ALL
    ${hive_sql15}
    UNION ALL
    ${hive_sql16}
    UNION ALL
    ${hive_sql19}
    UNION ALL
    ${hive_sql20}
    UNION ALL
    ${hive_sql22}
) t
group by t.stat_date
;"

executeHiveCommand "${hive_sql17}"

################################
########导出结果数据到ES#########
################################

hive_sql18="
add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
insert into bigdata.huya_live_statistics_es_data
select
      concat('${stat_date}','_','live_statistics'),
      '${stat_month}',
      unix_timestamp(dt, 'yyyy-MM-dd')*1000,
      'huya',
      'live_statistics',
      platform_user_count,
      live_money,
      total_live_num,
      active_live_user,
      seven_active_live_user,
      thirty_active_live_user,
      new_live_user,
      active_gift_pay_user,
      seven_active_gift_pay_user,
      thirty_active_gift_pay_user,
      new_pay_user,
      open_or_renew_nobility,
      new_nobility,
      active_pay_user,
      seven_active_pay_user,
      thirty_active_pay_user,
      max_online_num,
      open_noble_money,
      renew_noble_money,
      renew_guard_count,
      open_guard_count
from bigdata.huya_live_statistics_data
where dt = '${date}'
"

executeHiveCommand "${hive_sql18}"