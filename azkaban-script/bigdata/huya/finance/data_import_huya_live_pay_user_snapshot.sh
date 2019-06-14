#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
stat_date=`date -d "$date" +%Y%m%d`
stat_month=`date -d "${date}" +%Y%m`
########################################################
########计算付费用户付费类型、打赏对象主播、付费金额#########
########################################################

hive_sql="
insert into bigdata.huya_live_pay_user_snapshot partition(dt='${date}')
select 
      t1.audience_id,
      t1.user_name,
      t1.user_age,
      t1.user_sign,
      t1.user_image,
      t1.province,
      t1.city,
      t1.user_level,
      t1.user_love_channel,
      t1.user_subscribe_num,
      t1.user_fans_num,
      t1.favor_num,
      t1.live_id,
      t1.live_desc,
      t1.user_notice,
      t1.share_url,
      t1.pay_type,
      t2.live_id,
      t1.pay_times,
      t1.pay_month,
      t1.pay_money/1000
from
(
    select
          a.audience_id,
          b.user_name,
          b.user_age,
          b.user_sign,
          b.user_image,
          b.province,
          b.city,
          b.user_level,
          b.user_love_channel,
          b.user_subscribe_num,
          b.user_fans_num,
          b.favor_num,
          b.live_id,
          b.live_desc,
          b.user_notice,
          b.share_url,
          a.pay_type,
          a.object_room_id,
          a.pay_times,
          a.pay_month,
          a.pay_money
    from
    (
        select 
              audience_id,
              '礼物' as pay_type,
              live_user_id as object_room_id,
              count(1) as pay_times,
              0 as pay_month,
              sum(gift_real_money) as pay_money 
        from
        (
            select 
                  a.audience_id,
                  a.live_user_id,
                  b.gift_gold*a.gift_real_num as gift_real_money 
            from
            (
                select 
                      audience_id,
                      live_user_id,
                      gift_id,
                      gift_num*gift_dribble as gift_real_num 
                from
                (
                    select 
                          audience_id,
                          gift_id,
                          gift_num,
                          case when gift_dribble = 0 then 1 else gift_dribble end gift_dribble,
                          live_user_id
                    from bigdata.huya_live_danmu_gift_result_snapshot
                    where dt = '${date}'
                         and gift_md5 is null
                    UNION ALL
                    select
                          a.audience_id,
                          b.gift_id,
                          a.gift_num,
                          a.gift_dribble,
                          a.live_user_id
                    from
                    (
                        select 
                              audience_id,
                              gift_md5,
                              gift_num,
                              case when gift_dribble = 0 then 1 else gift_dribble end gift_dribble,
                              live_user_id
                        from bigdata.huya_live_danmu_gift_result_snapshot
                        where dt = '${date}'
                             and gift_md5 is not null
                    ) a
                    left join
                    (
                        select
                              gift_id,
                              gift_md5
                        from bigdata.huya_live_all_gift_snapshot
                        where dt = '${date}'
                    ) b
                    on a.gift_md5 = b.gift_md5
                ) t
            ) a
            left join
            (
                select 
                      distinct gift_id,gift_gold  
                from bigdata.huya_live_all_gift_snapshot 
                where dt = '${date}'
            ) b
            on a.gift_id = b.gift_id
        ) t
        group by t.audience_id,t.live_user_id
    ) a
    left join
    (
        select 
              user_name,
              user_age,
              user_sign,
              user_image,
              province,
              city,
              user_level,
              user_love_channel,
              user_subscribe_num,
              user_fans_num,
              favor_num,
              live_id,
              live_desc,
              user_notice,
              share_url,
              target_id
        from bigdata.huya_live_all_user_info_snapshot
        where dt = '${date}'
    ) b
    on a.audience_id = b.target_id
) t1
left join
(
    select 
          live_id,
          target_id
    from bigdata.huya_live_all_user_info_snapshot
    where dt = '${date}'
) t2
on t1.object_room_id = t2.target_id
"

executeHiveCommand "${hive_sql}"

hive_sql2="
insert into bigdata.huya_live_pay_user_snapshot partition(dt='${date}')
select 
      t1.audience_id,
      t1.audience_name,
      t1.user_age,
      t1.user_sign,
      t1.user_image,
      t1.province,
      t1.city,
      t1.user_level,
      t1.user_love_channel,
      t1.user_subscribe_num,
      t1.user_fans_num,
      t1.favor_num,
      t1.live_id,
      t1.live_desc,
      t1.user_notice,
      t1.share_url,
      t1.pay_type,
      t2.live_id,
      t1.pay_times,
      t1.pay_month,
      t1.pay_money
from
(
    select
          a.audience_id,
          a.audience_name,
          b.user_age,
          b.user_sign,
          b.user_image,
          b.province,
          b.city,
          b.user_level,
          b.user_love_channel,
          b.user_subscribe_num,
          b.user_fans_num,
          b.favor_num,
          b.live_id,
          b.live_desc,
          b.user_notice,
          b.share_url,
          a.pay_type,
          a.object_room_id,
          a.pay_times,
          a.pay_month,
          a.pay_money
    from
    (
        select 
              audience_id,
              audience_name,
              noble_classify as pay_type,
              live_user_id as object_room_id,
              count(1) as pay_times,
              sum(noble_num) as pay_month,
              case when noble_classify = '帝皇' then 30000*sum(noble_num)
              when noble_classify = '君王' then 3000*sum(noble_num)
              when noble_classify = '公爵' then 1200*sum(noble_num)
              when noble_classify = '领主' then 200*sum(noble_num)
              when noble_classify = '骑士' then 50*sum(noble_num)
              when noble_classify = '剑士' then 10*sum(noble_num)
              else 0 end pay_money 
        from bigdata.huya_live_danmu_origin_orc 
        where 
             dt = '${date}' 
             and danmu_type = 2 
             and is_this = 'true' 
        group by audience_id,audience_name,live_user_id,noble_classify
    ) a
    left join
    (
        select 
              user_name,
              user_age,
              user_sign,
              user_image,
              province,
              city,
              user_level,
              user_love_channel,
              user_subscribe_num,
              user_fans_num,
              favor_num,
              live_id,
              live_desc,
              user_notice,
              share_url,
              target_id
        from bigdata.huya_live_all_user_info_snapshot
        where dt = '${date}'
    ) b
    on a.audience_id = b.target_id
) t1
left join
(
    select 
          live_id,
          target_id
    from bigdata.huya_live_all_user_info_snapshot
    where dt = '${date}'
) t2
on t1.object_room_id = t2.target_id
"

executeHiveCommand "${hive_sql2}"

##############################################################################################
########从当日抓取的弹幕中找存在24小时直播的房间作为非直播间用户续费贵族的依据（不需要去重）#########
##############################################################################################

hive_sql3="
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
              and danmu_type = 1
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

result=`hive -e "${hive_sql3}"`
echo $result

data=(${result//\t / })

echo ${data[0]}

hive_sql4="
insert into bigdata.huya_live_pay_user_snapshot partition(dt='${date}')
select
      a.audience_id,
      a.audience_name,
      b.user_age,
      b.user_sign,
      b.user_image,
      b.province,
      b.city,
      b.user_level,
      b.user_love_channel,
      b.user_subscribe_num,
      b.user_fans_num,
      b.favor_num,
      b.live_id,
      b.live_desc,
      b.user_notice,
      b.share_url,
      a.pay_type,
      a.object_room_id,
      a.pay_times,
      a.pay_month,
      a.pay_money
from
(
    select 
          audience_id,
          audience_name,
          noble_classify as pay_type,
          '-1' as object_room_id,
          count(1) as pay_times,
          sum(noble_num) as pay_month,
          case when noble_classify = '帝皇' then 30000*sum(noble_num)
          when noble_classify = '君王' then 3000*sum(noble_num)
          when noble_classify = '公爵' then 1200*sum(noble_num)
          when noble_classify = '领主' then 200*sum(noble_num)
          when noble_classify = '骑士' then 50*sum(noble_num)
          when noble_classify = '剑士' then 10*sum(noble_num)
          else 0 end pay_money 
    from bigdata.huya_live_danmu_origin_orc 
    where dt = '${date}'
         and live_user_id = '${data[0]}'
         and audience_id is not null
         and audience_id != ''
         and danmu_type = 1
    group by audience_id,audience_name,noble_classify
) a
left join
(
    select 
          user_name,
          user_age,
          user_sign,
          user_image,
          province,
          city,
          user_level,
          user_love_channel,
          user_subscribe_num,
          user_fans_num,
          favor_num,
          live_id,
          live_desc,
          user_notice,
          share_url,
          target_id
    from bigdata.huya_live_all_user_info_snapshot
    where dt = '${date}'
) b
on a.audience_id = b.target_id
"

executeHiveCommand "${hive_sql4}"

################################
########导出结果数据到ES#########
################################

hive_sql1="
add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
insert into bigdata.huya_live_pay_user_es_data
select
      concat('${stat_date}','_','live_pay_user','_',audience_id,'_',pay_type,'_',object_room_id),
      '${stat_month}',
      unix_timestamp(dt, 'yyyy-MM-dd')*1000,
      'huya',
      'live_pay_user',
      audience_id,
      audience_name,
      user_age,
      user_sign,
      user_image,
      province,
      city,
      user_level,
      user_love_channel,
      user_subscribe_num,
      user_fans_num,
      favor_num,
      live_id,
      live_desc,
      user_notice,
      share_url,
      pay_type,
      object_room_id,
      pay_times,
      pay_month,
      pay_money
from bigdata.huya_live_pay_user_snapshot
where dt = '${date}'
     and audience_id is not null
     and object_room_id is not null
"

executeHiveCommand "${hive_sql1}"