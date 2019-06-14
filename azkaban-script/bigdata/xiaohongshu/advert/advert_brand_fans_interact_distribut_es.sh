#!/bin/bash
#***********************************************************************************
# **  文件名称: advert_brand_fans_interact_distribut.sh
# **  创建日期: 2018年10月25日 
# **  编写人员: zsk
# 
# **  输入信息: 
# **  输出信息: 
# **
# **  功能描述: 
# **  处理过程:
#***********************************************************************************

#***********************************************************************************
#==修改日期==|===修改人=====|======================================================|
# .修改时间
#***********************************************************************************
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
source ~/.bash_profile
source ${baseDirForScriptSelf}/common.fun
echo "${baseDirForScriptSelf}/common.fun"
ScriptName=$0
###############配置区############################################################################
#日志输出路径
#/data11/dacp/dws_pro/logs
#dwi层的脚本统一路径为/data11/dacp/dwi_pro/logs
#dws层的脚本统一路径为/data11/dacp/dws_pro/logs
#dal层的脚本需要分集市，除了BDCSC用：/data11/dacp/dal_pro/logs
#其余用/data11/dacp/集市功能账号（一般为库名，不明确的需确认）/logs
DACPDIR="/data11/dacp/dws_pro/logs"
LOGPATH=${DACPDIR}
echo LOGPATH=${LOGPATH}
if [ ! -d "${LOGPATH}" ]; then
        mkdir -p ${LOGPATH}
fi
#表名称
LOGNAME="advert_brand_fans_interact_distribut_es"

#库名、队列名
USERNAME="bigdata"
QUEUENAME="root.bigdata.motl.mt8"
#测试区队列，提交验收时注释掉
#QUEUENAME="root.test.test15"

##############SQL变量############################################################################
##############逗号分割###########################################################################
DATES=20170301
#开起并发参数,一般单独执行的sql,不建议开起并发参数
#开并发时将concurrency赋值这一样打开，不开并发注释掉
concurrency=4
if [[ ! ${concurrency} ]];then
   CYCLE=1
else
    #ods省份编码
    CYCLE=1,7,30,60
fi
#################################################################################################
#报错发送信息,联系邮箱#邮件组


###############脚本参数判断######################################################################
#不输入参数,月份默认上个月,省份默认为配置省份
#输入参数为月份（例如201608）,月份默认上个月,省份默认为配置省份
#输入参数为dates,月份、省份为配置月份、省份
#################################################################################################
QUEUE1=$(echo $1|awk -F '.' '{print $1}')
QUEUE2=$(echo $2|awk -F '.' '{print $1}')
if [ $# -eq 1 ] && [ "$1"x != "dates"x ]  && [ "$QUEUE1"x != "queue"x ];then
             DATES=($1)
elif [ $# -eq 1 ] && [ "$QUEUE1"x = "queue"x ];then
             QUEUENAME=$(echo $1 |awk -F 'queue\\.' '{print $2}')
    #默认上个月
    DATES=($(date -d "$(date +%Y%m)01 -1 month" +%Y%m))
    #默认昨天
    # DATES=($(date +"%Y%m%d" -d "-1day"))
    
elif [ $# -eq 2 ];then
             DATES=($1)
    if [ "$QUEUE2"x = "queue"x ];then
             QUEUENAME=$(echo $2 |awk -F 'queue\\.' '{print $2}')
        else CYCLE=($2)
    fi
elif [ $# -eq 3 ];then
             DATES=($1)
             CYCLE=($2)
             QUEUENAME=$(echo $3 |awk -F 'queue\\.' '{print $2}')
else
    #默认上个月
    DATES=($(date -d "$(date +%Y%m)01 -1 month" +%Y%m))
    #默认昨天
    # DATES=($(date +"%Y%m%d" -d "-1day"))
fi
echo ${QUEUENAME}
CYCLE=(${CYCLE//,/ })
DATES=(${DATES//,/ })

#############HIVE参数区###########################################################################
#常用jar包 路径 /home/st001/soft/
#md5 输出结果md5大写
#add jar /home/st001/soft/BoncHiveUDF.jar;
#CREATE TEMPORARY FUNCTION MD5Encode AS 'com.bonc.hive.MyMD5';
#常规参数
COMMON_VAR="use ${USERNAME};
set mapreduce.job.queuename=${QUEUENAME};
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=256000000;
set mapred.min.split.size.per.rack=256000000;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles= true;
set hive.merge.size.per.task=134217728;
set hive.merge.smallfiles.avgsize=150000000;
"
#合并小文件参数
MERGE_VAR="use ${USERNAME};
set mapreduce.job.queuename=${QUEUENAME};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles= true;
set hive.merge.size.per.task=134217728;
set hive.merge.smallfiles.avgsize=150000000;
set mapred.max.split.size=134217728;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack = 100000000;
 
set hive.exec.compress.output = true;
set hive.hadoop.supports.splittable.combineinputformat=true;"

###############函数区################################################################################
###############时间配置函数##########################################################################
function CONFIGURE(){
    DAY_ID=$1
    PRE_DAY_ID=$(date -d "${DAY_ID}  -${CYCLE} day" +%Y-%m-%d)
	YES_DAY_ID=$(date -d "${DAY_ID}  -1 day" +%Y-%m-%d)
	THIRTY_BEFORE_DAY_ID=$(date -d "${DAY_ID}  -30 day" +%Y-%m-%d)
	DAY_ID=$(date -d "${DAY_ID} -0 day" +%Y-%m-%d)
}
#####################################################################################################
#程序执行开始时间
current_date=`date "+%Y%m%d"`
start_dt=`date "+%Y-%m-%d %H:%M:%S"`
start_date=`date "+%Y%m%d%H%M%S"`
i=0
#执行时间段,使用下面注释的for循环
#for (( DAY_ID=20170501; DAY_ID<=20170531; DAY_ID=`date -d "${DAY_ID} +1 day" "+%Y%m%d"` ))
for DAY_ID in ${DATES[@]};
do
    for CYCLE in ${CYCLE[@]};
    do
	CONFIGURE ${DAY_ID}
    let i+=1
#################################FOR循环开始##########################################################
###############################以下为SQL编辑区########################################################
time=$(date "+%Y%m%d")
RECENT_DAY_ID1=$(hive -e "show partitions bigdata.advert_category;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -u|head -n 1)
RECENT_DAY_ID2=$(hive -e "show partitions bigdata.advert_brand;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -u|head -n 1)
INTEREST_DAY_ID=$(hive -e "show partitions bigdata.advert_interest;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -u|head -n 1)
#正常执行hql
SQL="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
with tmp1 as (select t4.top_brand,
       t4.pl_name,
       t4.brand_name,
       t5.comment_top_interest,
       t4.like_count_rate, -- 点赞率
       t4.comment_fans_rate, --评论率
       t4.dive_rate, --潜水率
       t5.fans_user_id comment_user_id,
       case
         when t5.age >= 0 and t5.age <= 17 then
          '0-17'
         when t5.age >= 18 and t5.age <= 24 then
          '18-24'
         when t5.age >= 25 and t5.age <= 29 then
          '25-29'
         when t5.age >= 30 and t5.age <= 39 then
          '30-39'
         when t5.age >= 40 and t5.age <= 49 then
          '40-49'
         when t5.age >= 50 and t5.age <= 59 then
          '50-59'
         else
          '-1'
       end age_level, -- 年龄段
       case
         when t5.sex = '1' then
          '男'
         when t5.sex = '0' then
          '女'
       end sex,
       t5.prov -- 省分
  from (select t3.pl_id,
         t3.pl_name,
         t3.pre_pl_id,
         t3.pre_pl_name,
         t3.brand_id,
         t3.brand_name,
         t3.pre_brand_id,
         t3.pre_brand_name,
         t3.kol_id,
         t3.top_brand,
         sum(t3.new_like_count) / sum(t3.cover_fans_num + t3.new_like_count) like_count_rate, -- 点赞率
         sum(t3.comment_fans_num) / sum(t3.cover_fans_num + t3.new_like_count) comment_fans_rate, --评论率
         sum(t3.cover_fans_num - t3.comment_fans_num) /
         sum(t3.cover_fans_num + t3.new_like_count) dive_rate --潜水率
    from (select t2.pl_id,
                 t2.pl_name,
                 t2.pre_pl_id,
                 t2.pre_pl_name,
                 t2.brand_id,
                 t2.brand_name,
                 t2.pre_brand_id,
                 t2.pre_brand_name,
                 t3.brand_1 top_brand,
                 t1.kol_id,
                 t1.content_id,
                 t4.new_like_count,
                 t4.cover_fans_num,
                 t4.comment_fans_num
            from (
                  -- 从keywords取出近一个月的品牌品类数据并去重
                  select category_id,
                          category_name,
                          kol_id,
                          brand_name,
                          brand_id,
                          content_id
                    from bigdata.xiaohongshu_advert_source_keywords
                   where cast(regexp_replace(dt, '-', '') as bigint) <=
                         cast(regexp_replace('${DAY_ID}', '-', '') as bigint)
                     and cast(regexp_replace(dt, '-', '') as bigint) >
                         cast(regexp_replace('${PRE_DAY_ID}', '-', '') as
                              bigint)
                   group by category_id,
                             category_name,
                             kol_id,
                             brand_name,
                             brand_id,
                             content_id) t1
           inner join (
                      -- 匹配品牌品类配置表（单独为品牌品类统计生成的表）
                      select pre_pl_name,
                              pre_pl_id,
                              pl_name,
                              pl_id,
                              pre_brand_name,
                              pre_brand_id,
                              brand_name,
                              brand_id,
                              relat_brand_id,
                              relat_pl_id
                        from bigdata.xiaohongshu_brand_category_config) t2
              on t1.category_id = t2.relat_pl_id
             and t1.brand_id = t2.relat_brand_id
           inner join (select * from bigdata.advert_brand  where dt='${RECENT_DAY_ID2}') t3
              on t2.brand_id = t3.id
            left join (select content_id,
                             new_like_count,
                             cover_fans_num,
                             comment_fans_num
                        from bigdata.xiaohongshu_advert_content_calc_data
                       where dt = '${DAY_ID}'
                         and cycle = '${CYCLE}') t4
              on t1.content_id = t4.content_id) t3
   group by t3.pl_id,
            t3.pl_name,
            t3.pre_pl_id,
            t3.pre_pl_name,
            t3.brand_id,
            t3.brand_name,
            t3.pre_brand_id,
            t3.pre_brand_name,
            t3.kol_id,
            t3.top_brand) t4
 inner join (select tt3.user_id kol_id,
                    tt3.fans_user_id,
                    tt4.age,
                    tt4.sex,
                    tt4.prov,
                    tt4.city,
                    tt5.comment_top_interest
               from (select user_id, fans_user_id
                       from bigdata.advert_xiaohongshu_user_fans_data_origin_orc
                      where dt = '${DAY_ID}') tt3
              inner join (select user_id kol_id, age, sex, prov, city
                           from bigdata.xiaohongshu_user_daily_snapshot
                          where dt = '${DAY_ID}') tt4
                 on tt3.fans_user_id = tt4.kol_id
               left join (select tt1.kol_id,
                                tt2.interest_1 comment_top_interest
                           from bigdata.advert_xiaohongshu_kol_mark_orc tt1
                          inner join (select * from bigdata.advert_interest  where dt='${INTEREST_DAY_ID}') tt2
                             on tt1.interest_id = tt2.id) tt5
                 on tt3.fans_user_id = tt5.kol_id) t5
    on t4.kol_id = t5.kol_id),
tmp2 as
 (select t4.top_industrys,
       t4.pl_name,
       t5.comment_top_interest,
       t4.like_count_rate, -- 点赞率
       t4.comment_fans_rate, --评论率
       t4.dive_rate, --潜水率
       t5.fans_user_id comment_user_id,
       case
         when t5.age >= 0 and t5.age <= 17 then
          '0-17'
         when t5.age >= 18 and t5.age <= 24 then
          '18-24'
         when t5.age >= 25 and t5.age <= 29 then
          '25-29'
         when t5.age >= 30 and t5.age <= 39 then
          '30-39'
         when t5.age >= 40 and t5.age <= 49 then
          '40-49'
         when t5.age >= 50 and t5.age <= 59 then
          '50-59'
         else
          '-1'
       end age_level, -- 年龄段
       case
         when t5.sex = '1' then
          '男'
         when t5.sex = '0' then
          '女'
       end sex,
       t5.prov -- 省分
  from (select t3.pl_id,
               t3.pl_name,
               t3.pre_pl_id,
               t3.pre_pl_name,
               t3.kol_id,
               t3.top_industrys,
               sum(t3.new_like_count) /
               sum(t3.cover_fans_num + t3.new_like_count) like_count_rate, -- 点赞率
               sum(t3.comment_fans_num) /
               sum(t3.cover_fans_num + t3.new_like_count) comment_fans_rate, --评论率
               sum(t3.cover_fans_num - t3.comment_fans_num) /
               sum(t3.cover_fans_num + t3.new_like_count) dive_rate --潜水率
          from (select t2.pl_id,
                       t2.pl_name,
                       t2.pre_pl_id,
                       t2.pre_pl_name,
                       t1.content_id,
                       t1.kol_id,
                       t3.pl_1 top_industrys,
                       t4.new_like_count,
                       t4.cover_fans_num,
                       t4.comment_fans_num
                  from (select category_id, category_name, content_id, kol_id
                          from bigdata.xiaohongshu_advert_source_keywords
                         where cast(regexp_replace(dt, '-', '') as bigint) <=
                               cast(regexp_replace('${DAY_ID}', '-', '') as
                                    bigint)
                           and cast(regexp_replace(dt, '-', '') as bigint) >
                               cast(regexp_replace('${PRE_DAY_ID}', '-', '') as
                                    bigint)
                         group by category_id,
                                  category_name,
                                  content_id,
                                  kol_id) t1
                 inner join (select pl_id,
                                   pl_name,
                                   pre_pl_name,
                                   pre_pl_id,
                                   relat_pl_id
                              from bigdata.xiaohongshu_brand_category_config
                             group by pl_id,
                                      pl_name,
                                      pre_pl_name,
                                      pre_pl_id,
                                      relat_pl_id) t2
                    on t1.category_id = t2.relat_pl_id
                 inner join (select * from bigdata.advert_category  where dt='${RECENT_DAY_ID1}') t3
                    on t2.pl_id = t3.id
                  left join (select content_id,
                                   new_like_count,
                                   cover_fans_num,
                                   comment_fans_num
                              from bigdata.xiaohongshu_advert_content_calc_data
                             where dt = '${DAY_ID}'
                               and cycle = '${CYCLE}') t4
                    on t1.content_id = t4.content_id) t3
         group by t3.pl_id,
                  t3.pl_name,
                  t3.pre_pl_id,
                  t3.pre_pl_name,
                  t3.kol_id,
                  t3.top_industrys) t4
 inner join (select tt3.user_id kol_id,
                    tt3.fans_user_id,
                    tt4.age,
                    tt4.sex,
                    tt4.prov,
                    tt4.city,
                    tt5.comment_top_interest
               from (select user_id, fans_user_id
                       from bigdata.advert_xiaohongshu_user_fans_data_origin_orc
                      where dt = '${DAY_ID}') tt3
              inner join (select user_id kol_id, age, sex, prov, city
                           from bigdata.xiaohongshu_user_daily_snapshot
                          where dt = '${DAY_ID}') tt4
                 on tt3.fans_user_id = tt4.kol_id
               left join (select tt1.kol_id,
                                tt2.interest_1 comment_top_interest
                           from bigdata.advert_xiaohongshu_kol_mark_orc tt1
                          inner join (select * from bigdata.advert_interest  where dt='${INTEREST_DAY_ID}') tt2
                             on tt1.interest_id = tt2.id) tt5
                 on tt3.fans_user_id = tt5.kol_id) t5
    on t4.kol_id = t5.kol_id)
insert overwrite table bigdata.advert_brand_fans_interact_distribut_es partition
  (CYCLE, dt) 
select t4.stat_month,
       t4.timeindex,
       'xhs' platform,
       t4.top_industrys, -- 行业根节点
       t4.top_brand, -- 品牌根节点
       t4.industrys, -- 行业
       t4.brand_name, -- 品牌
      t4.type,t4.col,t4.val,avg(t4.val) over(partition by t4.top_brand) avg_val,
  '${DAY_ID}' ,       '${CYCLE}' ,t4.search_type,
            '${CYCLE}' cycle,
       '${DAY_ID}' dt
  from (select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
                unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
               '' top_industrys,
               top_brand,
               pl_name industrys,
               brand_name,
               '1' search_type,
               '性别' type,
               sex col,
               count(1) val
          from tmp1
         where sex in ('男', '女')
         group by top_brand, pl_name, brand_name, sex) t4
union all
select t4.stat_month,
       t4.timeindex,
       'xhs' platform,
       t4.top_industrys, -- 行业根节点
       t4.top_brand, -- 品牌根节点
       t4.industrys, -- 行业
       t4.brand_name, -- 品牌
      t4.type,t4.col,t4.val,avg(t4.val) over(partition by t4.top_brand) avg_val,
  '${DAY_ID}' ,       '${CYCLE}' ,t4.search_type,
            '${CYCLE}' cycle,
       '${DAY_ID}' dt
  from (select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
                unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
               '' top_industrys,
               top_brand,
               pl_name industrys,
               brand_name,
               '1' search_type,
               '年龄' type,
               age_level col,
               count(1) val
          from tmp1
         where age_level<>'-1'
         group by top_brand, pl_name, brand_name, age_level) t4
union all
select t4.stat_month,
       t4.timeindex,
       'xhs' platform,
       t4.top_industrys, -- 行业根节点
       t4.top_brand, -- 品牌根节点
       t4.industrys, -- 行业
       t4.brand_name, -- 品牌
      t4.type,t4.col,t4.val,avg(t4.val) over(partition by t4.top_brand) avg_val,
  '${DAY_ID}' ,       '${CYCLE}' ,t4.search_type,
            '${CYCLE}' cycle,
       '${DAY_ID}' dt
  from (select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
                unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
               '' top_industrys,
               top_brand,
               pl_name industrys,
               brand_name,
               '1' search_type,
               '兴趣' type,
               comment_top_interest col,
               count(1) val
          from tmp1
         where length(comment_top_interest)>0
         group by top_brand, pl_name, brand_name, comment_top_interest) t4
union all
select t4.stat_month,
       t4.timeindex,
       'xhs' platform,
       t4.top_industrys, -- 行业根节点
       t4.top_brand, -- 品牌根节点
       t4.industrys, -- 行业
       t4.brand_name, -- 品牌
      t4.type,t4.col,t4.val,avg(t4.val) over(partition by t4.top_brand) avg_val,
  '${DAY_ID}' ,       '${CYCLE}' ,t4.search_type,
            '${CYCLE}' cycle,
       '${DAY_ID}' dt
  from (select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
                unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
               '' top_industrys,
               top_brand,
               pl_name industrys,
               brand_name,
               '1' search_type,
               '省分' type,
               prov col,
               count(1) val
          from tmp1
         where length(prov)>0
         group by top_brand, pl_name, brand_name, prov) t4
union all
-- brand_name, pl_name,top_industrys,top_brand, like_count_rate, comment_fans_rate, dive_rate
select t4.stat_month,
       t4.timeindex,
       'xhs' platform,
       t4.top_industrys, -- 行业根节点
       t4.top_brand, -- 品牌根节点
       t4.industrys, -- 行业
       t4.brand_name, -- 品牌
       t4.type,
       t4.col,
       t4.val,
       avg(t4.val) over(partition by t4.top_brand) avg_val,   
       '${DAY_ID}' ,
	   '${CYCLE}' ,
       '1' search_type,
        '${CYCLE}' cycle,
       '${DAY_ID}' dt
  from (select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
                unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
               'xhs' platform,
               brand_name,
               pl_name industrys,
               '' top_industrys,
               top_brand,
               '粉丝活跃度' type,
               '点赞率' col,
               like_count_rate val
          from (select brand_name, pl_name,top_brand, like_count_rate, comment_fans_rate, dive_rate
    from tmp1 where length(comment_fans_rate)>0
   group by brand_name, pl_name,top_brand, like_count_rate,comment_fans_rate, dive_rate) tmp1_1
        union all
        select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
                unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
               'xhs' platform,
               brand_name,
               pl_name industrys,
               '' top_industrys,
               top_brand,
               '粉丝活跃度' type,
               '评论率' col,
               comment_fans_rate val
          from (select brand_name, pl_name,top_brand, like_count_rate, comment_fans_rate, dive_rate
    from tmp1 where length(comment_fans_rate)>0
   group by brand_name, pl_name,top_brand, like_count_rate,comment_fans_rate, dive_rate) tmp1_1
        union all
        select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
                unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
               'xhs' platform,
               brand_name,
               pl_name industrys,
               '' top_industrys,
               top_brand,
               '粉丝活跃度' type,
               '潜水率' col,
               dive_rate val
          from (select brand_name, pl_name,top_brand, like_count_rate, comment_fans_rate, dive_rate
    from tmp1 where length(comment_fans_rate)>0
   group by brand_name, pl_name,top_brand, like_count_rate,comment_fans_rate, dive_rate) tmp1_1) t4
union all
----------------------------------品类
select t4.stat_month,
       t4.timeindex,
       'xhs' platform,
       t4.top_industrys, -- 行业根节点
       t4.top_brand, -- 品牌根节点
       t4.industrys, -- 行业
       t4.brand_name, -- 品牌
       t4.type,t4.col,t4.val,avg(t4.val) over(partition by t4.top_industrys) avg_val,
       '${DAY_ID}' ,
	   '${CYCLE}' ,
       t4.search_type,
        '${CYCLE}' cycle,
       '${DAY_ID}' dt
  from (select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
                unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
               top_industrys,
               '' top_brand,
               pl_name industrys,
               '' brand_name,
               '2' search_type,
               '性别' type,
               sex col,
               count(1) val
          from tmp2
         where sex in ('男', '女')
         group by top_industrys, pl_name, sex) t4
union all
select t4.stat_month,
       t4.timeindex,
       'xhs' platform,
       t4.top_industrys, -- 行业根节点
       t4.top_brand, -- 品牌根节点
       t4.industrys, -- 行业
       t4.brand_name, -- 品牌
       t4.type,t4.col,t4.val,avg(t4.val) over(partition by t4.top_industrys) avg_val,
  '${DAY_ID}' ,       '${CYCLE}' ,
  t4.search_type,
            '${CYCLE}' cycle,
       '${DAY_ID}' dt
  from (select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
                unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
               top_industrys,
               '' top_brand,
               pl_name industrys,
               '' brand_name,
               '2' search_type,
               '年龄' type,
               age_level col,
               count(1) val
          from tmp2
         where age_level<>'-1'
         group by top_industrys, pl_name, age_level) t4
union all
select t4.stat_month,
       t4.timeindex,
       'xhs' platform,
       t4.top_industrys, -- 行业根节点
       t4.top_brand, -- 品牌根节点
       t4.industrys, -- 行业
       t4.brand_name, -- 品牌
       t4.type,t4.col,t4.val,avg(t4.val) over(partition by t4.top_industrys) avg_val,
  '${DAY_ID}' ,       '${CYCLE}' ,
  t4.search_type,
            '${CYCLE}' cycle,
       '${DAY_ID}' dt
  from (select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
                unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
               top_industrys,
               '' top_brand,
               pl_name industrys,
               '' brand_name,
               '2' search_type,
               '兴趣' type,
               comment_top_interest col,
               count(1) val
          from tmp2
         where length(comment_top_interest)>0
         group by top_industrys, pl_name, comment_top_interest) t4
union all
select t4.stat_month,
       t4.timeindex,
       'xhs' platform,
       t4.top_industrys, -- 行业根节点
       t4.top_brand, -- 品牌根节点
       t4.industrys, -- 行业
       t4.brand_name, -- 品牌
       t4.type,t4.col,t4.val,avg(t4.val) over(partition by t4.top_industrys) avg_val,
  '${DAY_ID}' ,       '${CYCLE}' ,
  t4.search_type,
            '${CYCLE}' cycle,
       '${DAY_ID}' dt
  from (select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
                unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
               top_industrys,
               '' top_brand,
               pl_name industrys,
               '' brand_name,
               '2' search_type,
               '省分' type,
               prov col,
               count(1) val
          from tmp2
         where length(prov)>0
         group by top_industrys, pl_name, prov) t4
union all
select t4.stat_month,
       t4.timeindex,
       'xhs' platform,
       t4.top_industrys, -- 行业根节点
       t4.top_brand, -- 品牌根节点
       t4.industrys, -- 行业
       t4.brand_name, -- 品牌
       t4.type,t4.col,t4.val,avg(t4.val) over(partition by t4.top_industrys) avg_val,
  '${DAY_ID}' ,       '${CYCLE}' ,
  '2' search_type,
            '${CYCLE}' cycle,
       '${DAY_ID}' dt
from (select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
 unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
'xhs' platform,
'' brand_name,
pl_name industrys,
top_industrys,
'' top_brand,
'粉丝活跃度' type,
'点赞率' col,
like_count_rate val
from (select '' brand_name, pl_name,top_industrys,'' top_brand, like_count_rate, comment_fans_rate, dive_rate
    from tmp2 where length(comment_fans_rate)>0
   group by pl_name,like_count_rate,top_industrys,comment_fans_rate, dive_rate) tmp2_1
union all
select  regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
 unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
'xhs' platform,
'' brand_name,
pl_name industrys,
top_industrys,
'' top_brand,
'粉丝活跃度' type,
'评论率' col,
comment_fans_rate val
from (select '' brand_name, pl_name,top_industrys,'' top_brand, like_count_rate, comment_fans_rate, dive_rate
    from tmp2 where length(comment_fans_rate)>0
   group by pl_name,like_count_rate,top_industrys,comment_fans_rate, dive_rate) tmp2_1
union all
select  regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
 unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000 timeindex, -- ES分片
'xhs' platform,
'' brand_name,
pl_name industrys,
top_industrys,
'' top_brand,
'粉丝活跃度' type,
'潜水率' col,
dive_rate val
from (select '' brand_name, pl_name,top_industrys,'' top_brand, like_count_rate, comment_fans_rate, dive_rate
    from tmp2 where length(comment_fans_rate)>0
   group by pl_name,like_count_rate,top_industrys,comment_fans_rate, dive_rate) tmp2_1) t4;
"

RunScript "${SQL}"


#执行mr
#SQL="hadoop jar sort.jar /apps/hive/warehouse/st001.db/ztj_test3/ /apps/hive/warehouse/st001.db/ztj_test2/ mt1"
#RunMr "${SQL}"

##################################FOR循环结束#########################################################
    echo "================================================================================"
    done
done
wait
######################################################################################################
##################################开始合并小文件#######################################################
#合并小文件方法
#Mergefile "${USERNAME}" "dm_ind_req_zhjt_4guser_m" "where month_id = '${MONTH_ID}'"

#程序执行结束时间
end_dt=`date "+%Y-%m-%d %H:%M:%S"`
time1=$(($(date +%s -d "$end_dt") - $(date +%s -d "$start_dt")))
if [ -f "${LOGPATH}/${LOGNAME}_error_${start_date}.log" ];then
    echo "执行失败"
    exit -1
else
 echo "执行成功"
fi
######################################################################################################
