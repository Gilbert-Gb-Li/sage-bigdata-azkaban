#!/bin/bash
#***********************************************************************************
# **  文件名称: advert_challenge_calc_es.sh
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
DACPDIR="/data11/dacp/dws_pro/logs"
LOGPATH=${DACPDIR}
echo LOGPATH=${LOGPATH}
if [ ! -d "${LOGPATH}" ]; then
        mkdir -p ${LOGPATH}
fi
#表名称
LOGNAME="advert_challenge_calc_es"

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
CERT_DAY_ID=$(hive -e "show partitions bigdata.advert_cert;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -u|head -n 1)
INTEREST_DAY_ID=$(hive -e "show partitions bigdata.advert_interest;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -u|head -n 1)
#正常执行hql
SQL="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
add jar ${baseDirForScriptSelf}/quchong.jar;
CREATE TEMPORARY FUNCTION StringDistinct AS 'udf.StringDistinct';
insert overwrite table bigdata.advert_xiaohongshu_challenge_calc_es partition
  (CYCLE, dt) 
select regexp_replace('${DAY_ID}', '-', '') stat_month, -- 月
       unix_timestamp('${DAY_ID}', 'yyyy-MM-dd') * 1000, -- ES分片
       t2.kol_id, -- KOLID
       'xhs' platform, -- 平台显示什么内容
       t3.kol_name, -- KOL名称
       t7.industrys, -- 行业
       t7.brands, -- 品牌
       split(t4.interest_class, ','), -- KOL兴趣分类
       split(t4.cert_label, ','), -- KOL认证标签
       t6.fans_num, -- 粉丝数量
       t3.age, -- 年龄
       t3.sex, -- 性别：-1未上报、1男、0女
       t3.prov, -- 省
       t3.city, -- 市
       t1.record_time source_time, -- 数据产生日期
       t1.challenge_id, -- 话题id
       t1.challenge_name, -- 话题名称
       t1.challenge_desc, -- 话题描述
       t1.challenge_imag_url, -- 话题图片
       t1.challenge_author, -- 话题发起人
       t1.play_count, -- 话题总播放量
       t8.play_count - t1.play_count new_play_count, -- 话题新增播放量
       '' video_count, -- 话题视频数
       t1.challenge_look_count, -- 参与人数
       count(t2.kol_id) over(partition by t1.challenge_id) kol_count, -- 关联kol数
       sum(t2.cover_fans_num) over(partition by t1.challenge_id) cover_fans_num, -- 覆盖粉丝数
       sum(t2.interact_fans_num) over(partition by t1.challenge_id) interact_fans_num, -- 互动粉丝数
       (sum(t2.cover_fans_num) over(partition by t1.challenge_id)) /
       (sum(t2.interact_fans_num) over(partition by t1.challenge_id)) fans_interact_rate, -- 粉丝互动率
       sum(t2.content_num) over(partition by t1.challenge_id) content_num, -- 内容数
       sum(t2.new_interact_num) over(partition by t1.challenge_id) content_interact_num, -- 内容互动量
       (sum(t2.new_interact_num) over(partition by t1.challenge_id)) /
       (sum(t2.content_num) over(partition by t1.challenge_id)) content_rate_avg,
       t6.cover_fans_num kol_cover_fans_num, -- 涉及的kol覆盖粉丝数
       t6.content_interact_num kol_content_interact_num, -- 涉及的kol的互动量
       t5.challenge_industrys, -- 话题涉及行业集合
       t5.challenge_brands, -- 话题涉及品牌集合
       '${DAY_ID}',
       '${CYCLE}',
       '${CYCLE}' cycle, -- 统计周期
       '${DAY_ID}' dt --  计算日期
  from (select record_time,
               challenge_look_count,
               challenge_name,
               challenge_desc,
               challenge_imag_url,
               challenge_author,
               play_count,
               challenge_id
          from bigdata.xiaohongshu_advert_challenge_daily_snapshot
         where dt = '${DAY_ID}') t1
 inner join (
             -- 考虑话题kol去重的问题
             select tmp2.challenge_id,
                     tmp2.kol_id,
                     count(distinct content_id) content_num,
                     sum(cover_fans_num) cover_fans_num,
                     sum(interact_fans_num) interact_fans_num,
                     sum(new_interact_num) new_interact_num
               from (select challenge_id,
                             kol_id,
                             cover_fans_num,
                             interact_fans_num,
                             new_interact_num,
                             content_id
                        from (select challenge_ids,
                                     cover_fans_num,
                                     interact_fans_num,
                                     new_interact_num,
                                     kol_id,
                                     content_id
                                from bigdata.xiaohongshu_advert_content_calc_data tt0
                               where dt = '${DAY_ID}'
                                 and cycle = '${CYCLE}') tt1 lateral view explode(tt1.challenge_ids) challenge_id as challenge_id) tmp2
              where length(tmp2.challenge_id) > 0
              group by tmp2.challenge_id, tmp2.kol_id) t2
    on t1.challenge_id = t2.challenge_id
  left join (select user_id        kol_id,
                    nick_name      kol_name,
                    follower_count fans_num,
                    age,
                    sex,
                    prov,
                    city,
                    record_time    source_time
               from bigdata.xiaohongshu_user_daily_snapshot
              where dt = '${DAY_ID}') t3
    on t2.kol_id = t3.kol_id
  left join (select tt4.kol_id, tt5.path cert_label, tt6.path interest_class
               from bigdata.advert_xiaohongshu_kol_mark_orc tt4
               left join (select *
                           from bigdata.advert_cert
                          where dt = '${CERT_DAY_ID}') tt5
                 on tt4.cert_id = tt5.id
               left join (select *
                           from bigdata.advert_interest
                          where dt = '${INTEREST_DAY_ID}') tt6
                 on tt4.interest_id = tt6.id) t4
    on t2.kol_id = t4.kol_id
  left join (select tt1.challenge_id,
                    split(StringDistinct(concat_ws(',',
                                                   collect_set(tt2.path))),
                          ',') challenge_industrys,
                    split(StringDistinct(concat_ws(',',
                                                   collect_set(tt3.path))),
                          ',') challenge_brands
               from (select challenge_id,
                            content_id,
                            brand_id,
                            category_id,
                            brand_name,
                            category_name
                       from bigdata.xiaohongshu_advert_source_keywords
                      where cast(regexp_replace(dt, '-', '') as bigint) <=
                            cast(regexp_replace('${DAY_ID}', '-', '') as
                                 bigint)
                        and cast(regexp_replace(dt, '-', '') as bigint) >
                            cast(regexp_replace('${PRE_DAY_ID}', '-', '') as
                                 bigint)
                        and type = '1') tt1
              inner join (select *
                           from bigdata.advert_category
                          where dt = '${RECENT_DAY_ID1}') tt2
                 on tt1.category_id = tt2.id
              inner join (select *
                           from bigdata.advert_brand
                          where dt = '${RECENT_DAY_ID2}') tt3
                 on tt1.brand_id = tt3.id
              group by tt1.challenge_id) t5
    on t1.challenge_id = t5.challenge_id
  left join (select kol_id,
                    content_interact_num,
                    interact_fans_num,
                    fans_num,
                    cover_fans_num
               from bigdata.xiaohongshu_advert_kol_calc_data
              where dt = '${DAY_ID}'
                and cycle = '${CYCLE}') t6
    on t2.kol_id = t6.kol_id
  left join (select kol_id, pls industrys, brands
               from bigdata.xiaohongshu_advert_kol_property_brand_industry
              where dt = '${DAY_ID}'
                and cycle = '${CYCLE}') t7
    on t2.kol_id = t7.kol_id
  left join (select record_time,
                    challenge_look_count,
                    challenge_name,
                    challenge_desc,
                    challenge_imag_url,
                    challenge_author,
                    play_count,
                    challenge_id
               from bigdata.xiaohongshu_advert_challenge_daily_snapshot
              where dt = '${YES_DAY_ID}') t8
    on t1.challenge_id = t8.challenge_id;
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
