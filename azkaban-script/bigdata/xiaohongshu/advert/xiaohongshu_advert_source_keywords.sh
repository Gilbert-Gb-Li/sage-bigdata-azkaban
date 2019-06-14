#!/bin/bash
#***********************************************************************************
# **  文件名称: xiaohongshu_advert_source_keywords.sh
# **  创建日期: 2018年03月06日
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
#/data/log/advert
#dwi层的脚本统一路径为/data11/dacp/dwi_pro/logs
#dws层的脚本统一路径为/data/log/advert
#dal层的脚本需要分集市，除了BDCSC用：/data11/dacp/dal_pro/logs
#其余用/data11/dacp/集市功能账号（一般为库名，不明确的需确认）/logs
DACPDIR="logs"
LOGPATH=${DACPDIR}
echo LOGPATH=${LOGPATH}
if [ ! -d "${LOGPATH}" ]; then
        mkdir -p ${LOGPATH}
fi
#表名称
LOGNAME="xiaohongshu_advert_source_keywords"

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
#concurrency=4
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

    #默认昨天
    DATES=($(date +"%Y%m%d" -d "-2day"))

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

    #默认昨天
    DATES=($(date +"%Y%m%d" -d "-2day"))
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

RECENT_DAY_ID1=$(hive -e "show partitions bigdata.advert_category;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -udr|head -n 1)
RECENT_DAY_ID2=$(hive -e "show partitions bigdata.advert_brand;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -udr|head -n 1)
RECENT_DAY_ID11=$(hive -e "show partitions bigdata.advert_category_keywords;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -udr|head -n 1)
RECENT_DAY_ID22=$(hive -e "show partitions bigdata.advert_brand_keywords;"|awk -F '/' '{print $1}'|awk -F '=' '{print $2}'|sort -udr|head -n 1)
#正常执行hql
arr
SQL="add jar ${baseDirForScriptSelf}/keyword_1.0.jar;
CREATE TEMPORARY FUNCTION CwsUDF AS 'udf.CwsUDF';
insert overwrite table bigdata.xiaohongshu_advert_source_keywords partition (dt)
select t4.brand_keywords_id, -- 品牌关键词id
       (case
         when array_contains(split(t4.path, ','), t4.old_category_id) then
          t4.categroy_keywords_id
       end) categroy_keywords_id, -- 品类关键词id
       t4.brand_keywords, -- 品牌关键词
       (case
         when array_contains(split(t4.path, ','), t4.old_category_id) then
          t4.categroy_keywords
       end) categroy_keywords, -- 品类关键词
       t4.brand_id, -- 品牌id
       (case
         when array_contains(split(t4.path, ','), t4.old_category_id) then
          t4.new_category_id
         else
          t4.old_category_id
       end) category_id, -- 品类id
       t4.brand_name, -- 品牌的名称
       (case
         when array_contains(split(t4.path, ','), t4.old_category_id) then
          t4.new_category_name
         else
          t4.old_category_name
       end) category_name, -- 品类的名称
       '1' type, -- 1：内容，2：评论，3声音内容
       t4.content_id, -- 内容ID（短视频id）
       '' comment_id, -- 评论ID
       challenge_id, -- 话题id
       t4.kol_id, -- 用户ID
       t4.source_date, -- 资源上传时间
       t4.content_date, -- 内容上传时间
       '' comment_date, -- 评论时间
       t4.keywords_num, -- keywords提及数量
       '${DAY_ID}' dt -- 数据生成日期
  from (select t2.keyword_id brand_keywords_id, -- 品牌关键词id
               t3.keyword_id categroy_keywords_id,
               t2.old_category_id,
               t2.old_depth,
               t2.old_category_name,
               t3.new_category_id,
               t3.new_depth,
               t3.new_category_name,
               t3.path,
               row_number() over(partition by t2.brand_id, t1.short_video_id, t2.keyword_id order by(case
                 when array_contains(split(t3.path,','),t2.old_category_id) then
                  t3.new_depth + 1 else t2.old_depth end) desc) rn,
               t1.brand_keyword brand_keywords, -- 品牌关键词
               t1.categroy_keyword categroy_keywords,
               t2.brand_id, -- 品牌id
               t2.brand_name, -- 品牌的名称
               '1' type, -- 1：内容，2：评论，3声音内容
               t1.short_video_id content_id, -- 内容ID（短视频id）
               '' comment_id, -- 评论ID
               challenge_id, -- 话题id
               t1.author_id kol_id, -- 用户ID
               t1.video_create_time source_date, -- 资源上传时间
               t1.video_create_time content_date, -- 内容上传时间
               '' comment_date, -- 评论时间
               t1.brand_keyword_cnt keywords_num, -- keywords提及数量
               '${DAY_ID}' dt -- 数据生成日期
          from (select short_video_id,
                       description,
                       video_create_time,
                       author_id,
                       challenge_id,
                       split(brand_keyword, '-') [ 0 ] brand_keyword,
                       split(brand_keyword, '-') [ 1 ] brand_keyword_cnt,
                       split(categroy_keyword, '-') [ 0 ] categroy_keyword,
                       split(categroy_keyword, '-') [ 1 ] categroy_keyword_cnt
                  from (select short_video_id,
                               description,
                               video_create_time,
                               author_id,
                               challenge_id,
                               CwsUDF(description) as description_modle
                          from bigdata.xiaohongshu_video_daily_snapshot t
                         where dt = '${DAY_ID}'
                           and substr(video_create_time, 1, 8) =
                               regexp_replace('${DAY_ID}', '-', '')) t
                               LATERAL VIEW explode(split(split(t.description_modle, ':') [ 0 ], ',')) brand AS categroy_keyword
                               LATERAL VIEW explode(split(split(t.description_modle, ':') [ 1 ], ',')) categroy AS brand_keyword) t1
         inner join (select tmp1.keyword_id,
                           tmp1.keyword_name,
                           tmp1.brand_id,
                           tmp2.name         brand_name,
                           tmp1.category_id  old_category_id,
                           tmp3.depth        old_depth,
                           tmp3.name         old_category_name
                      from (select *
                              from bigdata.advert_brand_keywords
                             where dt = '${RECENT_DAY_ID22}') tmp1
                     inner join (select *
                                  from bigdata.advert_brand
                                 where dt = '${RECENT_DAY_ID2}') tmp2
                        on tmp1.brand_id = tmp2.id
                     inner join (select *
                                  from bigdata.advert_category
                                 where dt = '${RECENT_DAY_ID1}') tmp3
                        on tmp1.category_id = tmp3.id) t2
            on t1.brand_keyword = t2.keyword_name
          left join (select tmp1.keyword_id,
                           tmp1.keyword_name,
                           tmp1.category_id  new_category_id,
                           tmp2.path,
                           tmp2.depth        new_depth,
                           tmp2.name         new_category_name
                      from (select *
                              from bigdata.advert_category_keywords
                             where dt = '${RECENT_DAY_ID11}') tmp1
                     inner join (select *
                                  from bigdata.advert_category
                                 where dt = '${RECENT_DAY_ID1}') tmp2
                        on tmp1.category_id = tmp2.id) t3
            on t1.categroy_keyword = t3.keyword_name) t4
 where t4.rn = 1
union all
select t4.brand_keywords_id, -- 品牌关键词id
       (case
         when array_contains(split(t4.path, ','), t4.old_category_id) then
          t4.categroy_keywords_id
       end) categroy_keywords_id, -- 品类关键词id
       t4.brand_keywords, -- 品牌关键词
       (case
         when array_contains(split(t4.path, ','), t4.old_category_id) then
          t4.categroy_keywords
       end) categroy_keywords, -- 品类关键词
       t4.brand_id, -- 品牌id
       (case
         when array_contains(split(t4.path, ','), t4.old_category_id) then
          t4.new_category_id
         else
          t4.old_category_id
       end) category_id, -- 品类id
       t4.brand_name, -- 品牌的名称
       (case
         when array_contains(split(t4.path, ','), t4.old_category_id) then
          t4.new_category_name
         else
          t4.old_category_name
       end) category_name, -- 品类的名称
       '1' type, -- 1：内容，2：评论，3声音内容
       t4.content_id, -- 内容ID（短视频id）
       t4.comment_id, -- 评论ID
       t4.challenge_id, -- 话题id
       t4.kol_id, -- 用户ID
       t4.source_date, -- 资源上传时间
       t4.content_date, -- 内容上传时间
       t4.comment_date, -- 评论时间
       t4.keywords_num, -- keywords提及数量
       '${DAY_ID}' dt -- 数据生成日期
  from (select t2.keyword_id brand_keywords_id, -- 品牌关键词id
               t3.keyword_id categroy_keywords_id,
               t2.old_category_id,
               t2.old_depth,
               t2.old_category_name,
               t3.new_category_id,
               t3.new_depth,
               t3.new_category_name,
               t3.path,
               row_number() over(partition by t2.brand_id, t1.comment_id, t2.keyword_id,t2.old_category_id order by(case
                 when array_contains(split(t3.path,','),t2.old_category_id) then
                  t3.new_depth + 1 else t2.old_depth end) desc) rn,
               t1.brand_keyword brand_keywords, -- 品牌关键词
               t1.categroy_keyword categroy_keywords,
               t2.brand_id, -- 品牌id
               t2.brand_name, -- 品牌的名称
               '1' type, -- 1：内容，2：评论，3声音内容
               t1.short_video_id content_id, -- 内容ID（短视频id）
               t1.comment_id, -- 评论ID
               challenge_id, -- 话题id
               t1.author_id kol_id, -- 用户ID
               t1.video_create_time source_date, -- 资源上传时间
               t1.video_create_time content_date, -- 内容上传时间
               t1.created_time comment_date, -- 评论时间
               t1.brand_keyword_cnt keywords_num, -- keywords提及数量
               '${DAY_ID}' dt -- 数据生成日期
  from (select short_video_id,
               description,
               video_create_time,
               created_time,
               author_id,
               challenge_id,
               comment_id,
               split(brand_keyword, '-') [ 0 ] brand_keyword,
               split(brand_keyword, '-') [ 1 ] brand_keyword_cnt,
               split(categroy_keyword, '-') [ 0 ] categroy_keyword,
               split(categroy_keyword, '-') [ 1 ] categroy_keyword_cnt
          from (select a.short_video_id,
                       a.description,
                       a.video_create_time,
                       a.created_time,
                       a.author_id,
                       a.challenge_id,
             a.comment_id,
                       CwsUDF(a.description) as description_modle
                  from (select tmp1.short_video_id,
                               tmp1.created_time,
                               tmp2.video_create_time,
                               tmp1.comment        description,
                               tmp2.challenge_id,
                               tmp2.author_id,
                               tmp1.comment_id
                          from (select created_time, short_video_id, comment,comment_id
                                  from bigdata.xiaohongshu_video_comment_daily_snapshot
                                 where dt = '${DAY_ID}'
                 and substr(created_time,1,8)=regexp_replace('${DAY_ID}', '-', '')) tmp1
                          left join (select short_video_id,
                                           video_create_time,
                                           challenge_id,
                                           author_id
                                      from bigdata.xiaohongshu_video_daily_snapshot
                                     where dt = '${DAY_ID}') tmp2
                            on tmp1.short_video_id = tmp2.short_video_id) a) t 
        LATERAL VIEW explode(split(split(t.description_modle, ':') [ 0 ], ',')) brand AS categroy_keyword 
        LATERAL VIEW explode(split(split(t.description_modle, ':') [ 1 ], ',')) categroy AS brand_keyword) t1
 inner join (select tmp1.keyword_id,
                           tmp1.keyword_name,
                           tmp1.brand_id,
                           tmp2.name         brand_name,
                           tmp1.category_id  old_category_id,
                           tmp3.depth        old_depth,
                           tmp3.name         old_category_name
                      from (select *
                              from bigdata.advert_brand_keywords
                             where dt = '${RECENT_DAY_ID22}') tmp1
                     inner join (select *
                                  from bigdata.advert_brand
                                 where dt = '${RECENT_DAY_ID2}') tmp2
                        on tmp1.brand_id = tmp2.id
                     inner join (select *
                                  from bigdata.advert_category
                                 where dt = '${RECENT_DAY_ID1}') tmp3
                        on tmp1.category_id = tmp3.id) t2
            on t1.brand_keyword = t2.keyword_name
          left join (select tmp1.keyword_id,
                           tmp1.keyword_name,
                           tmp1.category_id  new_category_id,
                           tmp2.path,
                           tmp2.depth        new_depth,
                           tmp2.name         new_category_name
                      from (select *
                              from bigdata.advert_category_keywords
                             where dt = '${RECENT_DAY_ID11}') tmp1
                     inner join (select *
                                  from bigdata.advert_category
                                 where dt = '${RECENT_DAY_ID1}') tmp2
                        on tmp1.category_id = tmp2.id) t3
            on t1.categroy_keyword = t3.keyword_name) t4
 where t4.rn = 1;"

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
