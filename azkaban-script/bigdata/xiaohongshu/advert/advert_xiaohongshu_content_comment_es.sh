#!/bin/bash
#***********************************************************************************
# **  文件名称: advert_xiaohongshu_content_comment_es.sh
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
LOGNAME="advert_xiaohongshu_content_comment_es"

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
#正常执行hql
SQL="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
add jar ${baseDirForScriptSelf}/quchong.jar;
CREATE TEMPORARY FUNCTION StringDistinct AS 'udf.StringDistinct';
insert overwrite table bigdata.advert_xiaohongshu_content_comment_es partition
  ( dt) 
select regexp_replace('${DAY_ID}', '-', '') stat_month, --  月 
       unix_timestamp( '${DAY_ID}', 'yyyy-MM-dd' ) * 1000, -- ES分片
       t2.comment_id, -- 评论id
       t1.kol_id, -- KOLID
       'xhs' platform, -- 平台
       t1.kol_id, -- 平台KOLID
       t3.pls industrys, -- 行业集合
       t3.brands, -- 品牌集合
       t1.content_id, -- 内容id
       t1.cover_url, -- 封面url
       t1.description, -- 内容描述
       t1.source_time, -- 内容产生时间
       t1.like_count, -- 点赞总数
       t1.comments_count, -- 评论总数
       t1.share_count, -- 转发总数
       t1.interact_num, -- 互动总量
       t1.challenge_ids, -- 话题id
       t2.user_name, -- 评论人
       t2.comment, -- 评论内容
       t2.created_time, -- 评论时间 
       '' comment_prefer, -- 评论正负向 
       t2.like_count, -- 评论点总赞数 
	   '${DAY_ID}',
       '${DAY_ID}' dt --计算日期
  from (select content_id, -- 内容id
               kol_id, -- KOLID
               cover_url, -- 封面url
               description, -- 内容描述
               source_time, -- 内容产生时间
               like_count, -- 点赞总数
               comments_count, -- 评论总数
               share_count, -- 转发总数
               interact_num, -- 互动总量
               challenge_ids -- 话题id
          from bigdata.xiaohongshu_advert_content_calc_data
         where dt = '${DAY_ID}'
           and cycle = '1') t1
  left join (select short_video_id    content_id,
                    comment,
                    created_time,
                    comment_id,
                    comment_nick_name user_name,
                    like_count
               from bigdata.xiaohongshu_video_comment_daily_snapshot
              where dt = '${DAY_ID}') t2
    on t1.content_id = t2.content_id
  left join (select content_id,
                    split(StringDistinct(concat_ws(',', collect_set(t2.path))),
                          ',') pls,
                    split(StringDistinct(concat_ws(',', collect_set(t3.path))),
                          ',') brands
               from (select content_id, brand_id, category_id
                       from bigdata.xiaohongshu_advert_source_keywords
                      where dt='${DAY_ID}') t1
               left join (select * from bigdata.advert_category where dt='${RECENT_DAY_ID1}') t2
                 on t1.category_id = t2.id
               left join (select * from bigdata.advert_brand where dt='${RECENT_DAY_ID2}') t3
                 on t1.brand_id = t3.id
              group by t1.content_id) t3
    on t1.content_id = t3.content_id;
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
