#!/bin/bash
#***********************************************************************************
# **  文件名称: xiaohongshu_brand_volume.sh
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
LOGNAME="xiaohongshu_brand_volume"

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
#正常执行hql
SQL="insert overwrite table bigdata.xiaohongshu_brand_volume partition
  (CYCLE, dt) 
select t5.pre_brand_id pre_brand_id, -- 父品类id
       t5.pre_brand_name pre_brand_name, -- 父品类名称
       t5.brand_id brand_id, -- 品类id
       t5.brand_name brand_name, -- 品类名称
       t5.brand_depth, -- 品类深度
       max(t5.mention_content_interact_avg) over(partition by t5.brand_depth) mention_content_interact_max, -- 内容平均互动量的最大值
       min(t5.mention_content_interact_avg) over(partition by t5.brand_depth) mention_content_interact_min, -- 内容平均互动量的最小值
       t5.mention_content_interact_avg, -- 当前品牌内容平均互动量
       1 + ((100 - 1) / ((max(t5.mention_content_interact_avg)
        over(partition by t5.brand_depth)) -
       (min(t5.mention_content_interact_avg)
        over(partition by t5.brand_depth)))) *
       (t5.mention_content_interact_avg -
       (min(t5.mention_content_interact_avg)
        over(partition by t5.brand_depth))) industrys_volume, -- 品类声量
       '${CYCLE}' cycle, -- 统计周期
       '${DAY_ID}' dt -- 数据生成日期    
  from (select t4.brand_id,
               t4.brand_name,
               t4.pre_brand_id,
               t4.brand_depth,
               t4.pre_brand_name,
               sum(t4.new_interact_num) / count(t4.content_id) mention_content_interact_avg
          from (select t3.brand_id,
                       t3.brand_name,
                       t3.pre_brand_id,
                       t3.brand_depth,
                       t3.pre_brand_name,
                       t1.content_id,
                       t2.new_interact_num
                  from (select brand_id, content_id
                          from bigdata.xiaohongshu_advert_source_keywords
                         where cast(regexp_replace(dt,'-','') as bigint)<= cast(regexp_replace('${DAY_ID}','-','') as bigint)
                         and cast(regexp_replace(dt,'-','') as bigint) > cast(regexp_replace('${PRE_DAY_ID}','-','') as bigint)
                         group by brand_id, content_id) t1
                 inner join (select content_id, new_interact_num
                              from bigdata.xiaohongshu_advert_content_calc_data
                             where dt = '${DAY_ID}'
                               and cycle = '${CYCLE}') t2
                    on t1.content_id = t2.content_id
                 inner join (select brand_id,
                                   brand_name,
                                   relat_brand_id,
                                   pre_brand_id,
                                   pre_brand_name,
                                   brand_depth
                              from bigdata.xiaohongshu_brand_category_config
                             group by brand_id,
                                      brand_name,
                                      relat_brand_id,
                                      pre_brand_id,
                                      pre_brand_name,
                                      brand_depth) t3
                    on t1.brand_id = t3.relat_brand_id
                 group by t3.brand_id,
                          t3.brand_name,
                          t3.pre_brand_id,
                          t3.brand_depth,
                          t3.pre_brand_name,
                          t1.content_id,
                          t2.new_interact_num) t4
         group by t4.brand_id,
                  t4.brand_name,
                  t4.pre_brand_id,
                  t4.brand_depth,
                  t4.pre_brand_name) t5;
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
