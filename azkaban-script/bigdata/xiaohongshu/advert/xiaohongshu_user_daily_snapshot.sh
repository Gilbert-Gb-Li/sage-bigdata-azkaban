#!/bin/bash
#***********************************************************************************
# **  文件名称: xiaohongshu_user_daily_snapshot.sh
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
LOGNAME="xiaohongshu_user_daily_snapshot"

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
SQL="insert overwrite table bigdata.xiaohongshu_user_daily_snapshot partition(dt)
select tmp5.app_package_name,
       tmp5.app_version,
       tmp5.resource_key,
       tmp5.record_time,
       tmp5.xhs_id,
       tmp5.dynamic_count,
       tmp5.short_video_count,
       tmp5.like_video_count,
       tmp5.school,
       tmp5.location,
       tmp5.prov,
       tmp5.city,
       tmp5.sex,
       tmp5.nick_name,
       tmp5.user_id,
       tmp5.signature,
       tmp5.follower_count,
       tmp5.like_count,
       tmp5.certificate_type,
       tmp5.certificate_info,
       tmp5.following_count,
       tmp5.shop_window,
       tmp5.age,
       tmp5.user_birthday,
       tmp5.user_share_url,
       tmp5.avatar_url,
       '${DAY_ID}' dt
  from (select tmp4.app_package_name,
               tmp4.app_version,
               tmp4.resource_key,
               tmp4.record_time,
               tmp4.xhs_id,
               tmp4.dynamic_count,
               tmp4.short_video_count,
               tmp4.like_video_count,
               tmp4.school,
               tmp4.location,
               tmp4.prov,
               tmp4.city,
               tmp4.sex,
               tmp4.nick_name,
               tmp4.user_id,
               tmp4.signature,
               tmp4.follower_count,
               tmp2.like_count,
               tmp4.certificate_type,
               tmp4.certificate_info,
               tmp4.following_count,
               tmp4.shop_window,
               tmp2.age,
               tmp2.user_birthday,
               tmp4.user_share_url,
               tmp2.avatar_url,
               row_number() OVER(PARTITION BY tmp4.user_id ORDER BY tmp4.record_time desc) rank
          from (select tmp1.app_package_name,
                       '' app_version,
                       tmp1.resource_key,
                       tmp1.record_time,
                       tmp1.xhs_id,
                       tmp1.dynamic_count,
                       tmp1.short_video_count,
                       tmp1.like_video_count,
                       tmp1.school,
                       tmp1.location,
                       tmp3.prov_name prov,
                       tmp3.city_name city,
                       tmp1.sex,
                       tmp1.nick_name,
                       tmp1.user_id,
                       tmp1.signature,
                       tmp1.follower_count,
                       0 like_count,
                       tmp1.certificate_type,
                       tmp1.certificate_info,
                       tmp1.following_count,
                       tmp1.shop_window,
                       '' age,
                       '' user_birthday,
                       tmp1.user_share_url,
                       split('',',') avatar_url
                  from (select app_package_name,
                               '' app_version,
                               resource_key,
                               record_time,
                               xhs_id,
                               dynamic_count,
                               note_number short_video_count,
                               like_video_count,
                               school,
                               location,
                               prov,
                               city,
                               sex,
                               nick_name,
                               user_id,
                               signature,
                               follower_count,
                               '' like_count,
                               certificate_type,
                               certificate_info,
                               following_count,
                               shop_window,
                               '' age,
                               '' user_birthday,
                               split('', ',') user_share_url,
                               split('', ',') avatar_url
                          from bigdata.advert_xiaohongshu_user_data_origin_orc
                         where dt = '${DAY_ID}'
                           and length(user_id) > 0) tmp1
                  left join bigdata.xiaohongshu_config_area_info tmp3
                    on tmp1.location = tmp3.address
                union all
                select app_package_name,
                       app_version,
                       resource_key,
                       record_time,
                       xhs_id,
                       dynamic_count,
                       short_video_count,
                       like_video_count,
                       school,
                       location,
                       prov,
                       city,
                       sex,
                       nick_name,
                       user_id,
                       signature,
                       follower_count,
                       like_count,
                       certificate_type,
                       certificate_info,
                       following_count,
                       shop_window,
                       age,
                       user_birthday,
                       user_share_url,
                       avatar_url
                  from bigdata.xiaohongshu_user_daily_snapshot
                 where dt = '${YES_DAY_ID}') tmp4
          left join (select author_id kol_id,
                           max(user_birthday) user_birthday,
                           max(age) age,
                           sum(like_count) like_count,
                           max(avatar_url) avatar_url
                      from bigdata.xiaohongshu_video_daily_snapshot
                     where dt = '${DAY_ID}'
                       and length(author_id) > 0
                     group by author_id) tmp2
            on tmp4.user_id = tmp2.kol_id) tmp5
 where tmp5.rank = 1;
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
