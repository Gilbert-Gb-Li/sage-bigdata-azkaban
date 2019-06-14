#!/bin/bash
#***********************************************************************************
# **  文件名称: xiaohongshu_video_daily_snapshot.sh
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
LOGNAME="xiaohongshu_video_daily_snapshot"

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
SQL="insert overwrite table bigdata.xiaohongshu_video_daily_snapshot partition (dt)
select tt1.record_time,
       tt1.music_id,
       tt1.challenge_id,
       tt1.location,
       tt2.comments_count,
	   tt1.crawl_comments_count,
       tt1.description,
       tt1.commodity_id,
       tt1.like_count,
       tt1.is_advert,
       tt1.play_url_list,
       tt1.user_birthday,
       tt1.download_url_list,
       tt1.short_video_id,
       tt1.share_count,
       tt1.play_count,
       tt1.video_create_time,
       tt1.user_share_url,
       tt1.challenge_name,
       tt1.video_share_url,
       tt1.location_count,
       tt1.author_id,
       tt1.cover_url_list,
       tt1.avatar_url,
       tt1.hot_id,
       tt1.gender,
       tt1.age,
       tt1.content_nick_name,
       tt1.collect_num,
       tt1.hot_name,
       tt1.hot_type,
       '${DAY_ID}' dt
  from (select temp2.record_time,
               temp2.music_id,
               temp2.challenge_id,
               temp2.location,
               temp2.comments_count,
			   temp2.crawl_comments_count,
               temp2.description,
               temp2.commodity_id,
               temp2.like_count,
               temp2.is_advert,
               temp2.play_url_list,
               temp2.user_birthday,
               temp2.download_url_list,
               temp2.short_video_id,
               temp2.share_count,
               temp2.play_count,
               temp2.video_create_time,
               temp2.user_share_url,
               temp2.challenge_name,
               temp2.video_share_url,
               temp2.location_count,
               temp2.author_id,
               temp2.cover_url_list,
               temp2.avatar_url,
               temp2.hot_id,
               temp2.gender,
               temp2.age,
               temp2.content_nick_name,
               temp2.collect_num,
               temp2.hot_name,
               temp2.hot_type,
               '${DAY_ID}' dt
          from (select temp1.record_time,
                       temp1.music_id,
                       temp1.challenge_id,
                       temp1.location,
                       temp1.comments_count,
					   temp1.crawl_comments_count,
                       temp1.description,
                       temp1.commodity_id,
                       temp1.like_count,
                       temp1.is_advert,
                       temp1.play_url_list,
                       temp1.user_birthday,
                       temp1.download_url_list,
                       temp1.short_video_id,
                       temp1.share_count,
                       temp1.play_count,
                       temp1.video_create_time,
                       temp1.user_share_url,
                       temp1.challenge_name,
                       temp1.video_share_url,
                       temp1.location_count,
                       temp1.author_id,
                       temp1.cover_url_list,
                       temp1.avatar_url,
                       temp1.hot_id,
                       temp1.gender,
                       temp1.age,
                       temp1.content_nick_name,
                       temp1.collect_num,
                       temp1.hot_name,
                       temp1.hot_type,
                       '${DAY_ID}' dt,
                       row_number() OVER(PARTITION BY temp1.short_video_id ORDER BY temp1.record_time desc) rank
                  from (select tt1.record_time,
                               tt1.music_id,
                               tt1.challenge_id,
                               tt1.location,
							   0 comments_count,
                               tt1.crawl_comments_count,
                               tt1.description,
                               tt1.commodity_id,
                               tt1.like_count,
                               tt1.is_advert,
                               tt1.play_url_list,
                               tt1.user_birthday,
                               tt1.download_url_list,
                               tt1.short_video_id,
                               tt1.share_count,
                               tt1.play_count,
                               tt1.video_create_time,
                               tt1.user_share_url,
                               tt1.challenge_name,
                               tt1.video_share_url,
                               tt1.location_count,
                               tt1.author_id,
                               tt1.cover_url_list,
                               tt1.avatar_url,
                               tt1.hot_id,
                               tt1.gender, --没有该字段
                               tt1.age, -- 没有该字段
                               tt1.content_nick_name,
                               tt1.collect_num,
                               '' hot_name,
                               '' hot_type
                          from bigdata.advert_xiaohongshu_video_data_origin_orc tt1
                         where dt = '${DAY_ID}'
                           and length(short_video_id) > 0
                        union all
                        select record_time,
                               music_id,
                               challenge_id,
                               location,
                               comments_count,
							   crawl_comments_count,
                               description,
                               commodity_id,
                               like_count,
                               is_advert,
                               play_url_list,
                               user_birthday,
                               download_url_list,
                               short_video_id,
                               share_count,
                               play_count,
                               video_create_time,
                               user_share_url,
                               challenge_name,
                               video_share_url,
                               location_count,
                               author_id,
                               cover_url_list,
                               avatar_url,
                               hot_id,
                               gender,
                               age,
                               content_nick_name,
                               collect_num,
                               hot_name,
                               hot_type
                          from bigdata.xiaohongshu_video_daily_snapshot
                         where dt = '${YES_DAY_ID}') temp1) temp2
         where temp2.rank = 1) tt1
  left join (select short_video_id,
                    count(distinct comment_id) comments_count
               from bigdata.xiaohongshu_video_comment_daily_snapshot
              where dt = '${DAY_ID}'
              group by short_video_id) tt2
    on tt1.short_video_id = tt2.short_video_id;
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
