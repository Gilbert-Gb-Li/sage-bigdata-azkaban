#!/bin/bash
#***********************************************************************************
# **  文件名称: xiaohongshu_advert_content_calc_data.sh
# **  创建日期: 2018年03月06日 
# **  编写人员: zsk
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
LOGNAME="xiaohongshu_advert_content_calc_data"

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
SQL="insert overwrite table bigdata.xiaohongshu_advert_content_calc_data partition
  (CYCLE, dt)
  select new.record_time,  -- 大数据处理时间
         new.kol_id,  -- KOLID
         new.content_id,  -- 内容id
         new.description,  -- 内容描述
         new.cover_url,  -- 封面url
         new.video_url,  -- 视频url
         new.avatar_url,  -- 头像url
         new.challenge_ids,  -- 话题id
         nvl(new.like_count,0),  -- 点赞总数
         nvl(new.comments_count,0),  -- 评论总数
         nvl(new.share_count,0),  -- 转发总数
         nvl(new.interact_num,0),  -- 互动量
         nvl(new.like_count,0) - nvl(before.like_count,0) new_like_count,  -- 新增点赞数
         nvl(new.comments_count,0) - nvl(before.comments_count,0) new_comments_count,  -- 新增评论数
         nvl(new.share_count,0) - nvl(before.share_count,0) new_share_count,  -- 新增转发数
         nvl(new.interact_num,0) - nvl(before.interact_num,0) new_interact_num,  -- 新增互动量
         nvl(cover_fans_num,0),  -- 覆盖粉丝数
         nvl(new.like_count,0) - nvl(before.like_count,0) + nvl(comments_user_count,0) interact_fans_num,  -- 互动粉丝数
         (nvl(new.like_count,0) - nvl(before.like_count,0) + nvl(comments_user_count,0)) /
         cover_fans_num fans_interact_rate,  -- 粉丝互动率
         new.source_time,  -- 内容发布时间
         nvl(comments_user_count,0),  -- 评论用户数(去重)
         '${CYCLE}',
         '${DAY_ID}' dt
    from (select temp1.record_time,
                 temp1.kol_id,
                 temp1.content_id,
                 temp1.description,
                 temp1.cover_url,
                 temp1.video_url,
                 temp1.avatar_url,
                 split(temp1.challenge_ids,',') challenge_ids,
                 temp1.like_count,
                 temp1.comments_count,
                 temp1.share_count,
                 temp1.interact_num,
                 temp2.cover_fans_num,
                 temp3.comments_user_count,
                 temp1.source_time
          -- 使用内容表  需要合并话题去重
            from (select record_time,
                         author_id kol_id,
                         short_video_id content_id, -- short_video_id是否是内容id
                         description,
                         cover_url_list cover_url, -- 小红书无字段
                         play_url_list video_url, -- 小红书无字段
                         avatar_url,
                         challenge_id challenge_ids,
                         like_count,
                         comments_count,
                         share_count,
                         video_create_time source_time,
                         nvl(like_count,0) + nvl(comments_count,0) + nvl(collect_num,0) +
                         nvl(share_count,0) interact_num
                    from bigdata.xiaohongshu_video_daily_snapshot
                   where dt = '${DAY_ID}'
                     and substr(video_create_time, 1, 8) <= cast(regexp_replace('${DAY_ID}','-','') as bigint)
                     and substr(video_create_time, 1, 8) > cast(regexp_replace('${PRE_DAY_ID}','-','') as bigint)) temp1
           inner join (select user_id kol_id,
                             max(nvl(follower_count,0)) cover_fans_num
                        from bigdata.xiaohongshu_user_daily_snapshot
                       where  cast(regexp_replace(dt,'-','') as bigint)<= cast(regexp_replace('${DAY_ID}','-','') as bigint)
                         and cast(regexp_replace(dt,'-','') as bigint) > cast(regexp_replace('${PRE_DAY_ID}','-','') as bigint)
                       group by user_id) temp2
              on temp1.kol_id = temp2.kol_id
            left join (select short_video_id content_id,
                             count(distinct comment_id) comments_user_count
                        from bigdata.xiaohongshu_video_comment_daily_snapshot
                       where dt = '${DAY_ID}'
                         and substr(created_time, 1, 8) <=cast(regexp_replace('${DAY_ID}','-','') as bigint)
                         and substr(created_time, 1, 8) > cast(regexp_replace('${PRE_DAY_ID}','-','') as bigint)
                       group by short_video_id) temp3
              on temp1.content_id = temp3.content_id) new
    left join (select record_time,
                      kol_id,
                      content_id,
                      description,
                      cover_url,
                      video_url,
                      avatar_url,
                      challenge_ids,
                      like_count,
                      comments_count,
                      share_count,
                      interact_num,
                      source_time
                 from bigdata.xiaohongshu_advert_content_calc_data
                where dt = '${PRE_DAY_ID}'
                  and CYCLE = '${CYCLE}') before
      on new.content_id = before.content_id;
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
