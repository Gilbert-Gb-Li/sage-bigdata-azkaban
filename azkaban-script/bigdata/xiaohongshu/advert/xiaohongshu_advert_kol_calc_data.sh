#!/bin/bash
#***********************************************************************************
# **  文件名称: xiaohongshu_advert_kol_calc_data.sh
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
LOGNAME="xiaohongshu_advert_kol_calc_data"

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
#根据昨日KOL指数排名减去前日KOL指数排名
#################################FOR循环开始##########################################################
###############################以下为SQL编辑区########################################################
time=$(date "+%Y%m%d")
#正常执行hql
SQL="add jar ${baseDirForScriptSelf}/xiaohs-1.0.jar;
CREATE TEMPORARY FUNCTION ArraySum AS 'udf.ArraySum';
CREATE TEMPORARY FUNCTION fans_add_list AS 'udf.Cover';
CREATE TEMPORARY FUNCTION StandardDeviation AS 'udf.StandardDeviation';
insert overwrite table bigdata.xiaohongshu_advert_kol_calc_data partition
  (CYCLE, dt)
  select allin.record_time, -- 大数据处理时间
         allin.kol_id, -- KOLID
         allin.interact_avg, --   平均互动量
         allin.interact_min, --   最小平均互动量
         allin.interact_max, --   最大平均互动量
         1 + ((100 - 1) / (interact_max - interact_min)) *
         (interact_avg - interact_min) impact_index, --   影响力指数
         (case
           when length(fans_add_list) > 0 then
            fans_add_list(fans_add_list, fans_num - yes_fans_num)
           else
            fans_num - yes_fans_num
         end) fans_add_list,
         fans_num, -- 粉丝数
         cover_fans_num, --   覆盖粉丝数
         (case
           when ((fans_num - yes_fans_num) / yes_fans_num) > 0 and
                ((this_content_interact_num - yes_content_interact_num) /
                yes_content_interact_num) > 0 and
                ((fans_num - yes_fans_num) / yes_fans_num) /
                ((this_content_interact_num - yes_content_interact_num) /
                yes_content_interact_num) >= 3 or
                (fans_num - yes_fans_num) / yes_fans_num > 0 and
                (this_content_interact_num - yes_content_interact_num) /
                yes_content_interact_num < 0 then
            fans_num - yes_fans_num - (fans_num - third_fans_num) / 30 -
            StandardDeviation((case
                                when length(fans_add_list) > 0 then
                                 fans_add_list(fans_add_list, fans_num - yes_fans_num)
                                else
                                 fans_num - yes_fans_num
                              end))
		 else 0
         end) false_fans_num, --   假粉数
         interact_fans_num, --   互动粉丝数
         fans_interact_rate, --  粉丝互动率
         content_num, --   内容数
         content_interact_num, --   内容互动量
         content_rate_avg, -- 内容平均互动率
         1 + ((100 - 1) / (interact_max - interact_min)) *
         (interact_avg - interact_min) - nvl(last.impact_index, 0) impact_incr, -- KOL指数增长
         '${CYCLE}' cycle, -- 统计周期
         '${DAY_ID}' dt -- 数据生成日期
    from (select user3.record_time,
                 user3.kol_id,
                 user3.fans_num,
                 nvl(user3.content_interact_num, 0) content_interact_num,
                 nvl(yes.yes_fans_num, 0) yes_fans_num,
                 nvl(yes.third_fans_num, 0) third_fans_num,
                 yes.fans_add_list,
                 nvl(yes.yes_content_interact_num, 0) yes_content_interact_num,
                 nvl(yes.thirty_content_interact_num, 0) thirty_content_interact_num,
                 nvl(user3.interact_avg, 0) interact_avg,
                 last_value(nvl(user3.interact_avg, 0)) over(partition by '1' order by nvl(user3.interact_avg, 0) desc) interact_min,
                 first_value(nvl(user3.interact_avg, 0)) over(partition by '1' order by nvl(user3.interact_avg, 0) desc) interact_max,
                 nvl(user3.cover_fans_num, 0) cover_fans_num,
                 nvl(user3.interact_fans_num, 0) interact_fans_num,
                 nvl(user3.interact_fans_num, 0) / user3.cover_fans_num fans_interact_rate,
                 nvl(user3.content_num, 0) content_num,
                 nvl(user3.content_rate_avg, 0) content_rate_avg,
                 nvl(user3.this_content_interact_num, 0) this_content_interact_num
            from (select user2.kol_id,
                         user2.fans_num,
                         user2.cover_fans_num,
                         user2.record_time,
                         user2.interact_fans_num,
                         user2.content_num,
                         user2.content_interact_num,
                         user2.content_rate_avg,
                         user2.interact_avg,
                         content_yes.this_content_interact_num
                    from (select user1.kol_id,
                                 user1.fans_num,
                                 user1.cover_fans_num,
                                 user1.record_time,
                                 content.interact_fans_num,
                                 content.content_num,
                                 content.content_interact_num,
                                 content.content_rate_avg,
                                 content.interact_avg
                            from (
                                  -- 计算出当然粉丝数和周期内覆盖粉丝数以及大数据处理时间
                                  select user_id kol_id,
                                          max(case
                                                when dt = '${DAY_ID}' then
                                                 nvl(follower_count, 0)
                                              end) fans_num,
                                          max(nvl(follower_count, 0)) cover_fans_num,
                                          max(record_time) record_time
                                    from bigdata.xiaohongshu_user_daily_snapshot
                                   where cast(regexp_replace(dt, '-', '') as
                                              bigint) <=
                                         cast(regexp_replace('${DAY_ID}',
                                                             '-',
                                                             '') as bigint)
                                     and cast(regexp_replace(dt, '-', '') as
                                              bigint) >
                                         cast(regexp_replace('${PRE_DAY_ID}',
                                                             '-',
                                                             '') as bigint)
                                   group by user_id) user1
                            left join (
                                      -- 计算周期内互动粉丝数、内容数、新增互动数、粉丝平均互动率、平均互动量
                                      select kol_id,
                                              avg(nvl(interact_fans_num,0)) interact_fans_num,
                                              count(distinct content_id) content_num,
                                              sum(new_interact_num) content_interact_num,
                                              sum(new_interact_num) /
                                              count(distinct content_id) content_rate_avg,
                                              sum(new_interact_num) /
                                              count(distinct content_id) interact_avg
                                        from bigdata.xiaohongshu_advert_content_calc_data
                                       where dt = '${DAY_ID}'
                                         and cycle = '${CYCLE}'
                                       group by kol_id) content
                              on user1.kol_id = content.kol_id) user2
                    left join (
                              -- 计算出今天粉丝互动量（当天新增）
                              select kol_id,
                                      sum(new_interact_num) this_content_interact_num
                                from bigdata.xiaohongshu_advert_content_calc_data
                               where dt = '${DAY_ID}'
                                 and cycle = '1'
                               group by kol_id) content_yes
                      on user2.kol_id = content_yes.kol_id) user3
            left join (
                      -- 计算出昨天粉丝数、30天前粉丝数、昨天内容互动量、30天前内容互动量、昨天30天新增分析列表
                      select kol_id,
                              max(case
                                    when dt = '${YES_DAY_ID}' then
                                     fans_num
                                  end) yes_fans_num,
                              max(case
                                    when dt = '${THIRTY_BEFORE_DAY_ID}' then
                                     fans_num
                                  end) third_fans_num,
                              max(case
                                    when dt = '${YES_DAY_ID}' then
                                     content_interact_num
                                  end) yes_content_interact_num,
                              max(case
                                    when dt = '${THIRTY_BEFORE_DAY_ID}' then
                                     content_interact_num
                                  end) thirty_content_interact_num,
                              max(case
                                    when dt = '${YES_DAY_ID}' then
                                     fans_add_list
                                  end) fans_add_list
                        from bigdata.xiaohongshu_advert_kol_calc_data
                       where dt in
                             ('${YES_DAY_ID}', '${THIRTY_BEFORE_DAY_ID}')
                         and cycle = '1'
                       group by kol_id) yes
              on user3.kol_id = yes.kol_id) allin
    left join (select kol_id, impact_index
                 from bigdata.xiaohongshu_advert_kol_calc_data
                where dt = '${PRE_DAY_ID}'
                  and cycle = '${CYCLE}') last
      on allin.kol_id = last.kol_id;
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
