#!/bin/bash
#***********************************************************************************
# **  文件名称: advert_brand.sh
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
LOGNAME="advert_brand"

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
SQL="with tmp_biao as (
select tmp1.pid, tmp1.bid, tmp1.name ,case when tmp2.bid is null then '1' else '2' end type
from (select tt1.pid, tt1.bid, tt1.name
  from (select pid,
               bid,
               name,
               keywords,
               row_number() over(partition by bid order by cast(regexp_replace(dt, '-', '') as bigint)) rn
          from bigdata.advert_brand_daily_data) tt1
 where length(tt1.keywords) > 0
   and tt1.rn = 1) tmp1
left join (select tt1.pid, tt1.bid, tt1.name
  from (select pid,
               bid,
               name,
               keywords,
               row_number() over(partition by bid order by cast(regexp_replace(dt, '-', '') as bigint)) rn
          from bigdata.advert_brand_daily_data) tt1
 where length(tt1.keywords) > 0
   and tt1.rn = 1) tmp2
on tmp1.pid=tmp2.bid)
insert overwrite table bigdata.advert_brand partition (dt)
select COALESCE(temp10.bid,temp9.bid,temp8.bid,temp7.bid,temp6.bid,temp5.bid,temp4.bid,temp3.bid,temp2.bid,temp1.bid),
COALESCE(temp10.name,temp9.name,temp8.name,temp7.name,temp6.name,temp5.name,temp4.name,temp3.name,temp2.name,temp1.name),
COALESCE(temp10.pid,temp9.pid,temp8.pid,temp7.pid,temp6.pid,temp5.pid,temp4.pid,temp3.pid,temp2.pid,temp1.pid),
case when temp10.bid is not null then '10' 
when temp9.bid is not null then '9' 
when temp8.bid is not null then '8' 
when temp7.bid is not null then '7' 
when temp6.bid is not null then '6' 
when temp5.bid is not null then '5' 
when temp4.bid is not null then '4' 
when temp3.bid is not null then '3' 
when temp2.bid is not null then '2' 
when temp1.bid is not null then '1' 
end,
case when temp10.bid is not null then concat_ws(',',temp1.bid,temp2.bid,temp3.bid,temp4.bid,temp5.bid,temp6.bid,temp7.bid,temp8.bid,temp9.bid,temp10.bid)
when temp9.bid is not null then concat_ws(',',temp1.bid,temp2.bid,temp3.bid,temp4.bid,temp5.bid,temp6.bid,temp7.bid,temp8.bid,temp9.bid) 
when temp8.bid is not null then concat_ws(',',temp1.bid,temp2.bid,temp3.bid,temp4.bid,temp5.bid,temp6.bid,temp7.bid,temp8.bid)
when temp7.bid is not null then concat_ws(',',temp1.bid,temp2.bid,temp3.bid,temp4.bid,temp5.bid,temp6.bid,temp7.bid)
when temp6.bid is not null then concat_ws(',',temp1.bid,temp2.bid,temp3.bid,temp4.bid,temp5.bid,temp6.bid)
when temp5.bid is not null then concat_ws(',',temp1.bid,temp2.bid,temp3.bid,temp4.bid,temp5.bid)
when temp4.bid is not null then concat_ws(',',temp1.bid,temp2.bid,temp3.bid,temp4.bid)
when temp3.bid is not null then concat_ws(',',temp1.bid,temp2.bid,temp3.bid)
when temp2.bid is not null then concat_ws(',',temp1.bid,temp2.bid)
when temp1.bid is not null then concat_ws(',',temp1.bid)
end,
temp1.bid,
temp2.bid,
temp3.bid,
temp4.bid,
temp5.bid,
temp6.bid,
temp7.bid,
temp8.bid,
temp9.bid,
temp10.bid,
'${DAY_ID}' dt
from (select pid, bid, name
  from tmp_biao
 where type = '1') temp1
 left join tmp_biao temp2
 on temp1.bid=temp2.pid
 left join tmp_biao temp3
 on temp2.bid=temp3.pid
  left join tmp_biao temp4
 on temp3.bid=temp4.pid
  left join tmp_biao temp5
 on temp4.bid=temp5.pid
  left join tmp_biao temp6
 on temp5.bid=temp6.pid
  left join tmp_biao temp7
 on temp6.bid=temp7.pid
  left join tmp_biao temp8
 on temp7.bid=temp8.pid
  left join tmp_biao temp9
 on temp8.bid=temp9.pid
  left join tmp_biao temp10
 on temp9.bid=temp10.pid;
with tmp_biao as (
select tmp1.pid, tmp1.bid, tmp1.name ,case when tmp2.bid is null then '1' else '2' end type
from (select tt1.pid, tt1.bid, tt1.name
  from (select pid,
               bid,
               name,
               keywords,
               row_number() over(partition by bid order by cast(regexp_replace(dt, '-', '') as bigint)) rn
          from bigdata.advert_brand_daily_data) tt1
 where length(tt1.keywords) > 0
   and tt1.rn = 1) tmp1
left join (select tt1.pid, tt1.bid, tt1.name
  from (select pid,
               bid,
               name,
               keywords,
               row_number() over(partition by bid order by cast(regexp_replace(dt, '-', '') as bigint)) rn
          from bigdata.advert_brand_daily_data) tt1
 where length(tt1.keywords) > 0
   and tt1.rn = 1) tmp2
on tmp1.pid=tmp2.bid)
insert overwrite table bigdata.advert_brand partition (dt)
select t3.bid,t3.name,t3.pid,t3.depth,t3.path,
split(t3.path, ',')[0],
split(t3.path, ',')[1],
split(t3.path, ',')[2],
split(t3.path, ',')[3],
split(t3.path, ',')[4],
split(t3.path, ',')[5],
split(t3.path, ',')[6],
split(t3.path, ',')[7],
split(t3.path, ',')[8],
split(t3.path, ',')[9],
'${DAY_ID}' dt
from (select t1.bid,t1.name,t1.pid,
 case when split(t2.path, ',')[0]=t1.bid then '1'
   when split(t2.path, ',')[1]=t1.bid then '2'
   when split(t2.path, ',')[2]=t1.bid then '3'
   when split(t2.path, ',')[3]=t1.bid then '4'
   when split(t2.path, ',')[4]=t1.bid then '5'
   when split(t2.path, ',')[5]=t1.bid then '6'
   when split(t2.path, ',')[6]=t1.bid then '7'
   when split(t2.path, ',')[7]=t1.bid then '8'
   when split(t2.path, ',')[8]=t1.bid then '9'
   when split(t2.path, ',')[9]=t1.bid then '10'
     end depth,
   case when split(t2.path, ',')[0]=t1.bid then concat_ws(',',brand_1)
   when split(t2.path, ',')[1]=t1.bid then concat_ws(',',brand_1,brand_2)
   when split(t2.path, ',')[2]=t1.bid then concat_ws(',',brand_1,brand_2,brand_3)
   when split(t2.path, ',')[3]=t1.bid then concat_ws(',',brand_1,brand_2,brand_3,brand_4)
   when split(t2.path, ',')[4]=t1.bid then concat_ws(',',brand_1,brand_2,brand_3,brand_4,brand_5)
   when split(t2.path, ',')[5]=t1.bid then concat_ws(',',brand_1,brand_2,brand_3,brand_4,brand_5,brand_6)
   when split(t2.path, ',')[6]=t1.bid then concat_ws(',',brand_1,brand_2,brand_3,brand_4,brand_5,brand_6,brand_7)
   when split(t2.path, ',')[7]=t1.bid then concat_ws(',',brand_1,brand_2,brand_3,brand_4,brand_5,brand_6,brand_7,brand_8)
   when split(t2.path, ',')[8]=t1.bid then concat_ws(',',brand_1,brand_2,brand_3,brand_4,brand_5,brand_6,brand_7,brand_8,brand_9)
   when split(t2.path, ',')[9]=t1.bid then concat_ws(',',brand_1,brand_2,brand_3,brand_4,brand_5,brand_6,brand_7,brand_8,brand_9,brand_10)
     end path
     from tmp_biao t1, (select * from 
	 bigdata.advert_brand where dt='${DAY_ID}') t2
  where array_contains(split(t2.path, ','), t1.bid)) t3
  group by t3.bid,t3.name,t3.pid,t3.depth,t3.path;
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
