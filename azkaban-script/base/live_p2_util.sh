#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh


#爬虫正在上报数据的app
#all_live_app_list="cn.lanpaopao888.live com.asiainno.uplive com.jj.shows com.lanmao.live com.lepaitianyu.live com.machipopo.aoyy com.maimiao.live.tv com.tiange.miaolive com.tunvlang.live com.zhifu.live com.huomaotv.mobile com.longzhu.tga com.huaxianzilive.live cn.miaoxiaojie.live com.lingdu.live com.machipopo.mejojo com.avshow.live com.sohu.qianfan com.lehai.ui com.guagua.qiqi com.meelive.ingkee"
all_live_app_list=`execSqlOnMysql "select biz_name as '' from tbl_live_p2_app_info where state=1001" `

echo ${all_live_app_list}
echo '====================='

//直播检测的app
monitor_app=`execSqlOnMysql "select biz_name as '' from tbl_live_p2_app_info where type=1" `
tmp_str="'"
for i in ${monitor_app};
do
  tmp_str=${tmp_str}${i}\'\,\'
done

#report_live_app_list="'cn.lanpaopao888.live','com.asiainno.uplive','com.jj.shows','com.lanmao.live','com.lepaitianyu.live','com.machipopo.aoyy','com.maimiao.live.tv','com.tiange.miaolive','com.tunvlang.live','com.zhifu.live','com.huomaotv.mobile','com.longzhu.tga','com.huaxianzilive.live','cn.miaoxiaojie.live','com.lingdu.live','com.machipopo.mejojo','com.avshow.live','com.sohu.qianfan','com.lehai.ui','com.guagua.qiqi'"

report_live_app_list=${tmp_str%\,\'}
echo ${report_live_app_list}

sex_live_app_list="''"


hive_gift_list="'com.meelive.ingkee'"


#违规视频长度大于该长度才算
live_record_video_length=5000

#映客默认超过1个小时不上报数据算第二次开播
ingkee_not_online=3600000

gift_info_sep='\002'
message_info_sep='\002'
audience_info_sep='\002'
guardian_info_sep='\002'
contributor_info_sep='\002'



gift_mysql_host=10.201.10.22
gift_mysql_port=3306
gift_mysql_user=ywreader
gift_mysql_password=hmyw@2016
gift_mysql_db=sage_gift

#gift_mysql_host=172.16.2.145
#gift_mysql_port=3306
#gift_mysql_user=root
#gift_mysql_password=haima123outfox
#gift_mysql_db=sage_gift


p2_location=/data/ias_p2
p2_location_origin=${p2_location}/origin
p2_location_live_orc=${p2_location}/live/orc
p2_location_live_snapshot=${p2_location}/live/snapshot

report_max_user_rank_count=100
report_max_audience_count=100

report_insight_rank_section="1 7 30"
#report_insight_rank_section="1"
report_insight_remain_check_day="1 2 3 4 5 6 7 14 30"
#report_insight_remain_check_day="1 2"

user_weight_income=0.25
user_weight_message_count=0.25
user_weight_live_count=0.25
user_weight_fans_count=0.25

platform_weight_income=0.25
platform_weight_message_count=0.25
platform_weight_live_count=0.25
platform_weight_active_user_count=0.25


####### 创建天/小时/app_id 分区
#### 参数1：hive表名（带库名）
#### 参数2：HDFS目录
#### 参数3：日期
#### 参数4：小时
#### 参数5：app_id
function createLivePartition(){
    if [ $(${hive} -e "SHOW PARTITIONS $1 PARTITION(dt='$3',hour='$4',app_id='$5');"|wc -l) -eq 0 ]; then
        echo "############## createLivePartition path:" $2/$3/$4/$5
        ${hive} -e "alter table $1 add partition (dt='$3',hour='$4',app_id='$5') location '$2/$3/$4/$5'"
    fi
}

####### 删除HDFS以及分区
#### 参数1：hive库名
#### 参数2：hive表名
#### 参数3：日期
#### 参数4：小时
#### 参数5：app_id
#### 参数6: hdfs path
function deleteIasCollectPartiton(){
    echo "############## deleteIasCollectPartiton path:" $6/$2/dt=$3/hour=$4/app_id=$5
    ${hadoop} fs -rm -r -skipTrash $6/dt=$3/hour=$4/app_id=$5|echo "deleteIasCollectPartiton: $3,$4,$5 on $1.$2 doesn't exists"
    if [ $(${hive} -e "SHOW PARTITIONS $1.$2 PARTITION(dt='$3',hour='$4',app_id='$5');"|wc -l) -eq 1 ]; then
        ${hive} -e "alter table $1.$2 drop partition (dt='$3',hour='$4',app_id='$5')"
    fi
}

####### 删除HDFS以及分区
#### 参数1：hive库名
#### 参数2：hive表名
#### 参数3：日期
#### 参数4：小时
#### 参数5：app_id
#### 参数6: hdfs path
function deleteLivePartiton4Orc(){
    echo "############## deleteLivePartiton4Orc path:" $6/$2/dt=$3/hour=$4/app_id=$5
    ${hadoop} fs -rm -r -skipTrash $6/$2/dt=$3/hour=$4/app_id=$5|echo "deleteLivePartiton4Orc: $3,$4,$5 on $1.$2 doesn't exists"
    if [ $(${hive} -e "SHOW PARTITIONS $1.$2 PARTITION(dt='$3',hour='$4',app_id='$5');"|wc -l) -eq 1 ]; then
        ${hive} -e "alter table $1.$2 drop partition (dt='$3',hour='$4',app_id='$5')"
    fi
}

######### 执行hive查询,结果入mysql表(按天,小时) 按照天-小时删除记录
#### 参数1: sql
#### 参数2: 日期
#### 参数3: 小时
#### 参数4: mysql表名
#### 参数5: mysql字段列表
#### 参数6: mysql日期字段
#### 参数7: mysql小时字段
function liveHiveSqlToMysql() {
    deleteMysqlData $4 "$6='$2' AND $7=$3"
    hiveSqlToMysqlNoDelete "$1" "$4" "$5"
}


######### 执行hive查询,结果入mysql表(按天,小时) 按照天-小时删除记录
#### 参数1: sql
#### 参数2: 日期
#### 参数3: 小时
#### 参数4: mysql表名
#### 参数5: mysql字段列表
#### 参数6: mysql日期字段
#### 参数7: mysql小时字段
function liveHiveSqlToMysqlUTF8MB4() {
    deleteMysqlData $4 "$6='$2' AND $7=$3"
    hiveSqlToMysqlNoDeleteUTF8MB4 "$1" "$4" "$5"
}

function hiveSqlRedirectToLocalNoConvert() {
    currentTime=$(date "+%s%N")
    path=${localDir}/${currentTime}
    mkdir -p ${path}
    ${hive} -e "$2 $1" | grep -v 'WARN' > ${path}/data
    sed -i 's/NULL//g' ${path}/data
    ## sed -i 's/,/-/g' ${path}/data
    echo ${path}
}

function hiveSqlToMysqlNoConvert() {
    path=$(hiveSqlRedirectToLocalNoConvert "$1" "$4")
    path=`echo "${path}"|grep -v WARN`
    ${mysql} -h${host} -P${port} -u${user}  -p${password} -D${db} --local-infile=1 -e "load data local infile '${path}/data' into table $2($3);"
    rm -rf ${path}
}

######### 执行hive查询,结果入mysql表(按天,小时) 按照天-小时删除记录
#### 参数1: sql
#### 参数2: 日期
#### 参数3: 小时
#### 参数4: mysql表名
#### 参数5: mysql字段列表
#### 参数6: mysql日期字段
#### 参数7: mysql小时字段
function liveHiveSqlToMysqlNoConvert() {
    deleteMysqlData $4 "$6='$2' AND $7=$3"
    hiveSqlToMysqlNoConvert "$1" "$4" "$5"
}

