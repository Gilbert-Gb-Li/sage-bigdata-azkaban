#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo '################添加hive jar包####################'
hive_jar="
  add jar hdfs://HaimaNX:8020/user/hadoop/share/commons-httpclient-3.1.jar;
  add jar hdfs://HaimaNX:8020/user/hadoop/share/elasticsearch-hadoop-6.3.2.jar;
"

echo '====================='

finance_live_app="'com.meelive.ingkee'"

#映客默认超过1个小时不上报数据算第二次开播
ingkee_not_online=3600000

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



## 映客
p3_location=/data/ias_p3
p3_location_origin=${p3_location}/origin
p3_location_live_orc=${p3_location}/live/orc
p3_location_live_snapshot=${p3_location}/live/snapshot

## YY
p3_location_yy=/data/yy
p3_location_origin_yy=${p3_location_yy}/origin
p3_location_snapshot_yy=${p3_location_yy}/snapshot

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

