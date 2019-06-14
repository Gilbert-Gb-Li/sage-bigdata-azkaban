#!/bin/sh
tmpDir=/tmp
localDir=${tmpDir}/automake
mysql=${MYSQL_BIN}


host=${mysql_host}
port=3306
user=${mysql_user}
password=${mysql_password}
db=${mysql_db}
es_url=${es_url}

host1=${mysql_takeout_host}
port1=3306
user1=${mysql_takeout_user}
password1=${mysql_takeout_password}
db1=${mysql_takeout_db}

ias_source="ias"

######### 执行hive查询,结果入mysql表(按天) 按照日期删除记录
#### 参数1: sql
#### 参数2: 日期
#### 参数3: mysql表名
#### 参数4: mysql字段列表
#### 参数5: mysql日期字段
function hiveSqlToMysqlUTF8MB4() {
    deleteMysqlData $3 "$5='$2'"
    hiveSqlToMysqlNoDeleteUTF8MB4 "$1" "$3" "$4"
}

######### 执行hive查询,结果入mysql表(按天) 清空之前mysql记录
#### 参数1: sql
#### 参数2: 日期
#### 参数3: mysql表名
#### 参数4: mysql字段列表
#### 参数5: mysql日期字段
function hiveSqlToMysqlTruncateUTF8MB4() {
    truncateMysqlTable $3
    hiveSqlToMysqlNoDeleteUTF8MB4 "$1" "$3" "$4"
}

######### 清空mysql表 #########
### 参数1: 表名
function truncateMysqlTable(){
    ${mysql} -h${host} -P${port} -u${user}  -p${password} -D${db} --local-infile=1 -e "truncate table $1;"
}

######### 执行hive查询,结果入mysql表
#### 参数1: sql
#### 参数2: mysql表名
#### 参数3: mysql字段列表
#### 参数4: hive参数(可选)
function hiveSqlToMysqlNoDeleteUTF8MB4() {

    path=$(hiveSqlRedirectToLocal "$1" "$4")
    path=`echo "${path}"|grep -v WARN`

    ${mysql} -h${host} -P${port} -u${user}  -p${password} -D${db} --local-infile=1 -e "load data local infile '${path}/data' into table $2 character set utf8mb4  ($3);"


    rm -rf ${path}
}



######### 执行hive查询,结果入mysql表(按天) 按照日期删除记录
#### 参数1: sql
#### 参数2: 日期
#### 参数3: mysql表名
#### 参数4: mysql字段列表
#### 参数5: mysql日期字段
function hiveSqlToMysql() {
    deleteMysqlData $3 "$5='$2'"
    hiveSqlToMysqlNoDelete "$1" "$3" "$4"
}

######### 执行hive查询,结果入mysql表 按照条件删除记录
#### 参数1: sql
#### 参数2: 删除条件
#### 参数3: mysql表名
#### 参数4: mysql字段列表
function hiveSqlToMysqlFree() {
    deleteMysqlData $3 "$2"
    hiveSqlToMysqlNoDelete "$1" "$3" "$4"
}

######### 执行hive查询,结果入hive表 ##########
#### 参数1: hive表名
#### 参数2: sql
#### 参数3: 分区日期
#### 参数4： 库名
#### 参数5： 表类型（origin或者snapshot）
function hiveSqlToTable() {
    path=$(hiveSqlToLocal "$2")
    ${hadoop} fs -rm -r -skipTrash /data/$4/$5/$1/$3|echo "hiveSqlToTable: $3 on $4.$1 doesn't exists"
    ${hadoop} fs -mkdir /data/$4/$5/$1/$3|echo "hiveSqlToTable: hdfs dir /data/$4/$5/$1/$3 already exists"
    ${hadoop} fs -put ${localDir}/${path}/* /data/$4/$5/$1/$3
    rm -rf ${localDir}/${path}
    if [ $(${hive} -e "SHOW PARTITIONS $4.$1 PARTITION(dt='$3');"|wc -l) -eq 0 ]; then
        ${hive} -e "alter table $4.$1 add partition (dt='$3') location '/data/$4/$5/$1/$3'"
    fi
}

######### 按条件删除mysql数据 #########
### 参数1: 表名
### 参数2: 条件
function deleteMysqlData() {
    sql="DELETE FROM $1 WHERE $2"
    echo ${sql}
    ${mysql} -h${host} -P${port} -u${user}  -p${password} -D${db} -e "${sql}"
}

######### 清空mysql数据 #########
### 参数1: 表名
function truncateMysqlData() {
    sql="TRUNCATE TABLE $1"
    echo ${sql}
    ${mysql} -h${host} -P${port} -u${user}  -p${password} -D${db} -e "${sql}"
}

######### 执行hive查询,结果放入本地目录
#### 参数1: sql
#### 参数2: hive参数
#### 返回生成的时间戳
function hiveSqlToLocal() {
    currentTime=$(date "+%s%N")
    path=${localDir}/${currentTime}
    mkdir -p ${path}
    ${hive} -e "$2 INSERT OVERWRITE LOCAL DIRECTORY '${path}' $1"
    echo ${currentTime}
}

######### 执行hive查询,结果放入本地目录
#### 参数1: sql
#### 参数2: hive参数
#### 返回生成的路径
function hiveSqlRedirectToLocal() {
    currentTime=$(date "+%s%N")
    path=${localDir}/${currentTime}
    mkdir -p ${path}
    ${hive} -e "$2 $1" | grep -v 'WARN' > ${path}/data
    sed -i 's/NULL//g' ${path}/data
    sed -i 's/,/-/g' ${path}/data
    echo ${path}
}

########创建当天分区（表名与分区名相同）
#### 参数1：hive表名
#### 参数2：hive库名
#### 参数3：表类型（origin或者snapshot）
#### 参数4：日期
function createPartition(){
    if [ $(${hive} -e "SHOW PARTITIONS $2.$1 PARTITION(dt='$4');"|wc -l) -eq 0 ]; then
        ${hive} -e "alter table $2.$1 add partition (dt='$4') location '/data/$2/$1/$3/$4'"
    fi
}


########创建当天分区（表名与分区名不同）
#### 参数1：hive表名（带库名）
#### 参数2：HDFS目录
#### 参数3：日期
function createPartitionDiff(){
    if [ $(${hive} -e "SHOW PARTITIONS $1 PARTITION(dt='$3');"|wc -l) -eq 0 ]; then
        ${hive} -e "alter table $1 add partition (dt='$3') location '$2/$3'"
    fi
}


########创建当天分区（表名与分区名不同）
#### 参数1：hive表名（带库名）
#### 参数2：HDFS目录
#### 参数3：日期
#### 参数4：小时
function createPartitionDiffHour(){
    if [ $(${hive} -e "SHOW PARTITIONS $1 PARTITION(dt='$3',hour='$4');"|wc -l) -eq 0 ]; then
        ${hive} -e "alter table $1 add partition (dt='$3',hour='$4') location '$2/$3/$4'"
    fi
}

########创建当天分区（表名与分区名不同）
#### 参数1：hive表名（带库名）
#### 参数2：HDFS目录
#### 参数3：日期
#### 参数4：小时
#### 参数5：app_id (包名)
function createPartitionDiffHourApp(){
    if [ $(${hive} -e "SHOW PARTITIONS $1 PARTITION(dt='$3',hour='$4',app_id='$5');"|wc -l) -eq 0 ]; then
        ${hive} -e "alter table $1 add partition (dt='$3',hour='$4',app_id='$5') location '$2/$3/$4/$5'"
    fi
}

#####删除HDFS以及当天分区
##### 参数1：hive表名
##### 参数2：hive库名
##### 参数3：表类型（origin或者snapshot）
##### 参数4：日期
function deleteHdfsAndPartiton(){
    if [ "$3" == "origin" ]; then
        ${hadoop} fs -rm -r -skipTrash /data/$2/$3/$1/$4|echo "deleteHdfsAndPartiton: $4 on $2.$1 doesn't exists"
    fi
    if [ "$3" == "snapshot" ]; then
        ${hadoop} fs -rm -r -skipTrash /data/$2/$3/$1/dt=$4|echo "deleteHdfsAndPartiton: $4 on $2.$1 doesn't exists"
    fi
    if [ $(${hive} -e "SHOW PARTITIONS $2.$1 PARTITION(dt='$4');"|wc -l) -eq 1 ]; then
        ${hive} -e "alter table $2.$1 drop partition (dt='$4')"
    fi
}

######### 删除hive表 ##########
#### 参数1: hive表名
#### 参数2：hive库名
function dropHiveTable(){
    ${hive} -e "drop table $2.$1"|echo ""
}

######### 执行Hive CMD ##########
#### 参数1: SQL
function executeHiveCommand(){
    ${hive} -e "$1"|echo ""
}


######### 执行Hive CMD 并返回结果 ##########
#### 参数1: SQL
function executeHiveCommandResult(){
    ${hive} -e "$1"
}


######### 执行hive查询,结果入mysql表
#### 参数1: sql
#### 参数2: mysql表名
#### 参数3: mysql字段列表
#### 参数4: hive参数(可选)
function hiveSqlToMysqlNoDelete() {
    path=$(hiveSqlRedirectToLocal "$1" "$4")
    path=`echo "${path}"|grep -v WARN`
    ${mysql} -h${host} -P${port} -u${user}  -p${password} -D${db} --local-infile=1 -e "load data local infile '${path}/data' into table $2 FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' ($3);"
   #  rm -rf ${path}
}

########## 执行mysql脚本
#### 参数1: sql
function execSqlOnMysql() {
    ${mysql} -h${host} -p${password} -P${port} -u${user} -D${db} --default-character-set=utf8 -e "$1"
}

########## 执行mysql脚本
#### 参数1: sql
function execSqlOnMysqlUTF8() {
    ${mysql} -h${host} -p${password} -P${port} -u${user} -D${db} --default-character-set=utf8 -e "$1"
}

######### 执行hive查询,结果入hive临时表,返回临时表名
#### 参数1: select sql
#### 参数2: 临时表前缀
function hiveSqlToTmpHive(){
    currentTime=$(date "+%s%N")
    tmpTable=$2_${currentTime}
    ${hive} -e "CREATE TABLE ${tmpTable} AS $1"
    echo ${tmpTable}
}

######### 生成临时表,返回临时表名
#### 参数1: table前缀
function createTmpTableName(){
    currentTime=$(date "+%s%N")
    tmpTable=$1_${currentTime}
    echo ${tmpTable}
}

######### 更新mysql表某个字段的数据
#### 参数1: mysql表名
#### 参数2: mysql字段名
#### 参数3: 更新后的字段值
#### 参数4: where条件
function updateMysqlFieldValue() {
    ${mysql} -h${host} -P${port} -u${user}  -p${password} -D${db} -e "update $1 set $2=$3 where $4;"
}

######### 删除过期ES中的index
#### 参数1: 删除的index列表，多个空格分隔
#### 参数2: 结束日期
#### 参数3: 删除天数(可选，默认是6)
#### 例子：如结束日期是2018-01-22，删除天数是6，会删除2018-01-16到2018-01-22的数据
# function delete_es_expire_data() {
#   data_arr=$1
#   if [ ! -n "$3" ] ;then
#       interval_day=6
#   else
#       interval_day=$3
#   fi

#   start_date=`date -d "-$interval_day day $2" +%s`
#   end_date=`date -d "$2" +%s`

#   echo "startDate:"`date -d "-$interval_day day $2" +%Y-%m-%d`",endDate:"$2

#   date=${start_date}
#   while [[ $date -le $end_date ]]
#   do
#       echo "date:"$date
#       date_format=`date -d @$date +%Y.%m.%d`
#       echo "date_format:"$date_format

#       for data in $data_arr
#       do
#         curl -X DELETE ${es_url}/${data}*-${date_format}
#       done

#       date=`expr $date + 86400`
#   done
# }

######### 删除过期ES中的index
#### 参数1: 删除的index列表，多个空格分隔
#### 参数2: 传入日期
#### 参数3: 删除n天前数(可选，默认是3)
#### 例子：如传入日期是2018-01-22，删除n天前是3，会删除的数据2018-01-19
function delete_es_expire_data() {
  data_arr=$1
  if [ ! -n "$3" ] ;then
      interval_day=3
  else
      interval_day=$3
  fi

  delete_date=`date -d "-$interval_day day $2" +%Y.%m.%d`

  echo "delete_date:"$delete_date
  for data in $data_arr
  do
    curl -X DELETE ${es_url}/${data}*-${date_format}
  done
}

######### 从HDFS导出数据
#### 参数1: 需要导出的topic和对应的表，topic和表逗号分隔，多个元素空格分隔
#### 参数2: 结束日期
#### 参数3: 所属项目，如ias
#### 参数4: 删除天数(可选，默认是6)
#### 例子：如结束日期是2018-01-22，删除天数是6，会删除2018-01-16到2018-01-22的数据
function export_hdfs_data() {
  data_arr=$1
  project=$3
  if [ ! -n "$4" ] ;then
      interval_day=2
  else
      interval_day=$4
  fi

  copy_path=${cold_data_base_path}/copy
  tar_path=${cold_data_base_path}/tar

  start_date=`date -d "-$interval_day day $2" +%s`
  end_date=`date -d "$2" +%s`

  echo "startDate:"`date -d "-$interval_day day $2" +%Y-%m-%d`",endDate:"$2

  if [ ! -d "$tar_path" ]; then
      echo "create $tar_path"
      mkdir -p $tar_path
  fi

  date=${start_date}
  while [[ $date -le $end_date ]]
  do
      echo "date:"$date
      date_format=`date -d @$date +%Y-%m-%d`
      echo "date_format:"$date_format

      for data in $data_arr
      do
        arr=(${data//,/ })
        topic=${arr[0]}
        table_name=${arr[1]}
        echo "topic:$topic"
        echo "table_name:$table_name"
        temp_path=$copy_path/$topic
        if [ ! -d "$temp_path" ]; then
            echo "create $temp_path"
            mkdir -p $temp_path
        fi

        echo "hdfs exists:/data/${project}/origin/$topic/${date_format}"
        $hadoop fs -test -e /data/${project}/origin/$topic/${date_format}
        if [[ $? -eq 1 ]] ;then
          echo "hdfs not exists:/data/${project}/origin/$topic/${date_format}"
          continue
        fi

        echo "get file from hdfs:/data/${project}/origin/$topic/${date_format}"
        $hadoop fs -get /data/${project}/origin/$topic/${date_format} $temp_path

        if [[ $? -eq 0 ]]; then
          echo "zip file:$tar_path/$topic-$date_format.tar.gz"
          tar -zcvf $tar_path/$topic-$date_format.tar.gz $temp_path/$date_format/*
        fi

        #if [[ $? -eq 0 ]]; then
          #echo "remote host:$remote_ip:$remote_tar_path"
          #echo "scp file:$tar_path/$topic-$date_format.tar.gz"
          #scp $tar_path/$topic-$date_format.tar.gz $remote_ip:$remote_tar_path
        #fi

        if [[ $? -eq 0 ]]; then
          echo "rm $temp_path/$date_format"
          `rm -rf $temp_path/$date_format`
        fi

        #if [[ $? -eq 0 ]]; then
          #echo "rm $tar_path/$topic-$date_format.tar.gz"
          #`rm -rf $tar_path/$topic-$date_format.tar.gz`
        #fi

        if [[ $? -eq 0 ]]; then
          echo "rm hadoop path:/data/${project}/origin/$topic/${date_format}"
          $hadoop fs -rm -r /data/${project}/origin/$topic/${date_format}
          $hive -e "ALTER TABLE $table_name DROP IF EXISTS PARTITION(dt='${date_format}'); "
        fi
      done

      date=`expr $date + 86400`
  done
}

######### 执行hive查询,结果外卖数据库mysql表
#### 参数1: sql
#### 参数2: mysql表名
#### 参数3: mysql字段列表
#### 参数4: hive参数(可选)
function hiveSqlToTakeoutMysqlNoDelete() {
    path=$(hiveSqlRedirectToLocal "$1" "$4")
    path=`echo "${path}"|grep -v WARN`
    sed -i 's/\\//g' ${path}/data  
    ${mysql} -h${host1} -P${port1} -u${user1}  -p${password1} -D${db1} --local-infile=1 -e "load data local infile '${path}/data' into table $2 character set utf8mb4 ($3);"
   # rm -rf ${path}
}

######### 清空外卖数据库mysql数据 #########
### 参数1: 表名
function truncateTakeoutMysqlData() {
    sql="TRUNCATE TABLE $1"
    echo ${sql}
    ${mysql} -h${host1} -P${port1} -u${user1}  -p${password1} -D${db1} -e "${sql}"
}

######### 执行hive查询,结果入外卖mysql表(按天) 按照日期删除记录
#### 参数1: sql
#### 参数2: 日期
#### 参数3: mysql表名
#### 参数4: mysql字段列表
#### 参数5: mysql日期字段
function hiveSqlToTakeoutMysql() {
    deleteTakeoutMysqlData $3 "$5='$2'"
    hiveSqlToTakeoutMysqlNoDelete "$1" "$3" "$4"
}

######### 按条件删除外卖mysql数据 #########
### 参数1: 表名
### 参数2: 条件
function deleteTakeoutMysqlData() {
    sql="DELETE FROM $1 WHERE $2"
    echo ${sql}
    ${mysql} -h${host1} -P${port1} -u${user1}  -p${password1} -D${db1} -e "${sql}"
}

######### 执行hive查询,结果入外卖mysql表(按天) 按照日期删除记录
#### 参数1: sql
#### 参数2: 日期
#### 参数3: mysql表名
#### 参数4: mysql字段列表
#### 参数5: mysql日期字段
function hiveSqlToTakeoutMysqlUTF8MB4() {
    deleteTakeoutMysqlData $3 "$5='$2'"
    hiveSqlToTakeoutMysqlNoDeleteUTF8MB4 "$1" "$3" "$4"
}

######### 执行hive查询,结果入mysql表
#### 参数1: sql
#### 参数2: mysql表名
#### 参数3: mysql字段列表
#### 参数4: hive参数(可选)
function hiveSqlToTakeoutMysqlNoDeleteUTF8MB4() {

    path=$(hiveSqlRedirectToLocal "$1" "$4")
    path=`echo "${path}"|grep -v WARN`
	sed -i 's/\\//g' ${path}/data
    ${mysql} -h${host1} -P${port1} -u${user1}  -p${password1} -D${db1} --local-infile=1 -e "load data local infile '${path}/data' into table $2 character set utf8mb4  ($3);"


    #rm -rf ${path}
}

########## 执行mysql脚本
#### 参数1: sql
function execSqlTakeoutOnMysql() {
    ${mysql} -h${host1} -p${password1} -P${port1} -u${user1} -D${db1} -e "$1"
} 


