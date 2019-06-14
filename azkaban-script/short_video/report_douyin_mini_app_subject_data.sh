#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
day2=`date -d "-1 day ${yesterday}" +%Y-%m-%d`

mysql_table1="t_compute_task"
mysql_table2="tbl_douyin_mini_app_subject_data"

#创建hive临时表用来用户和视频拼接的宽表，目的是为了方便后面的任务，不用重复跑相同的程序。节省资源。待程序执行完成后将会自动删除
currentTime=$(date "+%s%N")
tmpTable=tmp.douyin_subject_${currentTime}
executeHiveCommand "create table "${tmpTable}" (subject string,key_word string,short_video_id string,author_id string,like_count string,description string,cover_url_list array<string>,play_url_list array<string>,challenge_name string,music_name string,user_location string,constellation string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';"

query_sql1="set names utf8;select replace(replace(replace(data,'，',','),'；',';'),'：',':') as '' from ${mysql_table1} where id = '12345678'"
args=`execSqlOnMysql "${query_sql1}"`
echo "获取到的专题参数：${args}"
data1=(${args//;/ })
for args1 in ${data1[@]}
do
  data2=(${args1//-/ })
  echo "专题名:${data2[0]}"
  data3=(${data2[1]//,/ })
  for args2 in ${data3[@]}
  do
    type=(${args2//:/ })
    echo "分类:${type[0]}"
    key_words=(${type[1]//、/ })
    condition=''
    length=${#key_words[@]}
    if [ ${length} -eq 0 ]
    then
      condition='1=1 or '
    fi
    for key_word in ${key_words[@]}
    do
      echo "关键字:${key_word}"
      condition=$condition"t.description like concat('%','"${key_word}"','%') or t.challenge_name like concat('%','"${key_word}"','%') or t.music_name like concat('%','"${key_word}"','%') or t.user_location like concat('%','"${key_word}"','%') or t.constellation like concat('%','"${key_word}"','%') or "
    done
    echo "条件：${condition}"    
    hive_sql1="insert into $tmpTable select '${data2[0]}' as subject,'${type[0]}' as key_word,short_video_id,author_id,like_count,description,cover_url_list,play_url_list,challenge_name,music_name,user_location,constellation from
    (select a.short_video_id,a.author_id,a.like_count,a.description,a.cover_url_list,a.play_url_list,a.challenge_name,a.music_name,
    case when b.user_location is null then '' else b.user_location end user_location,
    case when b.constellation is null then '' else b.constellation end constellation from
    (select short_video_id,author_id,like_count,description,cover_url_list,play_url_list,challenge_name,music_name from
    short_video.tbl_ex_short_video_data_daily_snapshot
    where dt = '${yesterday}') a
    left join
    (select user_id,user_location, case when constellation = '1' then '白羊座'
    when constellation = '2' then '金牛座'
    when constellation = '3' then '双子座'
    when constellation = '4' then '巨蟹座'
    when constellation = '5' then '狮子座'
    when constellation = '6' then '处女座'
    when constellation = '7' then '天秤座'
    when constellation = '8' then '天蝎座'
    when constellation = '9' then '射手座'
    when constellation = '10' then '魔羯座'
    when constellation = '11' then '水瓶座'
    when constellation = '12' then '双鱼座'
    else '' end constellation from
    short_video.tbl_ex_short_video_user_daily_snapshot
    where dt = '${yesterday}') b
    on a.author_id = b.user_id) t
    where (t.description like concat('%','${type[0]}','%') or t.challenge_name like concat('%','${type[0]}','%') or t.music_name like concat('%','${type[0]}','%') or t.user_location like concat('%','${type[0]}','%') or t.constellation like concat('%','${type[0]}','%')) and ("$condition"1=2)"
    echo $hive_sql1
    executeHiveCommand "${hive_sql1}"
  done
done

hive_sql2="select '${yesterday}' as stat_date,t1.subject,t1.key_word,t2.short_video_count,t1.hot_video_top6 from
(select subject,key_word,concat_ws('\;',collect_set(video_info)) as hot_video_top6 from
(select subject,key_word,concat_ws('\;',concat('[\"',concat_ws('\",\"',cover_url_list),'\"]'),concat('[\"',concat_ws('\",\"',play_url_list),'\"]'),concat('[',cast(like_count as string),']')) as video_info from
(select t.*,row_number() over (partition by subject,key_word order by cast(like_count as bigint) desc) num from
(select subject,key_word,like_count,cover_url_list,play_url_list from $tmpTable) t) r
where r.num <= 6) a
group by subject,key_word) t1
left join
(select key_word,count(distinct short_video_id) as short_video_count from $tmpTable group by key_word) t2
on t1.key_word = t2.key_word"

hiveSqlToMysqlNoDelete "${hive_sql2}" "${mysql_table2}" "stat_date,subject,key_word,short_video_count,hot_video_top6"

executeHiveCommand "drop table ${tmpTable}"