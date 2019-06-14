#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
tmpDir=/tmp/douyin
#${date}
hive_sql="select t.user_id,t.resource_key,t.app_version,t.app_package_name from
(select user_id,concat('com.ss.android.ugc.aweme',' ','USER_INFO',' ','com.ss.android.ugc.aweme.profile.ui.UserProfileActivity',' ',sha1(concat('com.ss.android.ugc.aweme','#_#','USER_INFO','#_#','com.ss.android.ugc.aweme.profile.ui.UserProfileActivity','#_#',user_id))) as resource_key
,app_version,app_package_name   from bigdata.douyin_advert_kol_data_snapshot where dt = '${date}' and follower_count > 600000 and user_id != '' and user_id is not null) t;"
   
# and app_version is not null and app_version != '' and app_package_name is not null and app_package_name != ''

#hdfs dfs -rmr /data/douyin/snapshot/ad/douyin_regreb_user_data_orc/dt=2019-05-14
##回灌用户
hive  -e "${hive_sql}" > /tmp/douyin/advert/ad_reimport_user_data.txt

hive_sql_video="select t.user_id,t.resource_key,t.app_version,t.app_package_name from
(select user_id,concat('com.ss.android.ugc.aweme',' ','USER_VIDEO_LIST',' ','com.ss.android.ugc.aweme.profile.ui.UserProfileActivity',' ',sha1(concat('com.ss.android.ugc.aweme','#_#','USER_VIDEO_LIST','#_#','com.ss.android.ugc.aweme.profile.ui.UserProfileActivity','#_#',user_id))) as resource_key
,app_version,app_package_name   from bigdata.douyin_advert_kol_data_snapshot where dt = '${date}' and follower_count > 600000 and user_id != '' and user_id is not null) t;"

##回灌视频
hive  -e "${hive_sql_video}" > /tmp/douyin/advert/ad_reimport_video_data.txt

hive_sql_follower_list="select t.user_id,t.resource_key,t.app_version,t.app_package_name from
(select user_id,concat('com.ss.android.ugc.aweme',' ','ATTENTION_FOLLOWER_LIST',' ','com.ss.android.ugc.aweme.following.ui.FollowingFollowerActivity',' ',sha1(concat('com.ss.android.ugc.aweme','#_#','ATTENTION_FOLLOWER_LIST','#_#','com.ss.android.ugc.aweme.following.ui.FollowingFollowerActivity','#_#',user_id))) as resource_key
,app_version,app_package_name   from bigdata.douyin_advert_kol_data_snapshot where dt = '${date}' and follower_count > 600000 and user_id != '' and user_id is not null) t;"

##回灌粉丝列表
hive  -e "${hive_sql_follower_list}" > /tmp/douyin/advert/ad_reimport_follower_data.txt

hive_sql_comment="select t.short_video_id,t.resource_key,t.app_version,t.app_package_name from
(select a.short_video_id,concat('com.ss.android.ugc.aweme',' ','COMMENT_LIST',' ','com.ss.android.ugc.aweme.detail.ui.DetailActivity',' ',sha1(concat('com.ss.android.ugc.aweme','#_#','COMMENT_LIST','#_#','com.ss.android.ugc.aweme.detail.ui.DetailActivity','#_#',a.short_video_id))) as resource_key,a.app_version,a.app_package_name
from (select short_video_id ,app_version,app_package_name,row_number() over(partition by author_id order by record_time desc) rank from bigdata.douyin_advert_content_reimport_snapshot where dt = '${date}' and author_id != '' and author_id is not null and short_video_id != '' and short_video_id is not null) a where a.rank =1 ) t;"

hive_sql_comment_all_video="select t.short_video_id,t.resource_key,t.app_version,t.app_package_name from
(select short_video_id,concat('com.ss.android.ugc.aweme',' ','COMMENT_LIST',' ','com.ss.android.ugc.aweme.detail.ui.DetailActivity',' ',sha1(concat('com.ss.android.ugc.aweme','#_#','COMMENT_LIST','#_#','com.ss.android.ugc.aweme.detail.ui.DetailActivity','#_#',short_video_id))) as resource_key,app_version,app_package_name
from bigdata.douyin_advert_content_reimport_snapshot where dt = '${date}' and author_id != '' and author_id is not null and short_video_id != '' and short_video_id is not null ) t;"
##回灌视频评论信息
hive  -e "${hive_sql_comment_all_video}" > /tmp/douyin/advert/ad_reimport_comment_data.txt

hive_sql_follower="with userandfanse as (select distinct c.kol_id from (
select a.kol_id ,fans_id,record_time,count(fans_id) over(partition by a.kol_id) fansenum from 
(select kol_id,fans_id,record_time from bigdata.douyin_advert_fans_data_snapshot where dt = '${date}') a 
left join
(select user_id from bigdata.douyin_user_daily_snapshot where dt = '${date}') b
on a.fans_id=b.user_id where b.user_id is not null )c where c.fansenum < 2000) -- 粉丝数小于2000的kol用户
select fans_id from (
select e1.kol_id,e1.fans_id,row_number() over(partition by e1.kol_id order by e1.record_time) as fanserank 
from (select kol_id,fans_id,record_time from bigdata.douyin_advert_fans_data_snapshot where dt = '${date}') e1 inner join
(select kol_id from userandfanse) e2 
on e1.kol_id = e2.kol_id
left join
(select user_id from bigdata.douyin_user_daily_snapshot where dt = '${date}' ) f 
on e1.fans_id = f.user_id where f.user_id is null ) d
where d.fanserank < 10  -- 每个kol取十个未抓取过的粉丝;"

##回灌补全kol粉丝到2000
#hive  -e "${hive_sql_follower}" >> /tmp/douyin/advert/ad_reimport_follower_user_data.txt

cat /tmp/douyin/advert/ad_reimport_user_data.txt >> /tmp/douyin/douyin_advert_data.txt
cat /tmp/douyin/advert/ad_reimport_video_data.txt >> /tmp/douyin/douyin_advert_data.txt
cat /tmp/douyin/advert/ad_reimport_follower_data.txt >> /tmp/douyin/douyin_advert_data.txt
cat /tmp/douyin/advert/ad_reimport_comment_data.txt >> /tmp/douyin/douyin_advert_data.txt