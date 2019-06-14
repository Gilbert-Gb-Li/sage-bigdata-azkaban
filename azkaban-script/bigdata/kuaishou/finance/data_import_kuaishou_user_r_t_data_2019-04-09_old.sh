#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
stat_date=`date -d "$date" +%Y%m%d`
month1=`date -d "${date}" +%Y-%m`
month1_01=`date -d "${month1}-01" +%Y-%m-%d`
month1_01_1=`date -d "${month1}-01" +%Y%m%d`
month1_01_yesterday_1=`date -d "-1 day $month1_01" +%Y-%m-%d`
month2=`date -d "${month1_01_yesterday_1}" +%Y-%m`
month2_01=`date -d "${month2}-01" +%Y-%m-%d`
month2_01_1=`date -d "${month2}-01" +%Y%m%d`
year=`date -d "$date" +%Y`
month=`date -d "$date" +%m`
echo "date:${date}"
echo "yesterday:${yesterday}"
echo "stat_date:${stat_date}"
echo "month1:${month1}"
echo "month1_01:${month1_01}"
echo "month1_01_1:${month1_01_1}"
echo "month1_01_yesterday_1:${month1_01_yesterday_1}"
echo "month2:${month2}"
echo "month2_01:${month2_01}"
echo "month2_01_1:${month2_01_1}"
echo "year:${year}"
echo "month:${month}"

apk_name="com.smile.gifmaker"


    echo "##################  获取用户的视频获赞数、评论数 ########################"
    tmp_finance_kuaishou_user_like_count_and_video_comment_count="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_user_like_count_and_video_comment_count AS
    select b.app_package_name,b.user_id,
        b.video_like_count as like_count,
        if(c.video_like_count is not null and c.video_like_count>0,b.video_like_count-c.video_like_count,b.video_like_count) as new_like_count,
        b.video_comment_count,
        if(c.video_comment_count is not null and c.video_comment_count>0,b.video_comment_count-c.video_comment_count,b.video_comment_count) as new_video_comment_count
    from(
        select a.app_package_name,a.user_id,sum(video_like_count) as video_like_count,sum(video_comment_count) as video_comment_count
        from (
            SELECT app_package_name,user_id,if(video_like_count>0,video_like_count,0) as video_like_count,if(video_comment_count>0,video_comment_count,0) as video_comment_count
            FROM bigdata.kuaishou_short_video_data_daily_snapshot
            WHERE dt='${date}' AND app_package_name='${apk_name}' AND user_id is not null AND user_id !=''
        ) as a group by a.app_package_name,a.user_id
    ) as b
    left join (
        select a.app_package_name,a.user_id,sum(video_like_count) as video_like_count,sum(video_comment_count) as video_comment_count
        from (
            SELECT app_package_name,user_id,if(video_like_count>0,video_like_count,0) as video_like_count,if(video_comment_count>0,video_comment_count,0) as video_comment_count
            FROM bigdata.kuaishou_short_video_data_daily_snapshot
            WHERE dt='${yesterday}' AND app_package_name='${apk_name}' AND user_id is not null AND user_id !=''
        ) as a group by a.app_package_name,a.user_id
    ) as c
    on b.app_package_name=c.app_package_name and b.user_id=c.user_id;
    "
    echo "${tmp_finance_kuaishou_user_like_count_and_video_comment_count}"
    echo "######################################################################"


    echo "##################  获取用户参加话题短视频数量 ########################"
    tmp_finance_kuaishou_user_challenge_video_count="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_user_challenge_video_count AS
    select c.app_package_name,c.user_id,
        max(c.video_count_1) as challenge_video_count,
        if(max(c.video_count_1)>max(c.video_count_2) ,max(c.video_count_1)-max(c.video_count_2),max(c.video_count_1)) as new_challenge_video_count
    from(
        select a.app_package_name,a.user_id,count(distinct video_id) as video_count_1,0 as video_count_2
        from (
            SELECT app_package_name,user_id,video_id
            FROM bigdata.kuaishou_challenge_video_data_daily_snapshot
            WHERE dt='${date}' AND app_package_name='${apk_name}'
            AND user_id is not null AND user_id !=''
        ) as a group by a.app_package_name,a.user_id
        UNION ALL
        select a.app_package_name,a.user_id,0 as video_count_1,count(distinct video_id) as video_count_2
        from (
            SELECT app_package_name,user_id,video_id
            FROM bigdata.kuaishou_challenge_video_data_daily_snapshot
            WHERE dt='${yesterday}' AND app_package_name='${apk_name}'
            AND user_id is not null AND user_id !=''
        ) as a group by a.app_package_name,a.user_id
    ) as c
    group by c.app_package_name,c.user_id
    ;
    "
    echo "${tmp_finance_kuaishou_user_challenge_video_count}"
    echo "##############################################################"


    echo "##################  获取用户评论数量 ########################"
    tmp_finance_kuaishou_user_comment_count_subset_1="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_user_comment_count_subset_1 AS
    select a.app_package_name,a.user_id,count(distinct comment_id) as comment_count
    from (
        SELECT app_package_name,comment_user_id as user_id,comment_id
        FROM bigdata.kuaishou_short_video_comment_data_daily_snapshot
        WHERE dt='${date}' AND app_package_name='${apk_name}'
        AND comment_user_id is not null AND comment_user_id !=''
    ) as a group by a.app_package_name,a.user_id;
    "

    tmp_finance_kuaishou_user_comment_count_subset_2="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_user_comment_count_subset_2 AS
    select a.app_package_name,a.user_id,count(distinct comment_id) as comment_count
    from (
        SELECT app_package_name,comment_user_id as user_id,comment_id
        FROM bigdata.kuaishou_short_video_comment_data_daily_snapshot
        WHERE dt='${yesterday}' AND app_package_name='${apk_name}'
        AND comment_user_id is not null AND comment_user_id !=''
    ) as a group by a.app_package_name,a.user_id;
    "

    tmp_finance_kuaishou_user_comment_count="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_user_comment_count AS
    select b.app_package_name,b.user_id,
        b.comment_count as user_comment_count,
        if(c.comment_count is not null and c.comment_count>0,b.comment_count-c.comment_count,b.comment_count) as new_user_comment_count
    from(
        select * from  default.tmp_finance_kuaishou_user_comment_count_subset_1
    )  b
    left join (
        select * from  default.tmp_finance_kuaishou_user_comment_count_subset_2
    ) c
    on b.app_package_name=c.app_package_name and b.user_id=c.user_id;
    "
    echo "${tmp_finance_kuaishou_user_comment_count_subset_1}"
    echo "${tmp_finance_kuaishou_user_comment_count_subset_2}"
    echo "${tmp_finance_kuaishou_user_comment_count}"
    echo "#################################################################"


    echo "##################  获取说说评论论数量 ########################"
    tmp_finance_kuaishou_user_talk_comment_count="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_user_talk_comment_count AS
    select b.app_package_name,b.user_id,
        b.comment_count as talk_comment_count,
        if(c.comment_count is not null and c.comment_count>0,b.comment_count-c.comment_count,b.comment_count) as new_talk_comment_count
    from(
        select a.app_package_name,a.user_id,sum(comment_count) as comment_count
        from (
            SELECT app_package_name,user_id,talk_id,if(max(comment_count) >0,max(comment_count)+count(distinct comment_id),count(distinct comment_id)) as comment_count
            FROM bigdata.kuaishou_talk_comment_data_daily_snapshot
            WHERE dt='${date}' AND app_package_name='${apk_name}'
            AND user_id is not null AND user_id !=''
            group by app_package_name,user_id,talk_id
        ) as a group by a.app_package_name,a.user_id
    ) as b
    left join (
        select a.app_package_name,a.user_id,sum(comment_count) as comment_count
        from (
            SELECT app_package_name,user_id,talk_id,if(max(comment_count) >0,max(comment_count)+count(distinct comment_id),count(distinct comment_id)) as comment_count
            FROM bigdata.kuaishou_talk_comment_data_daily_snapshot
            WHERE dt='${yesterday}' AND app_package_name='${apk_name}'
            AND user_id is not null AND user_id !=''
            group by app_package_name,user_id,talk_id
        ) as a group by a.app_package_name,a.user_id
    ) as c
    on b.app_package_name=c.app_package_name and b.user_id=c.user_id;
    "
    echo "${tmp_finance_kuaishou_user_talk_comment_count}"
    echo "##############################################################"


    echo "#################### 获取有商品的用户数据  ####################"
    tmp_finance_kuaishou_user_commodity_count="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_user_commodity_count AS
    select a.app_package_name,a.user_id,a.commodity_count
    from(
        select app_package_name,user_id,count(distinct commodity_name) as commodity_count
        from bigdata.kuaishou_user_commodity_info_data_origin_orc
        where dt='${date}' AND app_package_name='${apk_name}'
                AND user_id is not null AND user_id !=''
                AND commodity_name is not null AND commodity_name !=''
        group by app_package_name,user_id
    ) as a
    where a.commodity_count>0;
    "
    echo "${tmp_finance_kuaishou_user_commodity_count}"
    echo "##############################################################"

    echo "##################### 整合头部用户数据 #############################"
    tmp_finance_kuaishou_user_t_data="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_user_t_data AS
    select d.resource_key,d.app_version,d.data_generate_time,d.app_package_name,d.user_name,d.user_id,d.kwai_id,d.user_share_url,d.signature,d.store_or_curriculum,d.hometown,d.sex,d.constellation,d.certification,
            d.follower_count,d.new_follower_count,
            d.following_count,d.new_following_count,
            d.short_video_count,d.new_short_video_count,
            d.talk_count,d.new_talk_count,
            d.music_count,d.new_music_count,
            if(e.like_count is not null and e.like_count>0,e.like_count,0) as like_count,
            if(e.new_like_count is not null and e.new_like_count>0,e.new_like_count,0) as new_like_count,
            if(e.video_comment_count is not null and e.video_comment_count>0,e.video_comment_count,0) as video_comment_count,
            if(e.new_video_comment_count is not null and e.new_video_comment_count>0,e.new_video_comment_count,0) as new_video_comment_count,
            if(f.challenge_video_count is not null and f.challenge_video_count>0,f.challenge_video_count,0) as challenge_video_count,
            if(f.new_challenge_video_count is not null and f.new_challenge_video_count>0,f.new_challenge_video_count,0) as new_challenge_video_count,
            if(g.user_comment_count is not null and g.user_comment_count>0,g.user_comment_count,0) as user_comment_count,
            if(g.new_user_comment_count is not null and g.new_user_comment_count>0,g.new_user_comment_count,0) as new_user_comment_count,
            if(h.talk_comment_count is not null and h.talk_comment_count>0,h.talk_comment_count,0) as talk_comment_count,
            if(h.new_talk_comment_count is not null and h.new_talk_comment_count>0,h.new_talk_comment_count,0) as new_talk_comment_count,
            if(i.commodity_count is not null and i.commodity_count>0,i.commodity_count,0) as commodity_count
    from(
        select b.resource_key,b.app_version,b.data_generate_time,b.app_package_name,b.user_name,b.user_id,b.kwai_id,b.user_share_url,b.signature,b.store_or_curriculum,b.hometown,b.sex,b.constellation,b.certification,
            b.follower_count,
            if(c.follower_count is not null and c.follower_count>0,b.follower_count-c.follower_count,b.follower_count) as new_follower_count,
            b.following_count,
            if(c.following_count is not null and c.following_count>0,b.following_count-c.following_count,b.following_count) as new_following_count,
            b.short_video_count,
            if(c.short_video_count is not null and c.short_video_count>0,b.short_video_count-c.short_video_count,b.short_video_count) as new_short_video_count,
            b.talk_count,
            if(c.talk_count is not null and c.talk_count>0,b.talk_count-c.talk_count,b.talk_count) as new_talk_count,
            b.music_count,
            if(c.music_count is not null and c.music_count>0,b.music_count-c.music_count,b.music_count) as new_music_count
        from(

            select a1.resource_key,a1.app_version,a1.data_generate_time,a1.app_package_name,a1.user_name,a1.user_id,if(a1.kwai_id is null,'',a1.kwai_id) as kwai_id,a1.user_share_url,a1.signature,if(a1.store_or_curriculum>0,a1.store_or_curriculum,a1.curriculum) as store_or_curriculum,if(a1.label3 is null,'',a1.label3) as hometown,a1.sex,a1.constellation,a1.certification,
                if(a1.follower_count<0,0,a1.follower_count) as follower_count,
                if(a1.following_count<0,0,a1.following_count) as following_count,
                if(a1.short_video_count<0,0,a1.short_video_count) as short_video_count,
                if(a1.talk_count<0,0,a1.talk_count) as talk_count,
                if(a1.music_count<0,0,a1.music_count) as music_count
            from (
                select * from bigdata.kuaishou_user_data_daily_snapshot
                where dt='${date}' AND app_package_name='${apk_name}'
                AND user_id is not null AND user_id !=''
            ) as a1
            join(
                select app_package_name,user_id
                from bigdata.kuaishou_header_user_data_orc
                where dt='${date}' AND app_package_name='${apk_name}'
            ) as a2
            on a1.app_package_name=a2.app_package_name and a1.user_id=a2.user_id

        ) as b
        left join (
            select app_package_name,user_id,
                if(follower_count<0,0,follower_count) as follower_count,
                if(following_count<0,0,following_count) as following_count,
                if(short_video_count<0,0,short_video_count) as short_video_count,
                if(talk_count<0,0,talk_count) as talk_count,
                if(music_count<0,0,music_count) as music_count
            from bigdata.kuaishou_user_data_daily_snapshot
            where dt='${yesterday}' AND app_package_name='${apk_name}'
                AND user_id is not null AND user_id !=''
        ) as c
        on b.app_package_name=c.app_package_name and b.user_id=c.user_id
    ) as d
        left join (
            select app_package_name,user_id,like_count,new_like_count,video_comment_count,new_video_comment_count from default.tmp_finance_kuaishou_user_like_count_and_video_comment_count
        ) as e
        on d.app_package_name=e.app_package_name and d.user_id=e.user_id
        left join (
            select app_package_name,user_id,challenge_video_count,new_challenge_video_count from default.tmp_finance_kuaishou_user_challenge_video_count
        ) as f
        on d.app_package_name=f.app_package_name and d.user_id=f.user_id
        left join (
            select app_package_name,user_id,user_comment_count,new_user_comment_count from default.tmp_finance_kuaishou_user_comment_count
        ) as g
        on d.app_package_name=g.app_package_name and d.user_id=g.user_id
        left join (
            select app_package_name,user_id,talk_comment_count,new_talk_comment_count from default.tmp_finance_kuaishou_user_talk_comment_count
        ) as h
        on d.app_package_name=h.app_package_name and d.user_id=h.user_id
        left join (
            select app_package_name,user_id,commodity_count from default.tmp_finance_kuaishou_user_commodity_count
        ) as i
        on d.app_package_name=i.app_package_name and d.user_id=i.user_id;
    "
    echo "${tmp_finance_kuaishou_user_t_data}"
    echo "#####################################################################"

    echo "##################### 导出头部集用户数据到hive #############################"
    save_finance_kuaishou_user_t_data_to_hive="
    insert into bigdata.kuaishou_user_r_t_data partition(dt='${date}')
    select 'T_USER' as set_type,'kauishou' as  meta_app_name, 'kuaishou_user_r_t_data' as meta_table_name,resource_key,app_version,app_package_name,data_generate_time,
        user_name,user_id,kwai_id,user_share_url,signature,store_or_curriculum,hometown,sex,constellation,certification,follower_count,new_follower_count,
        following_count,new_following_count,short_video_count,new_short_video_count,talk_count,new_talk_count,music_count,new_music_count,like_count,new_like_count,
        video_comment_count,new_video_comment_count,challenge_video_count,new_challenge_video_count,user_comment_count,new_user_comment_count,
        talk_comment_count,new_talk_comment_count,commodity_count,'${stat_date}' as extract_date
    from default.tmp_finance_kuaishou_user_t_data;
    "
    echo "${save_finance_kuaishou_user_t_data_to_hive}"
    echo "###################################################################"


    echo "##################### 整合当月抽样集用户数据 #############################"
    tmp_finance_kuaishou_user_r_now_data="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_user_r_now_data AS
    select d.resource_key,d.app_version,d.data_generate_time,d.app_package_name,d.user_name,d.user_id,d.kwai_id,d.user_share_url,d.signature,d.store_or_curriculum,d.hometown,d.sex,d.constellation,d.certification,
            d.follower_count,d.new_follower_count,
            d.following_count,d.new_following_count,
            d.short_video_count,d.new_short_video_count,
            d.talk_count,d.new_talk_count,
            d.music_count,d.new_music_count,
            if(e.like_count is not null and e.like_count>0,e.like_count,0) as like_count,
            if(e.new_like_count is not null and e.new_like_count>0,e.new_like_count,0) as new_like_count,
            if(e.video_comment_count is not null and e.video_comment_count>0,e.video_comment_count,0) as video_comment_count,
            if(e.new_video_comment_count is not null and e.new_video_comment_count>0,e.new_video_comment_count,0) as new_video_comment_count,
            if(f.challenge_video_count is not null and f.challenge_video_count>0,f.challenge_video_count,0) as challenge_video_count,
            if(f.new_challenge_video_count is not null and f.new_challenge_video_count>0,f.new_challenge_video_count,0) as new_challenge_video_count,
            if(g.user_comment_count is not null and g.user_comment_count>0,g.user_comment_count,0) as user_comment_count,
            if(g.new_user_comment_count is not null and g.new_user_comment_count>0,g.new_user_comment_count,0) as new_user_comment_count,
            if(h.talk_comment_count is not null and h.talk_comment_count>0,h.talk_comment_count,0) as talk_comment_count,
            if(h.new_talk_comment_count is not null and h.new_talk_comment_count>0,h.new_talk_comment_count,0) as new_talk_comment_count,
            if(i.commodity_count is not null and i.commodity_count>0,i.commodity_count,0) as commodity_count
    from(
        select b.resource_key,b.app_version,b.data_generate_time,b.app_package_name,b.user_name,b.user_id,b.kwai_id,b.user_share_url,b.signature,b.store_or_curriculum,b.hometown,b.sex,b.constellation,b.certification,
            b.follower_count,
            if(c.follower_count is not null and c.follower_count>0,b.follower_count-c.follower_count,b.follower_count) as new_follower_count,
            b.following_count,
            if(c.following_count is not null and c.following_count>0,b.following_count-c.following_count,b.following_count) as new_following_count,
            b.short_video_count,
            if(c.short_video_count is not null and c.short_video_count>0,b.short_video_count-c.short_video_count,b.short_video_count) as new_short_video_count,
            b.talk_count,
            if(c.talk_count is not null and c.talk_count>0,b.talk_count-c.talk_count,b.talk_count) as new_talk_count,
            b.music_count,
            if(c.music_count is not null and c.music_count>0,b.music_count-c.music_count,b.music_count) as new_music_count
        from(

            select a1.resource_key,a1.app_version,a1.data_generate_time,a1.app_package_name,a1.user_name,a1.user_id,if(a1.kwai_id is null,'',a1.kwai_id) as kwai_id,a1.user_share_url,a1.signature,if(a1.store_or_curriculum>0,a1.store_or_curriculum,a1.curriculum) as store_or_curriculum,if(a1.label3 is null,'',a1.label3) as hometown,a1.sex,a1.constellation,a1.certification,
                if(a1.follower_count<0,0,a1.follower_count) as follower_count,
                if(a1.following_count<0,0,a1.following_count) as following_count,
                if(a1.short_video_count<0,0,a1.short_video_count) as short_video_count,
                if(a1.talk_count<0,0,a1.talk_count) as talk_count,
                if(a1.music_count<0,0,a1.music_count) as music_count
            from (
                select * from bigdata.kuaishou_user_data_daily_snapshot
                where dt='${date}' AND app_package_name='${apk_name}'
                AND user_id is not null AND user_id !=''
            ) as a1
            join(
                select app_package_name,user_id
                from bigdata.kuaishou_sampling_user_data_orc
                where dt='${month1_01}' AND app_package_name='${apk_name}'
            ) as a2
            on a1.app_package_name=a2.app_package_name and a1.user_id=a2.user_id

        ) as b
        left join (
            select app_package_name,user_id,
                if(follower_count<0,0,follower_count) as follower_count,
                if(following_count<0,0,following_count) as following_count,
                if(short_video_count<0,0,short_video_count) as short_video_count,
                if(talk_count<0,0,talk_count) as talk_count,
                if(music_count<0,0,music_count) as music_count
            from bigdata.kuaishou_user_data_daily_snapshot
            where dt='${yesterday}' AND app_package_name='${apk_name}'
                AND user_id is not null AND user_id !=''
        ) as c
        on b.app_package_name=c.app_package_name and b.user_id=c.user_id
    ) as d
        left join (
            select app_package_name,user_id,like_count,new_like_count,video_comment_count,new_video_comment_count from default.tmp_finance_kuaishou_user_like_count_and_video_comment_count
        ) as e
        on d.app_package_name=e.app_package_name and d.user_id=e.user_id
        left join (
            select app_package_name,user_id,challenge_video_count,new_challenge_video_count from default.tmp_finance_kuaishou_user_challenge_video_count
        ) as f
        on d.app_package_name=f.app_package_name and d.user_id=f.user_id
        left join (
            select app_package_name,user_id,user_comment_count,new_user_comment_count from default.tmp_finance_kuaishou_user_comment_count
        ) as g
        on d.app_package_name=g.app_package_name and d.user_id=g.user_id
        left join (
            select app_package_name,user_id,talk_comment_count,new_talk_comment_count from default.tmp_finance_kuaishou_user_talk_comment_count
        ) as h
        on d.app_package_name=h.app_package_name and d.user_id=h.user_id
        left join (
            select app_package_name,user_id,commodity_count from default.tmp_finance_kuaishou_user_commodity_count
        ) as i
        on d.app_package_name=i.app_package_name and d.user_id=i.user_id;
    "
    echo "${tmp_finance_kuaishou_user_r_now_data}"
    echo "#####################################################################"

    echo "##################### 导出当月抽样集用户数据到hive #############################"
    save_finance_kuaishou_user_r_now_data_to_hive="
    insert into bigdata.kuaishou_user_r_t_data partition(dt='${date}')
    select 'R_USER' as set_type,'kauishou' as  meta_app_name, 'kuaishou_user_r_t_data' as meta_table_name,resource_key,app_version,app_package_name,data_generate_time,
        user_name,user_id,kwai_id,user_share_url,signature,store_or_curriculum,hometown,sex,constellation,certification,follower_count,new_follower_count,
        following_count,new_following_count,short_video_count,new_short_video_count,talk_count,new_talk_count,music_count,new_music_count,like_count,new_like_count,
        video_comment_count,new_video_comment_count,challenge_video_count,new_challenge_video_count,user_comment_count,new_user_comment_count,
        talk_comment_count,new_talk_comment_count,commodity_count,'${month1_01_1}' as extract_date
    from default.tmp_finance_kuaishou_user_r_now_data;
    "
    echo "${save_finance_kuaishou_user_r_now_data_to_hive}"
    echo "###################################################################"
    
    echo "##################### 整合上月抽样集用户数据 #############################"
    tmp_finance_kuaishou_user_r_old_data="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_user_r_old_data AS
    select d.resource_key,d.app_version,d.data_generate_time,d.app_package_name,d.user_name,d.user_id,d.kwai_id,d.user_share_url,d.signature,d.store_or_curriculum,d.hometown,d.sex,d.constellation,d.certification,
            d.follower_count,d.new_follower_count,
            d.following_count,d.new_following_count,
            d.short_video_count,d.new_short_video_count,
            d.talk_count,d.new_talk_count,
            d.music_count,d.new_music_count,
            if(e.like_count is not null and e.like_count>0,e.like_count,0) as like_count,
            if(e.new_like_count is not null and e.new_like_count>0,e.new_like_count,0) as new_like_count,
            if(e.video_comment_count is not null and e.video_comment_count>0,e.video_comment_count,0) as video_comment_count,
            if(e.new_video_comment_count is not null and e.new_video_comment_count>0,e.new_video_comment_count,0) as new_video_comment_count,
            if(f.challenge_video_count is not null and f.challenge_video_count>0,f.challenge_video_count,0) as challenge_video_count,
            if(f.new_challenge_video_count is not null and f.new_challenge_video_count>0,f.new_challenge_video_count,0) as new_challenge_video_count,
            if(g.user_comment_count is not null and g.user_comment_count>0,g.user_comment_count,0) as user_comment_count,
            if(g.new_user_comment_count is not null and g.new_user_comment_count>0,g.new_user_comment_count,0) as new_user_comment_count,
            if(h.talk_comment_count is not null and h.talk_comment_count>0,h.talk_comment_count,0) as talk_comment_count,
            if(h.new_talk_comment_count is not null and h.new_talk_comment_count>0,h.new_talk_comment_count,0) as new_talk_comment_count,
            if(i.commodity_count is not null and i.commodity_count>0,i.commodity_count,0) as commodity_count
    from(
        select b.resource_key,b.app_version,b.data_generate_time,b.app_package_name,b.user_name,b.user_id,b.kwai_id,b.user_share_url,b.signature,b.store_or_curriculum,b.hometown,b.sex,b.constellation,b.certification,
            b.follower_count,
            if(c.follower_count is not null and c.follower_count>0,b.follower_count-c.follower_count,b.follower_count) as new_follower_count,
            b.following_count,
            if(c.following_count is not null and c.following_count>0,b.following_count-c.following_count,b.following_count) as new_following_count,
            b.short_video_count,
            if(c.short_video_count is not null and c.short_video_count>0,b.short_video_count-c.short_video_count,b.short_video_count) as new_short_video_count,
            b.talk_count,
            if(c.talk_count is not null and c.talk_count>0,b.talk_count-c.talk_count,b.talk_count) as new_talk_count,
            b.music_count,
            if(c.music_count is not null and c.music_count>0,b.music_count-c.music_count,b.music_count) as new_music_count
        from(

            select a1.resource_key,a1.app_version,a1.data_generate_time,a1.app_package_name,a1.user_name,a1.user_id,if(a1.kwai_id is null,'',a1.kwai_id) as kwai_id,a1.user_share_url,a1.signature,if(a1.store_or_curriculum>0,a1.store_or_curriculum,a1.curriculum) as store_or_curriculum,if(a1.label3 is null,'',a1.label3) as hometown,a1.sex,a1.constellation,a1.certification,
                if(a1.follower_count<0,0,a1.follower_count) as follower_count,
                if(a1.following_count<0,0,a1.following_count) as following_count,
                if(a1.short_video_count<0,0,a1.short_video_count) as short_video_count,
                if(a1.talk_count<0,0,a1.talk_count) as talk_count,
                if(a1.music_count<0,0,a1.music_count) as music_count
            from (
                select * from bigdata.kuaishou_user_data_daily_snapshot
                where dt='${date}' AND app_package_name='${apk_name}'
                AND user_id is not null AND user_id !=''
            ) as a1
            join(
                select app_package_name,user_id
                from bigdata.kuaishou_sampling_user_data_orc
                where dt='${month2_01}' AND app_package_name='${apk_name}'
            ) as a2
            on a1.app_package_name=a2.app_package_name and a1.user_id=a2.user_id

        ) as b
        left join (
            select app_package_name,user_id,
                if(follower_count<0,0,follower_count) as follower_count,
                if(following_count<0,0,following_count) as following_count,
                if(short_video_count<0,0,short_video_count) as short_video_count,
                if(talk_count<0,0,talk_count) as talk_count,
                if(music_count<0,0,music_count) as music_count
            from bigdata.kuaishou_user_data_daily_snapshot
            where dt='${yesterday}' AND app_package_name='${apk_name}'
                AND user_id is not null AND user_id !=''
        ) as c
        on b.app_package_name=c.app_package_name and b.user_id=c.user_id
    ) as d
        left join (
            select app_package_name,user_id,like_count,new_like_count,video_comment_count,new_video_comment_count from default.tmp_finance_kuaishou_user_like_count_and_video_comment_count
        ) as e
        on d.app_package_name=e.app_package_name and d.user_id=e.user_id
        left join (
            select app_package_name,user_id,challenge_video_count,new_challenge_video_count from default.tmp_finance_kuaishou_user_challenge_video_count
        ) as f
        on d.app_package_name=f.app_package_name and d.user_id=f.user_id
        left join (
            select app_package_name,user_id,user_comment_count,new_user_comment_count from default.tmp_finance_kuaishou_user_comment_count
        ) as g
        on d.app_package_name=g.app_package_name and d.user_id=g.user_id
        left join (
            select app_package_name,user_id,talk_comment_count,new_talk_comment_count from default.tmp_finance_kuaishou_user_talk_comment_count
        ) as h
        on d.app_package_name=h.app_package_name and d.user_id=h.user_id
        left join (
            select app_package_name,user_id,commodity_count from default.tmp_finance_kuaishou_user_commodity_count
        ) as i
        on d.app_package_name=i.app_package_name and d.user_id=i.user_id;
    "
    echo "${tmp_finance_kuaishou_user_r_old_data}"
    echo "#####################################################################"

    echo "##################### 导出上月抽样集用户数据到hive #############################"
    save_finance_kuaishou_user_r_old_data_to_hive="
    insert into bigdata.kuaishou_user_r_t_data partition(dt='${date}')
    select 'R_USER' as set_type,'kauishou' as  meta_app_name, 'kuaishou_user_r_t_data' as meta_table_name,resource_key,app_version,app_package_name,data_generate_time,
        user_name,user_id,kwai_id,user_share_url,signature,store_or_curriculum,hometown,sex,constellation,certification,follower_count,new_follower_count,
        following_count,new_following_count,short_video_count,new_short_video_count,talk_count,new_talk_count,music_count,new_music_count,like_count,new_like_count,
        video_comment_count,new_video_comment_count,challenge_video_count,new_challenge_video_count,user_comment_count,new_user_comment_count,
        talk_comment_count,new_talk_comment_count,commodity_count,'${month2_01_1}' as extract_date
    from default.tmp_finance_kuaishou_user_r_old_data;
    "
    echo "${save_finance_kuaishou_user_r_old_data_to_hive}"
    echo "###################################################################"



    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_user_r_t_data DROP IF EXISTS PARTITION (dt='${date}');
    "
    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_user_r_t_data/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${tmp_finance_kuaishou_user_like_count_and_video_comment_count}
    ${tmp_finance_kuaishou_user_challenge_video_count}
    ${tmp_finance_kuaishou_user_comment_count_subset_1}
    ${tmp_finance_kuaishou_user_comment_count_subset_2}
    ${tmp_finance_kuaishou_user_comment_count}
    ${tmp_finance_kuaishou_user_talk_comment_count}
    ${tmp_finance_kuaishou_user_commodity_count}
    ${tmp_finance_kuaishou_user_t_data}
    ${save_finance_kuaishou_user_t_data_to_hive}

    ${tmp_finance_kuaishou_user_r_now_data}
    ${save_finance_kuaishou_user_r_now_data_to_hive}

    ${tmp_finance_kuaishou_user_r_old_data}
    ${save_finance_kuaishou_user_r_old_data_to_hive}
    "




