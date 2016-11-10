
CREATE  TABLE  if not exists t_base_weibo_user(

id bigint COMMENT ' 用户UID ' ,
idstr string COMMENT ' 字符串型的用户UID ',
screen_name string COMMENT ' 用户昵称 ',
name string COMMENT ' 友好显示名称 ',
province int COMMENT ' 用户所在省级ID ',
city int COMMENT ' 用户所在城市ID ',
location string COMMENT ' 用户所在地 ',
description string COMMENT ' 用户个人描述 ',
url string COMMENT ' 用户博客地址 ',
profile_image_url string COMMENT ' 用户头像地址（中图），50×50像素 ',
profile_url string COMMENT ' 用户的微博统一URL地址 ',
domain string COMMENT ' 用户的个性化域名 ',
weihao string COMMENT ' 用户的微号 ',
gender string COMMENT ' 性别，m：男、f：女、n：未知 ',
followers_count int COMMENT ' 粉丝数 ',
friends_count int COMMENT ' 关注数 ',
statuses_count int COMMENT ' 微博数 ',
favourites_count int COMMENT ' 收藏数 ',
created_at string COMMENT ' 用户创建（注册）时间 ',
wfollowing string COMMENT ' 暂未支持 ',
allow_all_act_msg string COMMENT ' 是否允许所有人给我发私信，true：是，false：否 ',
geo_enabled string COMMENT ' 是否允许标识用户的地理位置，true：是，false：否 ',
verified string COMMENT ' 是否是微博认证用户，即加V用户，true：是，false：否 ',
verified_type int COMMENT ' 暂未支持 ',
remark string COMMENT ' 用户备注信息，只有在查询用户关系时才返回此字段 ',
status string COMMENT  '',
allow_all_comment boolean COMMENT ' 是否允许所有人对我的微博进行评论，true：是，false：否 ',
avatar_large string COMMENT ' 用户头像地址（大图），180×180像素 ',
avatar_hd string COMMENT ' 用户头像地址（高清），高清头像原图 ',
verified_reason string COMMENT ' 认证原因 ',
follow_me string COMMENT ' 该用户是否关注当前登录用户，true：是，false：否 ',
online_status int COMMENT ' 用户的在线状态，0：不在线、1：在线 ',
bi_followers_count int COMMENT ' 用户的互粉数 ',
lang string COMMENT ' 用户当前的语言版本，zh-cn：简体中文，zh-tw：繁体中文，en：英语 '
)
COMMENT '微博用户信息'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;




drop TABLE  t_zlj_tmp;
create table t_zlj_tmp as
select * from (
SELECT id from t_base_weibo_user
UNION  ALL
select id from t_base_weibo_user_fri
)t group by id ;

4.1亿
select count(1) from t_base_weibo_user;



select cast( log2(followers_count) as int )  ,count(1) from  t_base_weibo_user    group by  cast( log2(followers_count) as int )
select cast( log2(statuses_count) as int )  ,count(1) from  t_base_weibo_user    group by  cast( log2(statuses_count) as int )

select cast( log2(friends_count) as int )  ,count(1) from  t_base_weibo_user    group by  cast( log2(friends_count) as int )

select cast( log2(bi_followers_count) as int )  ,count(1) from  t_base_weibo_user    group by  cast( log2(bi_followers_count) as int )

-- SELECT bi_followers_count from  t_base_weibo_user limit 10

SELECT  verified ,COUNT(1) from t_base_weibo_user group by verified ;
False   416137495
True    2226004


SELECT  gender ,COUNT(1) from t_base_weibo_user group by gender ;
f       169665638
m       248697861

SELECT  geo_enabled ,COUNT(1) from t_base_weibo_user group by geo_enabled ;
False   7057849
True    411305650

SELECT  geo_enabled ,COUNT(1)  from t_base_weibo_user group by geo_enabled ;

