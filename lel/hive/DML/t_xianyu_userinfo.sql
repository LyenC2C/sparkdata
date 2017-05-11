
insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_userinfo PARTITION(ds='20160721')
select
userid,totalcount,gender,
idleuserid, nick, tradecount,
tradeincome, usernick, constellation,
birthday, province,null,null,'1469030400'
from wlbase_dev.t_base_ec_tb_xianyu_userinfo_old

load data inpath "/user/lel/temp/xianyu_userinfo" into table t_base_ec_xianyu_userinfo partition(ds=20170109);


insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_userinfo PARTITION(ds='20160721_old')
select
case when userid ='-' then '\\N' else userid end ,
case when totalcount ='-' then '\\N' else totalcount end,
case when gender ='-' then '\\N' else gender end,
case when idleuserid ='-' then '\\N' else idleuserid end,
case when nick ='-' then '\\N' else nick end,
case when tradecount ='-' then '\\N' else tradecount end,
case when tradeincome ='-' then '\\N' else tradeincome end,
case when usernick ='-' then '\\N' else usernick end,
case when constellation ='-' then '\\N'else constellation end,
case when birthday ='-' then '\\N' else birthday end,
case when city ='-' then '\\N'else city end,
case when infopercent ='-' then '\\N'else infopercent end,
case when signature ='-' then '\\N'else signature end,
ts
from wlbase_dev.t_base_ec_xianyu_userinfo where ds = '20160721'

insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_userinfo PARTITION(ds='20170110')
select
case when t1.usernick is null then t2.userid else t1.userid end,
case when t1.usernick is null then t2.totalcount else t1.totalcount end,
case when t1.usernick is null then t2.gender else t1.gender end ,
case when t1.usernick is null then t2.idleuserid else t1.idleuserid end,
case when t1.usernick is null then t2.nick else t1.nick end,
case when t1.usernick is null then t2.tradecount else t1.tradecount end ,
case when t1.usernick is null then t2.tradeincome else t1.tradeincome end,
case when t1.usernick is null then t2.usernick else t1.usernick end,
case when t1.usernick is null then t2.constellation else t1.constellation end,
case when t1.usernick is null then t2.birthday else t1.birthday end,
case when t1.usernick is null then t2.city else t1.city end,
case when t1.usernick is null then t2.infopercent else t1.infopercent end,
case when t1.usernick is null then t2.signature else t1.signature end,
case when t1.usernick is null then t2.ts else t1.ts end
from
(select * fromt_base_ec_xianyu_userinfo where ds = 20170109) t1
full  join
(select * from t_base_ec_xianyu_userinfo  where ds = 20160721) t2
on t1.usernick = t2.usernick


                        nick            usernick
2023452078	0	0	0	王胖爺	0	0	d]司徒大少爷	双鱼	1996-03-16	120100	NULL	NULL	1469030400	20170110
                        nick            usernick
0	0	NULL	0	司徒大少爷	0	0	司徒大少爷	NULL	NULL	NULL	0	NULL	1483050426	20170110

数据有问题,nick为闲鱼名(可更改),usernick为淘宝会员名(恒定不变),通过t_base_user_profile 淘宝会员名交叉验证,此处的usernick不一样属于脏数据,暂且不管.


--(select userid,totalcount,gender,
--        idleuserid, nick, tradecount,
--        tradeincome, usernick, constellation,
--        birthday, city,infopercent,signature,ts from t_base_ec_xianyu_userinfo where ds = 20170109) t1
--full  join
--(select userid,totalcount,gender,
--        idleuserid, nick, tradecount,
--        tradeincome, usernick, constellation,
--        birthday, city,infopercent,signature,ts from t_base_ec_xianyu_userinfo where ds = 20170109) t2
--on t1.usernick = t2.usernick

#关联t_base_user_profile的tb_nick和t_base_ec_xianyu_userinfo的usernick填补缺失的userid

insert overwrite table t_base_ec_xianyu_userinfo partition(ds = '20170110_userid_makeup')
select
distinct
case when t1.userid = '0' and t2.tb_nick is not null then t2.tb_id else t1.userid end as userid,
t1.totalcount,t1.gender,
t1.idleuserid, t1.nick, t1.tradecount,
t1.tradeincome, t1.usernick, t1.constellation,
t1.birthday,t1.city,t1.infopercent,t1.signature,t1.ts
from
(select * from t_base_ec_xianyu_userinfo where ds = '20170110') t1
left join
(select tb_id,tb_nick from t_base_user_profile) t2
on t1.usernick = t2.tb_nick



#关联t_base_ec_xianyu_userinfo和t_zlj_zhima1223_userinfo获取生日日期

create table if not exists t_zlj_zhima20170110_userinfo
as
select t1.*,t2.birthday as birthday from
(
select * from t_zlj_zhima1223_userinfo
) t1
join
(
select
userid,birthday
from wlbase_dev.t_base_ec_xianyu_userinfo
where ds='20170110_userid_makeup' and cast(split(birthday,"-")[0] as int) > 1950
) t2
on t1.tb_id = t2.userid




