



1.商品 
select count(1) from t_base_ec_xianyu_iteminfo where ds=20170103

count: 	81879807
2.用户

select count(1) from 
(select userid
from wlbase_dev.t_base_ec_xianyu_iteminfo 
where ds=20161218 
and userid is not null 
and gps is not null group by userid) t

count: 	17451075

3.地址(鱼塘名即鱼塘所在地)

select count(1) from 
(select userid,bar
from wlbase_dev.t_base_ec_xianyu_iteminfo 
where ds=20161218 
and userid is not null
and bar is not null
group by userid,bar) t

count: 15746260
4.日增量
count: 5-6 百万


select tel,id,verify,zhima from
(select t1.tel_index as tel ,t1.tb_id as id,t1.verify as verify ,t2.zhima as zhima,row_number() over (partition by t1.verify order by t1.rand desc ) as rn
from
(select 
tel_index,tb_id,verify,rand() as rand
from wlbase_dev.t_base_user_profile_telindex
where tel_index <> '-' and verify <> '-'
group by tel_index,tb_id,verify
) t1
join
(select userid,zhima
from 
wlbase_dev.t_base_ec_xianyu_iteminfo
where ds='20161218' and zhima='1'
group by userid,zhima
) t2
on t1.tb_id = t2.userid) re
where re.rn <= 10000


create table if not exists wlservice.t_base_ec_xianyu_idlocations as
select userid,province,city,concat_ws(',',collect_set(fishpoolname)) as locations
from wlbase_dev.t_base_ec_xianyu_iteminfo
where ds=20161218 and fishpoolname is not null and group by userid,province,city

create table lel.wc_2 as 
	select w.word as word,count(1) as count from
	 (select explode(split(id,' ')) as word from lel.wc_text) w group by word order by count desc)

create table if not exists wlcredit.t_lel_credit_feed as
select
transform(item_id,feed_id,f_date,content)
using 'python lel_udf.py'
as (item_id,feed_id,f_date,content,flag_1, kw_1, flag_2, kw_2,fraud_score,keywords)
from
(select
item_id,feed_id,f_date,content
from
wlbase_dev.t_base_ec_item_feed_dev_new
) t

select 
t1.itemid from
(select itemid,commentnum 
	from wlbase_dev.t_base_ec_xianyu_iteminfo 
	 where ds='20161222' 
	  and commentnum > 0 ) t1
left join 
(select itemid,commentnum 
	from wlbase_dev.t_base_ec_xianyu_iteminfo 
	 where ds='20161221' 
	  and commentnum > 0 ) t2
on t1.itemid = t2.itemid
where t1.commentnum > t2.commentnum or t2.item_id is null

insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_iteminfo PARTITION(ds = '20161222')
select
t2.itemid,
t2.userid,
t2.phone,
t2.contacts,
t2.title,
t2.province,
t2.city,
t2.area,
t2.auctionType,
t2.description,
t2.detailFrom,
t2.favorNum,
t2.commentNum,
t2.firstModified,
t2.firstModifiedDiff,
t2.t_from,
t2.gps,
t2.offline,
t2.originalPrice,
t2.price,
t2.postprice,
t2.userNick,
case when t2.categoryid is null then t1.categoryid else t2.categoryid end,
case when t2.categoryName is null then t1.categoryName else t2.categoryName end,
t2.fishPoolid,
t2.fishpoolName,
t2.bar,
t2.barInfo,
t2.abbr,
t2.zhima,
t2.shiren,
t2.ts
from
t_base_ec_xianyu_iteminfo_cname_notnull  t1
right join
(select * from t_base_ec_xianyu_iteminfo where ds ='20161222_temp') t2
on t1.categoryid = t2.categoryid