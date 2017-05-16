shopitem_b:
20161001 20160905
20161101 20161001
20161201 20161101
20170101 20161201
20170204 20170101
20170301 20170204
20170401 20170301
20170501 20170401

----------------------------------------------苹果
drop table wl_service.t_lel_talkingdata_ec_test_tmp;
create table wl_service.t_lel_talkingdata_ec_test_tmp
as
select shop_id,brand_name,item_id from wl_base.t_base_ec_record_dev_new
    where ds='true' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649|71955116|108837944|69184836|57301343|104480132|62147762|108837944|69184836|57301343|60790435|106096685|104417480|108225379|104439364|103516837|58499649'
    and brand_name  regexp 'Apple/苹果|Nike/耐克|Adidas/阿迪达斯|ANTA/安踏';

data = sqlContext.sql("select shop_id,brand_name,item_id from wl_base.t_base_ec_record_dev_new where ds='true' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649|71955116|108837944|69184836|57301343|104480132|62147762|108837944|69184836|57301343|60790435|106096685|104417480|108225379|104439364|103516837|58499649' and brand_name  regexp 'Apple/苹果|Nike/耐克|Adidas/阿迪达斯|ANTA/安踏'")
data.write.saveAsTable("wl_service.t_lel_talkingdata_ec_test_tmp",mode="overwrite")

create table wl_service.t_lel_talkingdata_ec_test_apple
as
select
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201609" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20161001' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20160905' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end
union all
select
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
'201610' as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20161101' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20161001' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end
union all
select
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201611" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20161201' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20161101' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end
union all
select
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201612" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170101' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20161201' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end
union all
select
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201701" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170204' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170101' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end
union all
select
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201702" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170301' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170204' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end
union all
select
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201703" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170401' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170301' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end
union all
select
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201704" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170501' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170401' and shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649'
    and sold is not null and saleprice is not null
    and b.item_id in (select cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '106117073' then '中国移动官方旗舰店'
when t1.shop_id = '107922698' then 'Apple Store官方旗舰店'
when t1.shop_id = '72370856' then '浙江移动官方旗舰店'
when t1.shop_id = '68412245' then '广东联通官方旗舰店'
when t1.shop_id = '103516837' then '广西移动官方旗舰店'
when t1.shop_id = '104951423' then '四川联通官方旗舰店'
when t1.shop_id = '58613162' then '三际数码官方旗舰店'
when t1.shop_id = '57303636' then '绿森数码官方旗舰店'
when t1.shop_id = '128573071' then '苏宁易购官方旗舰店'
when t1.shop_id = '58499649' then '能良数码官方旗舰店'
end
----------------------------------nike


drop table wl_service.t_lel_talkingdata_ec_test_nike;
create table wl_service.t_lel_talkingdata_ec_test_nike
as
select
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201609" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20161001' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20160905' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end
union all
select
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
'201610' as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20161101' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20161001' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end
union all
select
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201611" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20161201' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20161101' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end
union all
select
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201612" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170101' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20161201' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end
union all
select
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201701" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170204' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170101' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end
union all
select
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201702" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170301' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170204' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end
union all
select
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201703" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170401' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170301' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end
union all
select
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201704" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170501' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170401' and shop_id regexp '71955116|108837944|69184836|57301343|104480132'
    and sold is not null and saleprice is not null
    and b.item_id in (select cast(item_id as string) as item_id from wl_base.t_base_ec_record_dev_new where  shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克'  )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '71955116' then 'Nike官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
when t1.shop_id = '104480132' then '顶护运动专营店'
end


-----------------------------------------adidas

create table wl_service.t_lel_talkingdata_ec_test_adidas
as
select
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201609" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20161001' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20160905' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end
union all
select
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
'201610' as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20161101' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20161001' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end
union all
select
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201611" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20161201' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20161101' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end
union all
select
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201612" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170101' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20161201' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end
union all
select
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201701" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170204' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170101' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end
union all
select
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201702" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170301' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170204' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end
union all
select
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201703" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170401' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170301' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end
union all
select
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201704" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170501' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170401' and shop_id regexp '62147762|108837944|69184836|57301343'
    and sold is not null and saleprice is not null
    and b.item_id in (select cast(item_id as string) as item_id from wl_base.t_base_ec_record_dev_new where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯'  )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '62147762' then 'Adidas官方旗舰店'
when t1.shop_id = '108837944' then '幸运叶子官方旗舰店'
when t1.shop_id = '69184836' then '风驰运动专营店'
when t1.shop_id = '57301343' then '开心购物专营店'
end
-----------------------------anta

create table wl_service.t_lel_talkingdata_ec_test_anta
as
select
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201609" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20161001' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )
    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20160905' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end
union all
select
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
'201610' as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20161101' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20161001' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end
union all
select
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201611" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20161201' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20161101' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end
union all
select
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201612" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170101' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20161201' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end
union all
select
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201701" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170204' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170101' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end
union all
select
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201702" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170301' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170204' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end
union all
select
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201703" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170401' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170301' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and b.item_id in (select  cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end
union all
select
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end as shop,
sum(t1.sold - t2.sold) as totalsold,
sum((t1.sold - t2.sold) * t1.saleprice) as totalincome,
"201704" as m
from
(select a.item_id,a.shop_id,a.sold,a.saleprice from wl_base.t_base_ec_shopitem_b a
    where ds = '20170501' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and a.item_id in (select   cast(item_id as string) as itemid from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏' )

    ) t1
join
(select b.item_id,b.shop_id,b.sold,b.saleprice from wl_base.t_base_ec_shopitem_b b
    where ds = '20170401' and shop_id regexp '60790435|106096685|104417480|108225379|104439364'
    and sold is not null and saleprice is not null
    and b.item_id in (select cast(item_id as string) as item_id from wl_base.t_base_ec_record_dev_new where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏'  )
    ) t2
on
t1.item_id = t2.item_id
group by
case when t1.shop_id = '60790435' then '安踏官方网店'
when t1.shop_id = '106096685' then 'anta安踏童装旗舰店'
when t1.shop_id = '104417480' then 'anta安踏驰尚专卖店'
when t1.shop_id = '108225379' then '安踏安大专卖店'
when t1.shop_id = '104439364' then 'anta安踏安志专卖店'
end


select cast(item_id as string) as item_id from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '106117073|107922698|72370856|68412245|103516837|104951423|58613162|57303636|128573071|58499649' and brand_name  regexp '苹果'
select cast(item_id as string) as item_id from wl_service.t_lel_talkingdata_ec_test_tmp where shop_id regexp '71955116|108837944|69184836|57301343|104480132' and brand_name  regexp 'Nike/耐克'
select cast(item_id as string) as item_id from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '62147762|108837944|69184836|57301343' and brand_name  regexp 'Adidas/阿迪达斯'
select cast(item_id as string) as item_id from wl_service.t_lel_talkingdata_ec_test_tmp where  shop_id regexp '60790435|106096685|104417480|108225379|104439364' and brand_name  regexp 'ANTA/安踏'