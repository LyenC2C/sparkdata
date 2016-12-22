insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_iteminfo PARTITION(ds='20160721_old')
select
id,userId,null,null,title,province,city,area,auctiontype,description,detailFrom,favorNum,
commentNum,firstModified,null,t_from,gps,offline,originalPrice,price,null,userNick,categoryId,
null,null,community_name,community_name,null,null,null,null,null
from wlbase_dev.t_base_ec_tb_xianyu_item_old

load data inpath "/user/lel/result" into table t_lel_ec_xianyu_item_info;

spark-submit --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 ~/wolong/sparkdata/spark/t_xianyu.py



insert OVERWRITE table wlbase_dev.t_base_ec_xianyu_iteminfo PARTITION(ds='20160721_old')
select
case when itemid ='-' then '\\N' else itemid end ,
case when userId ='-' then '\\N' else userId end,
null,
null,
case when title ='-' then '\\N' else title end,
case when province ='-' then '\\N' else province end,
case when city ='-' then '\\N' else city end,
case when area ='-' then '\\N' else area end,
case when auctiontype ='-' then '\\N' else auctiontype end,
case when description ='-' then '\\N' else description end,
case when detailFrom ='-' then '\\N'else detailFrom end,
case when favorNum ='-' then 0 else favorNum end,
case when commentNum ='-' then 0 else commentNum end,
case when firstModified ='-' then '\\N' else firstModified end,
firstModifieddiff,
case when t_from ='-' then '\\N' else t_from end,
case when gps ='-' then '\\N' else gps end,
case when offline ='-' then '\\N'  else offline end,
case when originalPrice ='-' then 0.0 else originalPrice end,
case when price ='-' then 0.0 else price end,
0.0,
case when userNick ='-' then '\\N' else userNick end,
case when categoryId ='-' then '\\N'  else categoryId end,
case when categoryname ='-' then '\\N' else categoryname end,
fishpoolid,
fishpoolname,bar,
case when barinfo ='-' then '\\N'  else categoryname end,
case when abbr ='-' then '\\N' else categoryname end,
zhima,shiren,ts
from wlbase_dev.t_base_ec_xianyu_iteminfo_old where ds = '20160721_old'


