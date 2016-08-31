DROP table  t_zlj_tmp

DROP table  t_zlj_tmp  ;

create table  t_zlj_tmp  as
SELECT
user_id ,brand_name
from
t_base_ec_record_dev_new where

   brand_name
in (
'沙驰',
'迪柯尼',
'维克多',
'堡尼',
'雅戈尔'
	)
and cat_id in (
50024769,
50024768,
50017690,
50017692,
50017689,
50017688,
50017691,
50017693,
50003708,
50023245,
124246015,
50011130,
50010160
);



LOAD DATA  local  INPATH '/home/zlj/data_tmp' OVERWRITE INTO TABLE t_zlj_feed_tmp PARTITION (ds='0826') ;


SELECT

item_id      ,
item_title   ,
feed_id      ,
t1.id1 as user_id        ,
content      ,
f_date       ,
annoy        ,
ts           ,
sku          ,
rate_type    ,
crawl_type   ,
mark

from t_base_uid_tmp   t1 join t_zlj_feed_tmp t2 on t1.uid=t2.mark  where t1.ds='uid_mark' and t2.ds='uid_mark'



DROP  table wlservice.t_zlj_tmp_record;
create table wlservice.t_zlj_tmp_record as
SELECT
      t2.*,
      t1.title,
      cat_id,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name,
      bc_type,
      price,
      shop_id,
      location,
      case when cat_id is null then 'wrong' else 'right' end as ds
    FROM (SELECT
            cast(item_id AS BIGINT) item_id,
            title,
            cat_id,
            root_cat_id,
            root_cat_name,
            brand_id,
            brand_name,
            bc_type,
            price,
            shop_id,
            location
          FROM t_base_ec_item_dev_new
          WHERE ds = 20160824
          ) t1
      RIGHT  JOIN
      (

SELECT

item_id      ,
item_title   ,
feed_id      ,
t1.id1  as user_id      ,
content      ,
SUBSTRING(regexp_replace(f_date, '-', ''), 0, 8)  as dsns        ,
annoy        ,
ts           ,
sku          ,
rate_type    ,
crawl_type   ,
mark

from t_base_uid_tmp   t1 join t_zlj_feed_tmp t2 on t1.uid=t2.mark  where t1.ds='uid_mark' and t2.ds='0826'

      ) t2 ON t1.item_id = t2.item_id;


SELECT tage ,brand_name
from
(
select
brand_name,
case when tage<18 then 1
 when tage>=18 and tage<=24 then 2
 when tage>=25 and  tage<=29 then 3
 when tage>=30 and  tage<=34 then 4
 when tage>=35 and  tage<=39 then 5
 when tage>=40 and  tage<=44 then 6
 when tage>=45 and  tage<=49 then 7
 when tage>=50 then 8
end
tage

from
(
	SELECT
	id1  user_id ,brand_name
	from t_zlj_tmp_xizhuang where LENGTH(brand_name)>1
	)t1
	join
	t_base_user_info_s_tbuserinfo_t t2 on t1.user_id =t2.tb_id

) t group by tage ,brand_name
;

	LOAD DATA   INPATH '/user/zlj/tmp/uid_mark_freq' OVERWRITE INTO TABLE t_base_uid_tmp PARTITION (ds='uid_mark')


-- 	两粒单排扣   一粒单排扣  多粒单排扣  三粒双排扣   这四种在不同城市的销量分布    和  分别购买的人群年龄段
SELECT tage ,brand_name,COUNT(1) ,type
from
(
select
brand_name,type ,
case when tage<18 and tage>10 then 1
 when tage>=18 and tage<=24 then 2
 when tage>=25 and  tage<=29 then 3
 when tage>=30 and  tage<=34 then 4
 when tage>=35 and  tage<=39 then 5
 when tage>=40 and  tage<=44 then 6
 when tage>=45 and  tage<=49 then 7
 when tage>=50 then 8
end
tage
from
(
select user_id ,brand_name,type
from
(
SELECT

item_id      ,
item_title   ,
feed_id      ,
t1.id1 as user_id        ,
content      ,
f_date       ,
annoy        ,
ts           ,
sku          ,
rate_type    ,
crawl_type   ,
mark

from t_base_uid_tmp   t1 join t_zlj_feed_tmp t2 on t1.uid=t2.mark  where t1.ds='uid_mark' and t2.ds='uid_mark'
)t3 join
(SELECT  * from wlservice.t_wrt_xifu_menjin_itemid where type in  ('两粒单排扣',   '一粒单排扣',  '多粒单排扣',  '三粒双排扣'))t4
on t3.item_id=t4.item_id
)t5 join t_base_user_info_s_tbuserinfo_t t6 on t5.user_id =t6.tb_id

) t group by tage ,brand_name ,type
;



select
tel_city,brand_name,type  ,COUNT(1)
from
(
select user_id ,brand_name,type
from
(
SELECT

item_id      ,
item_title   ,
feed_id      ,
t1.id1 as user_id        ,
content      ,
f_date       ,
annoy        ,
ts           ,
sku          ,
rate_type    ,
crawl_type   ,
mark

from t_base_uid_tmp   t1 join t_zlj_feed_tmp t2 on t1.uid=t2.mark  where t1.ds='uid_mark' and t2.ds='uid_mark'
)t3 join
(SELECT  * from wlservice.t_wrt_xifu_menjin_itemid where type in  ('两粒单排扣',   '一粒单排扣',  '多粒单排扣',  '三粒双排扣'))t4
on t3.item_id=t4.item_id
)t5 join t_base_user_info_s_tbuserinfo_t t6 on t5.user_id =t6.tb_id
 group by tel_city ,brand_name ,type
;