


-- 客户流失



select from
brand_id ,user_id ,COUNT(1)


table_name t1
join
(
select
 DISTINCT  user_id
from  table_name

where brand_id=''
)t2 on t1.user_id=t2.user_id  and t1.cat=''

group by brand_id ,user_id


-- 用户评价

select

brand_id ,user_id ,user_feed-back_pos ,user_feed-back_neg
from table_name



-- 品牌参数和销量聚类

select  brand_id,tag,COUNT(1)
from
group by  brand_id,tag