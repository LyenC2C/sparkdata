


-- �ͻ���ʧ



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


-- �û�����

select

brand_id ,user_id ,user_feed-back_pos ,user_feed-back_neg
from table_name



-- Ʒ�Ʋ�������������

select  brand_id,tag,COUNT(1)
from
group by  brand_id,tag