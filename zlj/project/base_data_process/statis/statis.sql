

-- �����û���
select count(1) from
(SELECT

user_id

from
t_base_ec_item_feed_dev
where ds >20150701
group by user_id
)t ;


-- ������Ʒ��
select count(1) from
(SELECT

item_id

from
t_base_ec_item_feed_dev
where ds >20150701
group by item_id
)t ;


-- �����¼��
SELECT

COUNT(1)
from
t_base_ec_item_feed_dev
where ds >20150701 ;


-- �����¼��Ʒ������

select count(1) from
t_zlj_ec_userbuy  ;



-- ��Ʒ��

select count(1) from t_base_ec_item_dev where ds=20151030