

--Æû³µÀà

select
user_id, sum(f) as score
from
t_zlj_ec_perfer_dim
where  root_cat_id in
(
26,50016768,50024971,124470006
)
group by user_id
order by sum(f) desc  limit 100;