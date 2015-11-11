

--汽车类

drop table if EXISTS  t_zlj_ec_perfer_cat;
create table t_zlj_ec_perfer_cat as
select
user_id, '有车一族' tag ,sum(f) as score
from
t_zlj_ec_perfer_dim
where  root_cat_id in
(
26,50016768,50024971,124470006
)
group by user_id ;
-- order by sum(f) desc
--  limit 100;
;