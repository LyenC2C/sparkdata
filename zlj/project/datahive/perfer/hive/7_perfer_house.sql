

--房产类
drop table if EXISTS  t_zlj_ec_perfer_house;
create table t_zlj_ec_perfer_house as
select
user_id,'有房一族' tag, sum(f) as score
from
t_zlj_ec_perfer_dim
where  root_cat_id in
(
27,
50008164,
50020332,
50020808,
50022649,
50022703,
50022987,
50023804,
50025881,
122852001,
123302001,
124698018
)
group by user_id
order by sum(f) desc
-- limit 100;
;