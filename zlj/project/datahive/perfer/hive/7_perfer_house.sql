

--·¿²úÀà

select
user_id, sum(f) as score
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
order by sum(f) desc  limit 100;