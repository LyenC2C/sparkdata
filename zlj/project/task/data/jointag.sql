
select
t2.*

from

(select
uid from
t_base_uid where ds=20151125

)t1

join
t_zlj_user_tag_join_1125
t2
on  t1.uid=t2.uid