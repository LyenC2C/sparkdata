
select t2.*, case when t2.age<150  and t2.age >5 then t2.age  else t1.age end age
 from  qqage  t1  join
(select *,(2015-year(birthday)) age  from   t_base_q_user_dev )t2  on t1.qq=t2.uin
;