INSERT OVERWRITE TABLE  t_base_user_info_s  PARTITION(ds='20160418')
select
t1.tb_id ,
case when t1.tgender is null then t4.tgender else t1.tgender end as tgender,
case when t1.tage is null then t4.tage else t1.tage end as tage,
case when t1.tname is null then t4.tname else t1.tname end as tname,
case when t1.tloc is null then t4.tloc else t1.tloc end as tloc

from t_base_user_info_s t1

left join
(

select
t2.uid  as tb_id , t3.gender as tgender, (2016-b_year )  as tage , nickname  as tname ,concat_ws('\t',l_provincename , l_city) as tloc
from t_qqweibo_user_info t3

join t_base_uid t2   where t2.ds='tb-qqwb'  and t3.id=t2.id1
)t4  on  t1.tb_id=t4.tb_id  and t1.ds='20160310' ;