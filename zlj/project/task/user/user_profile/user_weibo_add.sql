INSERT OVERWRITE TABLE  t_base_user_info_s  PARTITION(ds='20160418')

SELECT
tb_id,
max(tgender),
max(tage),
max(tname),
max(tloc)
from
(
select
t1.tb_id ,
case when LENGTH(t1.tgender)<1 then t4.tgender else t1.tgender end as tgender,
case when LENGTH(t1.tage )<1   then t4.tage else t1.tage end as tage,
case when LENGTH(t1.tname )<2  then t4.tname else t1.tname end as tname,
case when LENGTH(t1.tloc )<2  then t4.tloc else t1.tloc end as tloc

from t_base_user_info_s t1



left join
(

select
t2.uid  as tb_id , CAST(t3.gender-1 as string) as tgender, (2016-b_year )  as tage , nickname  as tname ,concat_ws('\t',l_provincename , l_city) as tloc
from t_qqweibo_user_info t3

join t_base_uid t2   where t2.ds='tb-qqwb'  and t3.id=t2.id1
)t4  on  t1.tb_id=t4.tb_id  and t1.ds='20160310'
 )t group by tb_id;




-- select COUNT(1) from
-- (
--
-- select
-- t2.uid  as tb_id , CAST(t3.gender-1 as string) as tgender, (2016-b_year )  as tage , nickname  as tname ,concat_ws('\t',l_provincename , l_city) as tloc
-- from t_qqweibo_user_info t3
--
-- join t_base_uid t2   where t2.ds='tb-qqwb'  and t3.id=t2.id1
-- )t4 ;
--
--
-- select  COUNT(1) from t_base_user_info_s where    ds=20160418 ;
--
-- select COUNT(1) from (select  tb_id  from t_base_user_info_s where    ds=20160418 group by tb_id)t ;
-- select * from t_base_uid where  ds='tb-qqwb' limit 10;


-- select count(1) from t_base_user_info_s t1
--
--  join
-- (
--
-- select
-- t2.uid  as tb_id , t3.gender as tgender, (2016-b_year )  as tage , nickname  as tname ,concat_ws('\t',l_provincename , l_city) as tloc
-- from t_qqweibo_user_info t3
--
-- join t_base_uid t2   where t2.ds='tb-qqwb'  and t3.id=t2.id1
-- )t4  on  t1.tb_id=t4.tb_id ;

--
--  select count(1) from  t_base_user_info_s  where ds=20160418  and  length(tgender)>0 ;
--  76703156
--
--
--   select count(1) from  t_base_user_info_s  where ds=20160418  and  length(tage)>0 ;
--  56424359
--
--    select count(1) from  t_base_user_info_s  where ds=20160418  and  length(tname)>0 ;
--    56093685