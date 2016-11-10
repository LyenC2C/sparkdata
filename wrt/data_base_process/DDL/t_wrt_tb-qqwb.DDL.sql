use wlservice;

create table t_wrt_tb_qqwb AS
select t1.uid,t2.* from
(select uid,id1 from wlbase_dev.t_base_uid where ds = 'tb-qqwb') t1
JOIN
wlbase_dev.t_qqweibo_user_info t2
ON
t1.id1 = t2.id


create table t_wrt_tb_qqwb_sch_com AS
select uid,id,com_startyear1,com_endyear1,com_comname1,com_depname1,year1,background1,school1,department1 from t_wrt_tb_qqwb
where com_startyear1 <> "-" or background1 <> "-";