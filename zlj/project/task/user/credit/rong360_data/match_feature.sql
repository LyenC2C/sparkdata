

-- 'userid','timespan','view','view_s
t_zlj_tmp_csv_data

alter table wlfinance.t_zlj_tmp_csv_data add columns(id8 string);
alter table wlfinance.t_zlj_tmp_csv_data add columns(id9 string);
alter table wlfinance.t_zlj_tmp_csv_data add columns(id10 string);
alter table wlfinance.t_zlj_tmp_csv_data add columns(id11 string);
alter table wlfinance.t_zlj_tmp_csv_data add columns(id12 string);
alter table wlfinance.t_zlj_tmp_csv_data add columns(id13 string);
alter table wlfinance.t_zlj_tmp_csv_data add columns(id14 string);
alter table wlfinance.t_zlj_tmp_csv_data add columns(id15 string);
alter table wlfinance.t_zlj_tmp_csv_data add columns(id16 string);
alter table wlfinance.t_zlj_tmp_csv_data add columns(id17 string);
alter table wlfinance.t_zlj_tmp_csv_data add columns(id18 string);
alter table wlfinance.t_zlj_tmp_csv_data add columns(id19 string);


create table wlfinance.t_zlj_tmp_csv like wlfinance.t_zlj_tmp_csv_data;


-- 浏览数据
drop table wlfinance.t_zlj_tmp;
create table wlfinance.t_zlj_tmp as
SELECT
userid ,
view_c ,
COUNT(1) as num ,
round( log10(max(timespan)-min(timespan)+10),2) as time_cross ,
round( COUNT(1) /log10((max(timespan)-min(timespan))+10),2) as view_freq ,
round( log10(std(timespan )+10),2) as time_std
from
(
select tel userid,
id1 timespan ,
concat(id2,'_',id3) view_c
from
wlfinance.t_zlj_tmp_csv where ds='360_browse_history_train'
)t group by  userid ,view_c
;


-- 信用卡记录
drop table  wlfinance.t_zlj_tmp_360_bill;
create table wlfinance.t_zlj_tmp_360_bill as
select
  userid,bankid ,
  count(1) as num ,
  round( log10(max(timespan)-min(timespan)+10),2) as time_cross ,
  round( sum(last_return_money) ,2)  as last_return_money_sum ,
round( std(last_return_money) ,2)  as last_return_money_std ,
round( max(credit_e_du) ,2) as credit_e_du_max,
round( min(credit_e_du) ,2) as credit_e_du_min,
round( std(credit_e_du) ,2) as credit_e_du_std,
round( max(credit_yu_e) ,2) as credit_yu_e_max,
round( min(credit_yu_e) ,2) as credit_yu_e_min,
round( std(credit_yu_e) ,2) as credit_yu_e_std,
round( sum(last_credit_money/credit_e_du),2) load_sum ,
round( avg(last_credit_money/credit_e_du),2) load_avg ,
round( max(last_credit_money/credit_e_du),2) load_max ,
round( sum(consum_times),2) as consum_times,
round( max(cash),2) as cash_max ,
round( min(cash),2) as cash_min ,
round( std(cash),2) as cash_std ,
round( sum(state),2) as state_num
  from
(
  select
tel  as userid ,
id1  as timespan ,
id2  as bankid ,
id3  as last_credit_money ,
id4  as last_return_money ,
id5  as credit_e_du ,
id6  as credit_yu_e ,
id7  as zuidi_huankuan ,
id8  as consum_times ,
id9  as cur_money ,
id10  as modey_money ,
id11 as rate ,
id12  as balance ,
id13  as cash ,
id14  as state
from
wlfinance.t_zlj_tmp_csv where ds='bill_detail_train'
)t
group by userid,bankid ;



--银行记录
drop table  wlfinance.t_zlj_tmp_360_bank;
create table wlfinance.t_zlj_tmp_360_bank as
SELECT
 userid ,concat(trade_type,'trade_salary',salary_label) as label  ,
 count(1) as num ,
round( log10(max(timespan)-min(timespan)+10),2) as time_cross ,
round( log10(std(timespan )+10),2) as time_std ,
round(  std(smoney )+10,2) as money_std ,
round( sum(smoney),2) as money_sum,
round( max(smoney),2) as money_max,
round( avg(smoney),2) as money_avg
 from
(
select
  tel as userid,
 	id1 as timespan,
 	id2 as trade_type,
  id3 as smoney ,
 	id4 as salary_label
from
wlfinance.t_zlj_tmp_csv where ds='bank_detail_train'
)t group by userid ,trade_type,salary_label ;