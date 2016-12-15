

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
wlfinance.t_zlj_tmp_csv where ds='360_browse_history_dropdup'
)t group by  userid ,view_c
;

select * from wlfinance.t_zlj_tmp t ;

-- 信用卡记录
drop table  wlfinance.t_zlj_tmp_360_bill;
create table wlfinance.t_zlj_tmp_360_bill as
select
  userid,bankid ,
  count(1) as record_count ,
  round( log10(max(timespan)-min(min_timespan)+10),2) as time_cross ,
  round( sum(consum_times) /log10((max(timespan)-min(min_timespan))+10),2) as use_freq ,
  round( sum(last_return_money) ,2)  as last_return_money_sum ,
  round( max(last_return_money) ,2)  as last_return_money_max ,
  round( min(last_return_money) ,2)  as last_return_money_min ,
  round( std(last_return_money) ,2)  as last_return_money_std ,

  round( avg(last_credit_money/last_return_money) ,2) avg_return_rate,
  round( min(last_credit_money/last_return_money) ,2) min_return_rate,
  round( max(last_credit_money/last_return_money) ,2) max_return_rate,
  round( sum(last_credit_money/last_return_money) ,2) sum_return_rate,
round( max(credit_e_du) ,2) as credit_e_du_max,
round( min(credit_e_du) ,2) as credit_e_du_min,
round( std(credit_e_du) ,2) as credit_e_du_std,

round( sum(abs(credit_yu_e)) ,2) as credit_abs_yu_e_sum,
round( max(abs(credit_yu_e)) ,2) as credit_abs_yu_e_max,
round( min(abs(credit_yu_e)) ,2) as credit_abs_yu_e_min,
round( std(abs(credit_yu_e)) ,2) as credit_abs_yu_e_std,

round( sum(abs(case when credit_yu_e>0 then credit_yu_e else 0 end )) ,2) as credit_abs_pos_yu_e_sum,
round( max(abs(case when credit_yu_e>0 then credit_yu_e else 0 end )) ,2) as credit_abs_pos_yu_e_max,
round( min(abs(case when credit_yu_e>0 then credit_yu_e else 0 end )) ,2) as credit_abs_pos_yu_e_min,
round( std(abs(case when credit_yu_e>0 then credit_yu_e else 0 end )) ,2) as credit_abs_pos_yu_e_std,

round( sum(abs(case when credit_yu_e<0 then credit_yu_e else 0 end )) ,2) as credit_abs_neg_yu_e_sum,
round( max(abs(case when credit_yu_e<0 then credit_yu_e else 0 end )) ,2) as credit_abs_neg_yu_e_max,
round( min(abs(case when credit_yu_e<0 then credit_yu_e else 0 end )) ,2) as credit_abs_neg_yu_e_min,
round( std(abs(case when credit_yu_e<0 then credit_yu_e else 0 end )) ,2) as credit_abs_neg_yu_e_std,


round( sum(last_credit_money/case when credit_e_du <0.5  then last_credit_money*100000 else credit_e_du end ),2) load_sum ,
round( avg(last_credit_money/case when credit_e_du <0.5  then last_credit_money*100000 else credit_e_du end ),2) load_avg ,
round( max(last_credit_money/case when credit_e_du <0.5  then last_credit_money*100000 else credit_e_du end ),2) load_max ,
round( std(last_credit_money/case when credit_e_du <0.5  then last_credit_money*100000 else credit_e_du end ),2) load_std ,
round( sum(consum_times),2) as consum_times,
round( std(consum_times),2) as consum_times_std,

 sum(case when consum_times<0.5  then 1 else 0 end ) as consum_times_zero_sum ,
 sum(case when credit_e_du <0.5  then 1 else 0 end ) as consum_credit_e_du_zero_sum ,

round( max(cash),2) as cash_max ,
round( min(cash),2) as cash_min ,
round( std(cash),2) as cash_std ,
round( sum(state),2) as state_num
  from
    (
      select
        t1.* ,t2.min_timespan
          from
      (
      SELECT
        tel  AS userid,
        id1  AS timespan,
        id2  AS bankid,
        id3  AS last_credit_money,
        id4  AS last_return_money,
        id5  AS credit_e_du,
        id6  AS credit_yu_e,
        id7  AS zuidi_huankuan,
        id8  AS consum_times,
        id9  AS cur_money,
        id10 AS modey_money,
        id11 AS rate,
        id12 AS balance,
        id13 AS cash,
        id14 AS state
      FROM
        wlfinance.t_zlj_tmp_csv
      WHERE ds = '360_bill_detail_dropdup'
    )
      t1 join
      (
      select
        tel as userid,
          min(id1) as min_timespan,
          id2 as bankid
      FROM
        wlfinance.t_zlj_tmp_csv
      WHERE ds = '360_bill_detail_dropdup' and id1>0  group by tel ,id2
      )t2 on t1.userid=t2.userid and t1.bankid =t2.bankid
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
round( sum(smoney),2) as money_sum ,
round( max(smoney),2) as money_max ,
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

--
select
  count(1)
from
wlfinance.t_zlj_tmp_csv where ds='360_bank_detail_dropdup' and  id1<0.5