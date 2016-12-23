
-- 分时间段统计占比
drop table wlfinance.t_zlj_tmp;
create table wlfinance.t_zlj_tmp as
SELECT
t1.userid ,
t1.view_c ,
num as all_num ,
round(time_cross          , 4)  all_time_cross,
round(view_freq            , 4) all_view_freq,
round(time_std            , 4)  all_time_std,
round(COALESCE(before_num ,0.1)          , 4) before_num ,
round(COALESCE(before_time_cross,0.1)    , 4) before_time_cross ,
round(COALESCE(before_view_freq ,0.1)    , 4) before_view_freq ,
round(COALESCE(before_time_std  ,0.1)    , 4) before_time_std ,
round(COALESCE(before_num       ,0,1)  /num       ,4)        as num_ratio,
round(COALESCE(before_time_cross,0,1)  /time_cross ,4) as time_cross_ratio,
round(COALESCE(before_view_freq ,0,1)  / view_freq ,4)  as view_freq_ratio,
round(COALESCE(before_time_std  ,0,1)  /time_std  ,4)   as time_std_ratio
from
(
	SELECT
userid ,
view_c ,
COUNT(1) as num ,
 log10(max(timespan)-min(timespan)+10) as time_cross ,
 COUNT(1) /log10((max(timespan)-min(timespan))+10) as view_freq ,
 log10(std(timespan )+10) as time_std
from
(
	SELECT
	t1.userid,
	t1.timespan ,
	view_c ,
	case when timespan>loan_time then 'after' else 'before' end time_flag
	from
(
select tel userid,
id1 timespan ,
concat(id2,'_',id3) view_c
from
wlfinance.t_zlj_tmp_csv where ds='360_browse_history_dropdup'
)t1 join
 (
 select tel as userid ,id1 as 	loan_time ,
 case when tel>55596 then 'test' else 'train' end as user_label
 	from
wlfinance.t_zlj_tmp_csv where ds='360_loan_time'
)t2 on t1.userid=t2.userid

)t3
 group by  userid ,view_c
 )t1
left join
(
SELECT
userid ,
view_c ,
COUNT(1) as before_num ,
log10(max(timespan)-min(timespan)+10) as before_time_cross ,
COUNT(1) /log10((max(timespan)-min(timespan))+10) as before_view_freq ,
log10(std(timespan )+10) as before_time_std
from
(
	SELECT
	t1.userid,
	t1.timespan ,
	view_c
	from
(
select tel userid,
id1 timespan ,
concat(id2,'_',id3) view_c
from
wlfinance.t_zlj_tmp_csv where ds='360_browse_history_dropdup'
)t1 join
 (
 select tel as userid ,id1 as 	loan_time ,
 case when tel>55596 then 'test' else 'train' end as user_label
 	from
wlfinance.t_zlj_tmp_csv where ds='360_loan_time'
)t2 on t1.userid=t2.userid
where  timespan<loan_time
)t3
 group by  userid ,view_c
 )t2 on t1.userid=t2.userid and t1.view_c=t2.view_c
;
