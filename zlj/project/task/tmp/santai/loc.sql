select
user_id , row_number()  OVER (PARTITION BY user_id ORDER BY home_score desc) as home_score_rn ,
		  row_number()  OVER (PARTITION BY user_id ORDER BY work_score desc) as work_score_rn ,
 home_score ,work_score,loc
from
(
SELECT user_id, sum(home_score) as home_score, sum(work_score) as work_score,loc
from
	(
	select
	user_id,
	case when  pmod(datediff(take_time, '2012-01-01'), 7)  in (0 ,6 )     then  1
	when hour(take_time)>18 then 1
	else 0  end  as home_score ,
	case when  pmod(datediff(take_time, '2012-01-01'), 7) not in (0 ,6 )     then  1
	when hour(take_time)<=18 then 1
	else 0  end  as  work_score,
	location as loc
	from
	order_log
	)t group by user_id
)t

)t where home_score_rn=1 or work_score_rn=1