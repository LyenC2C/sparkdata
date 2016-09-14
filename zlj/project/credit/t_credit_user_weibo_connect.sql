


create table t_credit_user_weibo_connect_step1 as
select t1.id1,t1.id1_tel,t1.id2,t2.uid as id2_tel
from
(
SELECT
t1.id1 ,uid as id1_tel ,t1.id2
from t_base_weibo_user_fri_bi_friends t1
join t_base_uid_tmp t2 on t1.id1=t2.id1  and    t2.ds='wid'
)t1 join t_base_uid_tmp t2 on t1.id2=t2.id1  and    t2.ds='wid' ;




-- hfs -cp   /user/zlj/t_credit_user_weibo_connect  /hive/warehouse/wlcredit.db/

CREATE  TABLE  wlcredit.t_credit_user_weibo_connect as
SELECT
id1 as weibo_id , id1_tel as weibo_tel ,concat_ws(' ',collect_set(concat_ws('_', id2,id2_tel) )) AS feed_tags
from
t_credit_user_weibo_connect_step1
group by  id1,id1_tel
union all
SELECT
id2 as weibo_id , id2_tel as weibo_tel ,concat_ws(' ',collect_set(concat_ws('_', id1,id1_tel) )) AS feed_tags
from
t_credit_user_weibo_connect_step1
group by  id2,id2_tel
;


-- hfs -mv /hive/warehouse/wlcredit.db/t_credit_user_weibo_connect  /user/zlj/