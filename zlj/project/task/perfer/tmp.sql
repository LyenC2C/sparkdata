


-- 只包含微博 以及手机号
SELECT
weibo_id,id1
from
(
select * from t_base_uid_tmp where ds='atm' and length(id1)>5
)t1 join t_zlj_visul_tel_bi_friends_link_rs t2 on t1.id3=t2.weibo_id where id5='None'  limit 100
;


SELECT
t2.uid,id1
from
(
select * from t_base_uid_tmp where ds='atm' and length(id1)>5 and id1=
)t1 join t_zlj_user_tag_join_t t2 on t1.id5=t2.uid where id3='None'
 limit 100;


select * from t_base_uid_tmp where ds='atm' and length(id1)>5 and id1=13915757829
 tb_id             tel
