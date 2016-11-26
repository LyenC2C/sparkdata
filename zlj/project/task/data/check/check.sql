----------------------------
select  count(1) from table ;

select count(1) from(select  name from table group by name )t;
----------------------------


328053114
select  count(1) from t_zlj_phone_rank_index ;

325857943
select count(1) from(select  tb_id from t_zlj_phone_rank_index group by tb_id )t;

328053114
SELECT  COUNT(1) from t_zlj_t_base_uid_tmp_rank ;

325857943
select count(1) from(select  id from t_zlj_t_base_uid_tmp_rank group by id )t;


