

create table  t_zlj_credit_valid_api2_step1  AS

SELECT  user_id ,COUNT(1) as shop_num
FROM
 t_base_ec_record_dev_new_simple
where dsn>20160701  group by user_id ;