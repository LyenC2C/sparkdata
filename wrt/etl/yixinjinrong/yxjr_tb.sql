drop table wlservice.t_wrt_tb_yxjr;
create table wlservice.t_wrt_tb_yxjr as
select t1.tel,
case
when keywords like "%套现%" then "套现"
when keywords like "%pos%" or keywords like "%POS%" then "pos"
when keywords like "%花呗%" then "花呗"
when keywords like "%京东白条%" then "京东白条"
END
FROM
(select * from wlfinance.t_hx_taobao_fraud_record_out where
keywords like "%套现%" or
keywords like "%pos%" or
keywords like "%POS%" or
keywords like "%花呗%" or
keywords like "%京东白条%"
) t1
JOIN
(select * from wlbase_dev.t_base_mobile_loc where province = '上海') t2
ON
substr(t1.tel,1,7) = t2.prefix;