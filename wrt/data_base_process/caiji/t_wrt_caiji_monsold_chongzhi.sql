drop table wlservice.t_wrt_caiji_monthsold_cz;
create table wlservice.t_wrt_caiji_monthsold_cz AS
select t1.item_id,t1.month_sold from
(select item_id,sum(day_sold)as month_sold from wlbase_dev.t_base_ec_item_daysale_dev_new
where ds > 20161015 and ds < 20161115 group by item_id)t1
JOIN
(select item_id from wlbase_dev.t_base_ec_item_dev_new where ds = 20161104 and root_cat_id in ('50008907','50004958'))t2
ON
t1.item_id = t2.item_id
;


-- root_cat_id in ('50008907','50004958')

create table wlservice.t_wrt_caiji_monsold_cz_1_4999 as
select
item_id
from
wlservice.t_wrt_caiji_monthsold_cz where month_sold > 1 and month_sold < 4999;

create table wlservice.t_wrt_caiji_monsold_cz_5000_20000 as
select
item_id
from
wlservice.t_wrt_caiji_monthsold where month_sold > 5000 and month_sold < 20000;

create table wlservice.t_wrt_caiji_monsold_cz_20001_150000 as
select
item_id
from
wlservice.t_wrt_caiji_monthsold_cz where month_sold > 20001 and month_sold < 150000;

create table wlservice.t_wrt_caiji_monsold_cz_150001_3600000 as
select
item_id
from
wlservice.t_wrt_caiji_monthsold_cz where month_sold > 150001 and month_sold < 3600000;

create table wlservice.t_wrt_caiji_monsold_cz_3600000 as
select
item_id
from
wlservice.t_wrt_caiji_monthsold_cz where month_sold > 3600000;