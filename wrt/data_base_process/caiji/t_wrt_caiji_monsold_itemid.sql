drop table wlservice.t_wrt_caiji_monthsold;
create table wlservice.t_wrt_caiji_monthsold AS
select item_id,sum(day_sold)as month_sold from wlbase_dev.t_base_ec_item_daysale_dev_new
where ds > 20161015 and ds < 20161115 group by item_id;


-- root_cat_id in ('50008907','50004958')

create table wlservice.t_wrt_caiji_monsold_itemid_1_4999 as
select
item_id
from
wlservice.t_wrt_caiji_monthsold where month_sold > 1 and month_sold < 4999;

create table wlservice.t_wrt_caiji_monsold_itemid_5000_20000 as
select
item_id
from
wlservice.t_wrt_caiji_monthsold where month_sold > 5000 and month_sold < 20000;

create table wlservice.t_wrt_caiji_monsold_itemid_20001_150000 as
select
item_id
from
wlservice.t_wrt_caiji_monthsold where month_sold > 20001 and month_sold < 150000;

create table wlservice.t_wrt_caiji_monsold_itemid_150001_3600000 as
select
item_id
from
wlservice.t_wrt_caiji_monthsold where month_sold > 150001 and month_sold < 3600000;

create table wlservice.t_wrt_caiji_monsold_itemid_3600000 as
select
item_id
from
wlservice.t_wrt_caiji_monthsold where month_sold > 3600000;


-- create table t_wrt_caiji_monsold_itemid_1_4900 as
-- select
-- item_id,
-- case0
-- when month_sold >=1 and month_sold <=4999 then " [1,4999]"
-- when month_sold > 5000 and month_sold <= 20000 then "[5000,20000]"
-- when month_sold > 20000 and month_sold <= 150000 then "[20001, 150000]"
-- when month_sold > 150000 and month_sold <= 3600000 then "[150001, 3600000]"
-- when month_sold > 3600000 then "[3600000, ~]"
-- end
-- as sold_fenbu from
-- (select item_id,sum(day_sold)as month_sold from t_base_ec_item_daysale_dev_new where ds > 20161015 and ds < 20161115 group by item_id)t;


