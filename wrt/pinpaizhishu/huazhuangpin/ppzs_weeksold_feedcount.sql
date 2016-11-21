-- drop table wlservice.ppzs_brandid_weeksold_feedcount;
-- create table wlservice.ppzs_brandid_weeksold_feedcount as
-- select t1.brand_id,t1.weeksold,t2.good_count,t2.mid_count,t2.bad_count from
-- (select * from wlservice.ppzs_brandid_weeksold where ds = '20161106')t1
-- join
-- (select * from wlservice.ppzs_brandid_rate_count where ds = '20161108')t2
-- ON
-- t1.brand_id = t2.brand_id

-- hive -e "
select t1.brand_id,t1.weeksold,t2.good_count,t2.mid_count,t2.bad_count from
(select * from wlservice.ppzs_brandid_weeksold where ds = '${hiveconf:yes_day}')t1
join
(select * from wlservice.ppzs_brandid_rate_count where ds = '${hiveconf:yes_day}')t2
ON
t1.brand_id = t2.brand_id
-- " >> ppzs_brandid_weeksold_feedcount_testd