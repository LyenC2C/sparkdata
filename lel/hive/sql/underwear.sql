try to avoid  cheater:

step 1:
  filter underwear cat_id:

create table wl_service.t_lel_underwear_catids
as
SELECT distinct(cat_id) FROM wl_base.t_base_ec_record_dev_new where ds='true' and title regexp '内裤'
step 2:
  filter underwear user_id:
create table wl_service.t_lel_underwear_userids
as
SELECT distinct a.user_id from
(SELECT user_id,cat_id FROM wl_base.t_base_ec_record_dev_new where ds = 'true') a
left semi join 
(select cat_id from wl_service.t_lel_underwear_catids) b
on a.cat_id = b.cat_id





