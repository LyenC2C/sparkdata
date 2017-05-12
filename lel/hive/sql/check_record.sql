check record:

create table wl_service.t_lt_taobao_chongfu_item_dsn 
as 
select feed_id,item_id,user_id,shop_id,ds,dsn,count() as count_item from wl_base.t_base_ec_record_dev_new group by feed_id,item_id,user_id,shop_id,ds,dsn having count_item>1;

select * from wl_base.t_base_ec_record_dev_new where ds = 'true' and feed_id='224563515877' and item_id=40952707203 and user_id='1837205886' and shop_id='62941831'
select * from wl_base.t_base_ec_item_feed_dev_new where feed_id='224563515877' and item_id='40952707203' and user_id='1837205886'



	feed_id	         item_id	     user_id	shop_id	
	303496342076	541579345742	26363558	36292689

	303521855839	535971504092	1693568210	111214834
	303547372615	543061330777	662151407	110367977
	303571449593	534551898900	1659363902	35234007
	303580927766	539595283331	2436359980	457519254

select * from wl_base.t_base_ec_record_dev_new where ds = 'true' and feed_id regexp '303521855839|303547372615|303571449593|303580927766' 
and item_id in (535971504092,543061330777,534551898900,539595283331) and user_id regexp '1693568210|662151407|1659363902|2436359980' and shop_id regexp '111214834|110367977|35234007|457519254'


select * from wl_base.t_base_ec_item_feed_dev_new where feed_id regexp '303482627109|303521855839|303547372615|303571449593|303580927766' and item_id regexp '535971504092|543061330777|534551898900|539595283331'
 and user_id regexp '1693568210|662151407|1659363902|2436359980'
