insert overwrite table t_base_ec_item_daysale_dev partition(ds = 20160316)
select t1.item_id,ceiling((t1.total - t2.total_sold) / 39) as daysold,
cast((t1.total - t2.total_sold) / 39 * t1.price as float) as daysales from
(select * from t_base_ec_item_sold_dev where ds = 20160424)t1
left join
(select item_id,total_sold from t_base_ec_item_sale_dev_new where ds = 20160316)t2
ON
t1.item_id = t2.item_id;


LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160317');


insert OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160318');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;


insert OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160319')
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;


insert OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160320')
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

insert
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160321')
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;


insert OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160322')
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160323');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160324');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160325');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160326');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160327');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160328');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160329');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160330');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160331');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160401');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160402');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160403');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160404');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160405');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160406');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160407');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160408');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160409');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160410');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160411');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160412');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160413');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160414');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160415');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160416');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160417');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160418');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160419');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160420');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160421');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160422');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

LOAD DATA  INPATH '/hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316'
OVERWRITE INTO TABLE t_base_ec_item_daysale_dev PARTITION (ds='20160423');
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;


--hfs -cp /hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316/* /user/wrt/temp/20160317


insert overwrite table t_base_ec_item_daysale_dev PARTITION (ds = 20160317)
select item_id,day_sold,day_sold_price from t_base_ec_item_daysale_dev where ds = 20160316;

hfs -mkdir /user/wrt/temp/repair/20160317
hfs -mkdir /user/wrt/temp/repair/20160318
hfs -mkdir /user/wrt/temp/repair/20160319
hfs -mkdir /user/wrt/temp/repair/20160320
hfs -mkdir /user/wrt/temp/repair/20160321
hfs -mkdir /user/wrt/temp/repair/20160322
hfs -mkdir /user/wrt/temp/repair/20160323
hfs -mkdir /user/wrt/temp/repair/20160324
hfs -mkdir /user/wrt/temp/repair/20160325
hfs -mkdir /user/wrt/temp/repair/20160326
hfs -mkdir /user/wrt/temp/repair/20160327
hfs -mkdir /user/wrt/temp/repair/20160328
hfs -mkdir /user/wrt/temp/repair/20160329
hfs -mkdir /user/wrt/temp/repair/20160330
hfs -mkdir /user/wrt/temp/repair/20160331

hfs -mkdir /user/wrt/temp/repair/2016031

20160317
hive
hfs -cp /user/wrt/temp/daysale_tmp_repair /hive/warehouse/wlbase_dev.db/t_base_ec_item_daysale_dev/ds=20160316/* /user/wrt/temp/daysale_tmp_

hfs -mkdir /user/wrt/temp/daysale_tmp_repair