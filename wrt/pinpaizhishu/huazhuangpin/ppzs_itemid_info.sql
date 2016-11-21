LOAD DATA  INPATH '/user/wrt/temp/ppzs_itemid_info' OVERWRITE INTO TABLE
wlservice.ppzs_itemid_info PARTITION (ds=$yes_day);
