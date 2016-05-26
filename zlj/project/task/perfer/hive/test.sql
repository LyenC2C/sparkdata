# SELECT
#       item_id,
#       user_id,
#   price ,
# log(1.3,price+1 ),
# log(2,price+1 ),
#   datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'),
#                                                             concat_ws('-', substring(ds, 1, 4), substring(ds, 5, 2),
#                                                                       substring(ds, 7, 2))),
#   pow(2.8, (datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'),
#                                                             concat_ws('-', substring(ds, 1, 4), substring(ds, 5, 2),
#                                                                       substring(ds, 7, 2)))) *(-0.005)),
#
#       round(log(1.3,price+1 ) * pow(2.8, (datediff(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'),
#                                                             concat_ws('-', substring(ds, 1, 4), substring(ds, 5, 2),
#                                                                       substring(ds, 7, 2)))) *(-0.005)) , 4)+1 AS score
#
#     FROM
#       t_zlj_tmp_test1
#
#     WHERE CAST(item_id AS INT) > 0 AND CAST(brand_id AS INT) > 0 AND CAST(user_id AS INT) > 0
# limit 1000;