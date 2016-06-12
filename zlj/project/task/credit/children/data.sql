


CREATE TABLE t_zlj_credit_children_feed_data AS
SELECT *
FROM t_base_ec_item_feed_dev
WHERE content LIKE '%º¢×Ó%' or content LIKE '%¶ù×Ó%'  or content LIKE '%Å®¶ù%'  or   content LIKE '%Ó¤¶ù%';