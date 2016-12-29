source ~/.bashrc

last_item_feedidls_date=$1
last_uid_mark_date=$2
start_date=$3
end_date=$4

echo "last item feedid uid ls: "$last_item_feedidls_date
echo -e "\nlast uid mark: "$last_uid_mark_date
echo -e "\nmissiondata: "$mission_data

spark-submit --master spark://cs100:7077 \
--executor-memory 25g \
--driver-memory 10g \
--total-executor-cores 100 \
/home/pzz/workspace/sparkdata/pzz/cmt/cmt_inc_updated_1128.py \
-gen_cmt_inc \
/data/develop/ec/tb/cmt/itemid_feedid/itemid_feediduidls.${last_item_feedidls_date} \
/data/develop/ec/tb/cmt/uid_mark/uid_mark_freq.json.${last_uid_mark_date} \
${start_date} \
${end_date} \
/data/develop/ec/tb/cmt/itemid_feedid/itemid_feediduidls.${end_date} \
/data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.${end_date} \
/data/develop/ec/tb/cmt/tmpdata.nouid/cmt_inc_data.nouid.${end_date} \
/data/develop/ec/tb/cmt/user/user.${end_date}


