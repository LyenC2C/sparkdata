
#zlj


#有ds的数据统计最新ds
#表基本信息统计


date_now =`date +%Y%m%d`

echo $date_now >> file
echo '---------\n\n' >>file


dstables=("alipay" \
"buycnt" \
"verify" \
"regtime" \
"tb_nick" \
"tb_location" \
"tgender" \
"tage" \
"tname" \
"tloc" \
"tel_prov" \
"tel_city" \
"xianyu_gender" \
"xianyu_birthday" \
"xianyu_constellation" \
"xianyu_province" \
"xianyu_city" \
"predict_gender" \
"xianyu_detail_loc" \
"weibo_id" \
"weibo_screen_name" \
"weibo_gender" \
"weibo_location" \
"weibo_verified"  )


for var in ${dstables[@]};
do
    echo $var >>file
    table=$var
    data=`hive -e "use wlbase_dev;  select count(1) from t_base_user_info_s_tbuserinfo_t_step6 where length($table)>1  "  1>>file`
    echo  "">>file
    echo  "">>file
done
