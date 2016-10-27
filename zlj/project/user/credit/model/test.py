#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

import pandas

data = pandas.read_csv(u'E:\\项目\\征信&金融\\模型\\rong360_allclass_taguser.csv')

index=[
    'label',
'std_cnt',
'avg_price',
'std_price',
'alipay_flag',
'buycut',
'verify_level',
'regtime_month',
'qq_gender',
'local_buycount',
'total_price',
'buy_month',
'avg_cnt',
'car_flag',
'house_flag',
'child_flag',
'pet_flag',
'annoy_num',
'annoy_ratio',
'brand_id_num',
'root_cat_id_num',
'b_bc_type_num',
'b_bc_type_num_ratio',
'b_bc_price_ratio',
'brand_effec_price_ratio',
'brand_effec_num_ratio',
'b50_num_ratio',
'b50_ratio',
'avg_price_ratio',
'std_price_ratio',
'price_ratio_50011740',
'price_ratio_50006842',
'price_ratio_1512',
'cnt_ratio_50002766',
'price_ratio_50011665',
'price_ratio_30',
'price_ratio_50468001',
'cnt_ratio_28',
'cnt_ratio_50010404',
'cnt_ratio_16',
'price_ratio_50008907',
'price_ratio_28',
'cnt_ratio_50011740',
'price_ratio_50008164',
'cnt_ratio_50004958',
'cnt_ratio_50008090',
'price_ratio_1801',
'cnt_ratio_50012082',
'cnt_ratio_50008165',
'price_ratio_50007218',
'cnt_ratio_50014811',
'price_ratio_1625',
'cnt_ratio_50013886',
'cnt_ratio_26',
'cnt_ratio_50020485',
'price_ratio_35',
'price_ratio_50002768',
'cnt_ratio_50025705',
'price_ratio_29',
'cnt_ratio_50014812',
'cnt_ratio_50016348',
'price_ratio_50022703',
'price_ratio_50017300',
'price_ratio_27',
'price_ratio_50023717',
'cnt_ratio_23',
'price_ratio_50011949',
'cnt_ratio_50013864',
'cnt_ratio_50008164',
'price_ratio_50010728',
'cnt_ratio_124468001',
'cnt_ratio_124458005',
'price_ratio_21',
'price_ratio_50008163',
'cnt_ratio_124354002',
'price_ratio_99',
'cnt_ratio_50016422',
'price_ratio_50011699',
'cnt_ratio_50023282',
'cnt_ratio_122852001',
'price_ratio_50008141',
'price_ratio_50007216',
'cnt_ratio_50023722',
'price_ratio_25',
'price_ratio_50011972'
]

index_data=data.loc[:,index]
# index_lable=data.loc[:,  ['label' ]]
from sklearn.cross_validation import train_test_split

train_X,test_X=train_test_split(index_data ,  test_size=0.3 , random_state=15)

train_X.to_csv(u'E:\\项目\\征信&金融\\模型\\rong360_allclass_taguser_train_X.csv')
test_X.to_csv(u'E:\\项目\\征信&金融\\模型\\rong360_allclass_taguser_test_X.csv')
