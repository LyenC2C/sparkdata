#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

import pandas as pd
import numpy as np
import matplotlib
matplotlib.style.use('ggplot')
import seaborn as sns
sns.set(context='notebook',style="ticks",palette="GnBu_d",font_scale=1.5,font='ETBembo',rc={"figure.figsize": (10, 6)})
#plt.rcParams['figure.figsize']=(15,10)
import warnings
warnings.filterwarnings('ignore') #为了整洁，去除弹出的warnings
pd.set_option('precision', 5) #设置精度
pd.set_option('display.float_format', lambda x: '%.5f' % x) #为了直观的显示数字，不采用科学计数法
pd.options.display.max_rows = 200 #最多显示200行



from  multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

# pool=Pool(8)
# pool.map
#  统计
# t=file[file['label']==0]
# t_sum=(t<=0).sum(axis=1)
# df=pd.DataFrame({'k':t_sum,'v':[1 for i in xrange(len(t_sum))]})
# df['v'].groupby[df['k']]


# Series
'''
删除空置率过高的特征
'''
def filter_feature(t,score):
	ls=[]
	kv=zip(t.index, t.values)
	for k,v in kv:
	   if v>score:
		   # print k,v
		   ls.append(k)
	return ls



def filter_feature_df(df, filter_v):
	'''
	删除空置率过高的特征
	'''
	t=(df<=0).sum(axis=0)/len(df)
	drop_f_list=filter_feature(t,filter_v)
	print 'drop f yuzhi: ',filter_v
	print 'drop f ratio: ',len(drop_f_list), len(df.columns) ,len(drop_f_list)*1.0/len(df.columns)
	# print 'drop f ',drop_f_list
	df.drop(drop_f_list,axis=1,inplace=True)


def filter_feature_df(df, filter_v):
	'''
	删除空置率过高的样本
	'''
	t=(df<=0).sum(axis=1)/len(df.columns)
	drop_f_list=filter_feature(t,filter_v)
	print 'drop f yuzhi: ',filter_v
	print 'drop f ratio: ',len(drop_f_list), len(df.columns) ,len(drop_f_list)*1.0/len(df.columns)
	# print 'drop f ',drop_f_list
	df.drop(drop_f_list,axis=1,inplace=True)
	return df



'''
丢弃波动太小特征
'''
def std_fiter(df, filter_v):
	ls=[]
	for col in df.columns:
		std=df[col].std()
		if(std<filter_v):
			ls.append(col)
	return ls

'''

  # Get a random DataFrame
	df = pandas.DataFrame(numpy.random.randn(25, 3), columns=['a', 'b', 'c'])

	# Make some random categorical columns
	df['e'] = [random.choice(('Chicago', 'Boston', 'New York')) for i in range(df.shape[0])]
	df['f'] = [random.choice(('Chrome', 'Firefox', 'Opera', "Safari")) for i in range(df.shape[0])]
	print df

	# Vectorize the categorical columns: e & f
	df, _, _ = one_hot_dataframe(df, ['e', 'f'], replace=True)
	print df

'''
def  cols_dummy(df , cols ):
	data=df.loc[:,list(set(df.columns)-set(cols))]
	df_ls=[]
	df_ls.append(data)
	for col in cols:
		df_ls.append(pd.get_dummies(df[col],prefix=col))
	return pd.concat(df_ls,axis=1)

from sklearn.feature_extraction import DictVectorizer
def one_hot_dataframe(data, cols, replace=False):
	""" Takes a dataframe and a list of columns that need to be encoded.
		Returns a 3-tuple comprising the data, the vectorized data,
		and the fitted vectorizor.
	"""
	vec = DictVectorizer()
	mkdict = lambda row: dict((col, row[col]) for col in cols)
	# manuplate the column
	vecData = pd.DataFrame(vec.fit_transform(data[cols].apply(mkdict, axis=1)).toarray())
	# get column names
	vecData.columns = vec.get_feature_names()
	vecData.index = data.index
	if replace is True:
		data = data.drop(cols, axis=1)
		#column join based on index
		data = data.join(vecData)
	return (data, vecData, vec)
'''
取值然后修改 sex==male 的所有 sex 数据
titanic.loc[titanic["Sex"] == "male", "Sex"] = 1
'''


# 列转行
# b_group.pivot(values='count', index='userid', columns='view')

def calc_feature_woe_iv(data, tgt_col_name, col_type='continuous' ,bin_size=10,verbose=False):
	"""
	根据用户自定义分组来统计good/bad个数，已经对应的WOE，IV值,版本2
	:param data: 进行分组计算的数据集名称
	:param tgt_col_name: 目标变量名
	:param col_type: 变量类型 。默认连续性
	:param bin_size: 分桶大小。 连续性特征默认10个通
	:param verbose: 输出中间过程
	continuous 类型分箱处理
	discrete  按照类型处理
	"""
	#check
	if 'label' not in data.columns :
		print 'not label in'
		return ''
	# 计算总体样本量，good/bad总数
	dataset=data.loc[:,['label',tgt_col_name]].copy()

	# dataset.dropna(inplace=True)
	total_good  = len(dataset[dataset.label==0])
	total_bad   = len(dataset[dataset.label==1])
	total_count = len(dataset)
	rank_col='rn_'+tgt_col_name
	if col_type=='continuous':
		dataset=dataset.fillna(-999)
		dataset[rank_col]=dataset[tgt_col_name].rank(method='max')/(total_count/(bin_size-1))
		dataset[rank_col]=dataset[rank_col].apply(lambda  x:int(x))
	if col_type=='discrete':
		size=dataset[tgt_col_name].unique().size
		dataset=dataset.fillna(-999)
		if size>100:
			print 'discrete is too much. size is {}'.format(size)
		dataset[rank_col]=dataset[tgt_col_name]
	# 记录每一个分组的统计结果
	grouping_data = []
	# print dataset[rank_col].unique()
	for grp_var, grp_data in dataset.groupby(rank_col):
		# print grp_var
		d = dict()
		d['group_name'] = grp_var
		d['count'] = len(grp_data)
		d['cross'] ='-'.join( [str(min(grp_data[tgt_col_name])),str(max(grp_data[tgt_col_name]))])
		d['total_dist'] = 1.0 * d['count'] / total_count
		d['good'] = len(grp_data[grp_data.label == 0])
		d['good_dist'] = 1.0 * d['good'] / total_good
		d['bad'] = len(grp_data[grp_data.label == 1])
		d['bad_dist'] = 1.0 * d['bad'] / total_bad
		d['bad_rate'] = 1.0 * d['bad'] / d['count']
		if d['bad'] > 0 and d['good'] > 0:
			d['woe'] = np.math.log(1.0 * d['good_dist'] / d['bad_dist'])
		elif d["good"] == 0:
			d['woe'] = -1
		else:
			d["woe"] = 1
		d['iv'] = 1.0 * (d['good_dist'] - d['bad_dist']) * d['woe']
		grouping_data.append(d)

	# 保存到数据集
	grouping_cols = ['group_name', 'count', 'total_dist',
					 'good', 'good_dist', 'bad', 'bad_dist',
					 'bad_rate', 'woe', 'iv']
	grouping_df = pd.DataFrame(grouping_data, columns=grouping_cols)
	total_iv = grouping_df.iv.sum()
	if verbose:
		print tgt_col_name, '  total_iv:', total_iv
	return grouping_df

#IV calc
# def calc_iv_col_funs(data):
# 	'''
# 	计算连续性特征的统计特征，并计算相应的iv值
# 	:param bill_anay:
# 	:return:
# 	'''
# 	funs=['sum','mean','std','min','count']
# 	for col in data.columns[4:]:
# 		print col,'----------------------------------'
# 		data=data.agg({col:['sum','mean',np.std,'min']}).add_prefix(col+'_')
# 		data=data[col+'_'+col].reset_index()
# 		for fun_col in funs:
# 			print fun_col ,calc_feature_woe_iv(data=data,tgt_col_name=col+'_'+fun_col).iv.sum()

def evel_ks_calc(y_pred,y_true):
	import math
	data=pd.DataFrame({'label':y_true, 'rs':y_pred})
	data=data.sort_index(by=['rs'])
	data['sortindex'] =[i for i in xrange(len(data))]
	data['bucket']= pd.cut(data.sortindex ,10,precision=1)
	group=data.groupby('bucket',as_index=False)
	data['bad']=data['label'].map(lambda x: 1 if x==0 else 0)
	data['good']=data['label'].map(lambda x: 1 if x==1 else 0)
	cumsum=group.sum().cumsum()
	kv=group.sum()
	print kv
	cumsum['bad_cumsum_p']=cumsum['bad']*1.0/kv.sum()['bad']
	cumsum['good_cumsum_p']=cumsum['good']*1.0/kv.sum()['good']
	cumsum['ks']= cumsum['bad_cumsum_p']-cumsum['good_cumsum_p']
	ks=np.array([ i for  i in cumsum['ks']]).max()
	return ks

# feature geneartor
def  feature_gen(df ,feature_df,feature_data,iv_limit ):
	'''
	特征的加减乘除
	:param df:
	:param feature_df:
	:param feature_data:
	:return: 合并保留iv值大于阈值的特征
	'''
	feature_df=feature_df.fillna(-1)
	new_fields_count = 0
	cols=feature_df.columns
	iv_cols={}
	def check(value,col, iv_cols):
		if value>iv_limit:iv_cols[name]=value
	for i,col_i in enumerate(cols,0):
		for j,col_j in enumerate(cols,0):
#             print col_i ,col_j
#             print calc_feature_woe_iv(data=feature_data,tgt_col_name=col_i).iv.sum()
#             print calc_feature_woe_iv(data=feature_data,tgt_col_name=col_j).iv.sum()
			labeldf=df.loc[:,['label']]
			if i <= j:
				name = col_i+ "*" + col_j
				labeldf[name]=pd.Series(feature_df[col_i] * feature_df[col_j])
				labeldf=labeldf.fillna(-1)
				iv=calc_feature_woe_iv(data=labeldf,tgt_col_name=name).iv.sum()
				check(iv,name ,iv_cols)
				new_fields_count += 1
			if i < j:
				name =  col_i+ "+" + col_j
				labeldf[name]= pd.Series(feature_df[col_i]  + feature_df[col_j])
				labeldf=labeldf.fillna(-1)
				iv=calc_feature_woe_iv(data=labeldf,tgt_col_name=name).iv.sum()
				check(iv,name ,iv_cols)
				new_fields_count += 1
			if not i == j:
#                 try:
				name = col_i + "/" + col_j
				labeldf[name]= pd.Series(feature_df[col_i]/(feature_df[col_j]+1))
				labeldf.dropna(inplace=True)
				iv=calc_feature_woe_iv(data=labeldf,tgt_col_name=name).iv.sum()
				check(iv,name ,iv_cols)
#                 except: pass
				name =col_i  + "-" + col_j
				labeldf[name]= pd.Series(feature_df[col_i]  - feature_df[col_j])
				labeldf=labeldf.fillna(-1)
				iv=calc_feature_woe_iv(data=labeldf,tgt_col_name=name).iv.sum()
				check(iv,name ,iv_cols)
				new_fields_count += 2
	print "\n", new_fields_count, "new features generated"
	return iv_cols

def  pool_feature_gen(feature_df,cols,iv_limit,verbose=False):
	'''
	特征的加减乘除
	:param df:
	:param feature_df:
	:param feature_data:
	:return: 合并保留iv值大于阈值的特征
	'''
	iv_cols={}
	df=feature_df.loc[:,['label']]
	return_data=[]
	new_fields_count=0
	def check(value,name, labeldf):
		if value>iv_limit:
			if verbose==True: print name,value
			iv_cols[name]=value
			return_data.append(labeldf.loc[:,[name]])
			# new_fields_count+=1
	def f_fun(key):
		i,col_i=key
		for j,col_j in enumerate(cols,0):
#             print col_i ,col_j
#             print calc_feature_woe_iv(data=feature_data,tgt_col_name=col_i).iv.sum()
#             print calc_feature_woe_iv(data=feature_data,tgt_col_name=col_j).iv.sum()
			labeldf=df.loc[:,['label']]
			if i <= j:
				name = col_i+ "*" + col_j
				labeldf[name]=pd.Series(feature_df[col_i] * feature_df[col_j])
				labeldf=labeldf.fillna(-1)
				iv=calc_feature_woe_iv(data=labeldf,tgt_col_name=name).iv.sum()
				check(iv,name ,labeldf)
			if i < j:
				name =  col_i+ "+" + col_j
				labeldf[name]= pd.Series(feature_df[col_i]  + feature_df[col_j])
				labeldf=labeldf.fillna(-1)
				iv=calc_feature_woe_iv(data=labeldf,tgt_col_name=name).iv.sum()
				check(iv,name ,labeldf)
			if not i == j:
#                 try:
				name = col_i + "/" + col_j
				labeldf[name]= pd.Series(feature_df[col_i]/(feature_df[col_j]+1))
				labeldf.dropna(inplace=True)
				iv=calc_feature_woe_iv(data=labeldf,tgt_col_name=name).iv.sum()
				check(iv,name ,labeldf)
#                 except: pass
				name =col_i  + "-" + col_j
				labeldf[name]= pd.Series(feature_df[col_i]  - feature_df[col_j])
				labeldf=labeldf.fillna(-1)
				iv=calc_feature_woe_iv(data=labeldf,tgt_col_name=name).iv.sum()
				check(iv,name ,labeldf)
		print "\n", new_fields_count, "new features generated"
		# return iv_cols

	feature_df = feature_df.fillna(-1)
	new_fields_count = 0
	# cols=feature_df.columns
	# iv_cols={}
	col_ls=[(i,col_i)for i,col_i in enumerate(cols,0)]
	pool=Pool(8)
	pool.map(f_fun ,col_ls)
	return pd.concat(return_data,axis=1)

# gen_f_df=pool_feature_gen(feature_data.iloc[:,:6],feature_data.columns[3:6],0.1,verbose=True)

def calc_cols_funsiv(org_data,cols,iv_limit,group_cols,prefix='',otest=False):
	'''
	:param data: 需要统计的数据
	:param cols: 需要计算iv值的统计字段 比如['time','money']
	:param iv_limit: iv 过滤值
	:param group_cols: 要统计groupby的 字段list ['userid','label']
	:return:
	'''
	groupdata=org_data.groupby(by=group_cols)
	feature_data=groupdata.agg({cols[0]:'count'})[cols[0]].reset_index()
	funs=['sum','mean','std','min','max','median','count']
	for col in cols:
		if len(prefix)>0:col_common=prefix+'_'+col+'_'
		else:col_common=col+'_'
		print col,'----------------------------------'

		data=groupdata.agg({col:funs}).add_prefix(col_common)
		data=data[col_common+col].reset_index()
		funs_cols=[col_common+fun for fun in  funs]
		data[col_common+'cross']=data[col_common+'max']-data[col_common+'min']
		data[col_common+'diff']=data[col_common+'count']/(data[col_common+'cross']+1)
		funs_cols=[col_common+fun for fun in  funs]+[col_common+'cross',col_common+'diff']
		for fun_col in funs_cols:
			if otest==False :
				iv=calc_feature_woe_iv(data=data,tgt_col_name=fun_col,bin_size=20).iv.sum()
				if iv>iv_limit:
					print fun_col,iv
				feature_data[fun_col]=data[fun_col]
			else:feature_data[fun_col]=data[fun_col]
	return feature_data



#数据特征转置
#可以转置多个特征
def data_pivot(merge_data ,index_col,columns_col):
	ls_df=[]
	cols=set(merge_data.columns)-set([index_col,columns_col])
	for col in cols:
		tmpdf=merge_data.pivot(values=col, index=index_col, columns=columns_col).reset_index()
		tmpcols=[str(i) for i in tmpdf.columns ]
		# bank_v_num
		# #不包括关键词
		tmpdf.columns=['_'.join([columns_col,i,col]) if index_col not  in i    else  i   for i in  tmpcols]
		ls_df.append(tmpdf)
	data=pd.concat(ls_df[1:],axis=1)
	data.drop([columns_col,index_col],axis=1,inplace=True)
# 	print ls_df[0].columns
# 	print data.columns
	return pd.concat([ls_df[0],data],axis=1)

def pool_f(data,):
	f_cols=data.columns
	iv_cols=[]
	def  feature_gen(df ,feature_df,cols,iv_limit):
		return_data=[]
		feature_df=feature_df.fillna(-1)
		new_fields_count = 0
		# cols=feature_df.columns
		iv_cols={}
		def check(value,col, labeldf):
			if value>iv_limit:
				iv_cols[name]=value
				return_data.append(labeldf.loc[:,[col]])
		for i,col_i in enumerate(cols,0):
			for j,col_j in enumerate(cols,0):
	#             print col_i ,col_j
	#             print calc_feature_woe_iv(data=feature_df,tgt_col_name=col_i).iv.sum()
	#             print calc_feature_woe_iv(data=feature_df,tgt_col_name=col_j).iv.sum()
				labeldf=df.loc[:,['label']]
				if i <= j:
					name = 'gen_'+col_i+ "*" + col_j
					labeldf[name]=pd.Series(feature_df[col_i] * feature_df[col_j])
					labeldf=labeldf.fillna(-1)
					iv=calc_feature_woe_iv(data=labeldf,tgt_col_name=name).iv.sum()
					check(iv,name ,labeldf)
					new_fields_count += 1
				if i < j:
					name =  'gen_'+ col_i+ "+" + col_j
					labeldf[name]= pd.Series(feature_df[col_i]  + feature_df[col_j])
					labeldf=labeldf.fillna(-1)
					iv=calc_feature_woe_iv(data=labeldf,tgt_col_name=name).iv.sum()
					check(iv,name ,labeldf)
					new_fields_count += 1
				if not i == j:
	#                 try:
	#                 name = col_i + "/" + col_j
	#                 labeldf[name]= pd.Series(feature_df[col_i]/(feature_df[col_j]+1))
	#                 labeldf.dropna(inplace=True)
	#                 iv=calc_feature_woe_iv(data=labeldf,tgt_col_name=name).iv.sum()
	#                 check(iv,name ,iv_cols)
	#                 except: pass
					name = 'gen_'+col_i  + "-" + col_j
					labeldf[name]= pd.Series(feature_df[col_i]  - feature_df[col_j])
					labeldf=labeldf.fillna(-1)
					iv=calc_feature_woe_iv(data=labeldf,tgt_col_name=name).iv.sum()
					check(iv,name ,labeldf)
					new_fields_count += 2
		print "\n", new_fields_count, "new features generated"
		return iv_cols,pd.concat(return_data,axis=1)

def map_sparse_feature_calc_iv(j):
	'''
	电商稀疏特征IV值并发计算
	:param j: 划分的数据块id
	:return:
	step 块大小
	'''
	s=step*(j-1)
	e=step*j
	print s,e
	f_dic={ col:index  for index, col in enumerate(list(f_cols_tmp)[s:e])}
	from scipy.sparse import coo_matrix
	rows, cols, values = [], [], []
	for i, line in enumerate(f_data):
		for cell in line.strip().split(' '):
			col, value = cell.split(':')
	#         print col ,value
	#         print f_dic[col]
			# col, value = cell.split(':')
			if not f_dic.has_key(col):continue
			rows.append(i)
			cols.append(f_dic[col])
			values.append(float(value))
	matrix = coo_matrix((values, (rows, cols)))

	f_csc=matrix.tocsc()
	f_gen_df=pd.SparseDataFrame([ pd.SparseSeries(f_csc[i].toarray().ravel())
								  for i in np.arange(f_csc.shape[0]) ])
	f_gen_df.columns=list(f_cols_tmp)[s:e]
	f_gen_df_label=pd.concat([cate123_8000cdf['label'],f_gen_df],axis=1)

	gen_f_iv_cols=[(col ,calc_val_woe_iv(f_gen_df_label, col, col_type='continuous' ,bin_size=10).iv.sum()) for col  in f_gen_df_label.columns[1:]]
	iv_cols.extend([k for k in gen_f_iv_cols if k[-1]>0.01])

def plot_feature(model_v6,topN=50):
	# %matplotlib inline
	s=pd.DataFrame.from_dict(model_v6.get_fscore(),orient='index')
	s.columns=['weight']
	s.sort_values(by='weight',ascending=False).head(50).plot(kind='bar')


def crs_std(yixin_train_test_X):
	dict={}
	pool=Pool(8)
	pool.map(f_fun ,col_ls)
	cols=yixin_train_test_X.shape[1]
	for i in xrange(cols):
		v=np.std(yixin_train_test_X[:,i].toarray()[:,0])
		dict[i]=v
	return dict

def gridsearch():
	from sklearn.grid_search import GridSearchCV
	xgb_model = xgb.XGBClassifier(objective="binary:logistic", nthread=4)

	# clf = GridSearchCV(
	#     xgb_model,
	#     {
	#         'max_depth': xrange(1,10,2),
	#         'n_estimators':xrange(10,500,20),
	#         'learning_rate': [0.05,0.1,0.15,0.2]
	#     },

	#     verbose=1, scoring='roc_auc'
	# )

	# clf.fit(train_X, train_Y)
	param = {'max_depth':    [i for  i in xrange(1,10,2)] ,
			 'n_estimators': [i for i in xrange(10,500,20)] ,
			 'learning_rate': [0.05,0.1,0.15,0.2]
			}
	clf = GridSearchCV(xgb_model, param, verbose=1, scoring='roc_auc')
	clf.fit(train_X, train_Y)
	auc = roc_auc_score(val_Y, clf.predict(val_X))
	print("auc score: %.5f" % auc)
	print(clf.best_score_)


# 稀疏特征

train_test_Y,train_test_X= load_svmlight_file(path+'train_test.csv')
feature_densedf=pd.DataFrame(train_test_Y.toarray())
feature_densedf.columns=feature_index.feature

# NaN