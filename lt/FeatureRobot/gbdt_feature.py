# coding: utf-8

# In[102]:

from svmloader import load_svmfile
from sklearn.model_selection import train_test_split
import xgboost as xgb
import re
import os
import warnings
warnings.filterwarnings("ignore")
from scipy import sparse

'''
GBDT特征构造
思想:先用已有特征训练GBDT模型，然后利用GBDT模型学习到的树来构造新特征，最后把这些新特征加入原有特征一起训练模型.

特征:构造的新特征向量是取值0/1的，向量的每个元素对应于GBDT模型中树的叶子结点。当一个样本点通过某棵树最终落在这棵树的一个叶子结点上，
那么在新特征向量中这个叶子结点对应的元素值为1，而这棵树的其他叶子结点对应的元素值为0。新特征向量的长度等于GBDT模型里所有树包含的叶子结点数之和.
'''

def load(filename):
    '''
    加载libsvm格式数据
    '''
    print "1:load data......"
    x, y = load_svmfile(filename)
    return x, y


# In[104]:

def gbdt_model(X, y, modeltype, depth=4, treenum=300):
    '''
     训练GBDT模型用于构造新特征
     ==============================
    #X,y              np.mat
    #modeltype        线性模型或者回归模型
    #depth            树的深度，最好不超过5。defult=4
    #treenum          树的个数。defult=300

    #return:          训练出的造特征模型
    '''
    print "2:trian model for generating feature....."
    if modeltype == "lin":
        params = {
            'booster': 'gbtree',
            'objective': 'reg:linear',
            # 'scale_pos_weight': float(len(y)-sum(y))/float(sum(y)),
            'eval_metric': 'mae',
            'gamma': 0.2,
            'silent': False,
            'max_depth': depth,
            'lambda': 10,
            'reg_alpha': 0,
            'colsample_bytree': 0.3,
            'min_child_weight': 2.5,
            'eta': 0.015,
            'nthread': 18
        }
    elif modeltype == "log":
        params = {
            'booster': 'gbtree',
            'objective': 'binary:logistic',
            'scale_pos_weight': 484130 / 7183,
            'eval_metric': 'auc',
            'gamma': 0.2,
            'silent': 1,
            'max_depth': depth,
            'lambda': 1000,
            'subsample': 0.7,
            'colsample_bytree': 0.3,
            'min_child_weight': 2.5,
            'eta': 0.016,
            'nthread': 18
        }

    train_X, val_X, train_Y, val_Y = train_test_split(X, y,
                                                      test_size=0.01, random_state=3)
    dval = xgb.DMatrix(val_X, label=val_Y)
    dtrain = xgb.DMatrix(train_X, label=train_Y)
    watchlist = [(dtrain, 'train'), (dval, 'val')]
    evals_result = {}
    model_aft = xgb.train(params, dtrain, num_boost_round=treenum, early_stopping_rounds=100,
                          # feval=feval_ks_calc,
                          evals=watchlist,
                          verbose_eval=200,
                          evals_result=evals_result)
    # 得到树模型
    return model_aft


# In[106]:

def generate_feature(model, x):
    '''
    gbdt构造样本新特征

    输入参数:
    model:提前训练好的用于新特征构造的模型
    X: 样本原特征

    输出:
    样本集合的新特征向量
    '''
    print "3:generating new features........."
    # 1)得到树的叶子节点集合,构成新特征向量,为每个节点分配特征向量index[0~]
    trees = model.get_dump()
    leafs = []
    index = 0
    for i in xrange(len(trees)):
        leaf = {}
        nodes = re.findall('(\d+):leaf', trees[i])
        for node in nodes:
            leaf[int(node)] = index
            index = index + 1
        leafs.append(leaf)
    print "total leafs of all trees(demention of new feature):%s " % str(index)

    # 每个样本落在每棵树的叶子节点编号
    leafindex = model.predict(xgb.DMatrix(x), pred_leaf=True, )  # ntree_limit参数指定用于新特征构造的树的数量

    # 2)字符串拼接方法构造新特征(速度快)
    libsvm = ""
    for x_num in range(leafindex.shape[0]):
        t_num = 0
        for i in leafindex[x_num]:
            if t_num == 0:
                libsvm = libsvm + str(str(rows) + "\t" + str(leafs[t_num][i]) + ":" + "1" + " ")
            else:
                libsvm = libsvm + str(str(leafs[t_num][i]) + ":" + "1" + " ")
            t_num = t_num + 1
        libsvm = libsvm + ("\n")

    print "cache new feature........"
    f = open('./gbdt_tmp.libsvm', "wb")
    f.write(libsvm)
    f.close()

    f_new, id = load("./gbdt_tmp.libsvm")
    os.remove('./gbdt_tmp.libsvm')
    print "clean cache data......"
    # 稀疏矩阵方法速度较慢
    #     samples_num= x.shape[0]
    #     f_csr = csr_matrix((samples_num,index),dtype=np.float64)
    #     tree_num = leafindex.shape[1]
    #     for x_num in xrange(samples_num):
    #         for t_num in xrange(tree_num):
    #             leaf_index = leafindex[x_num][t_num]
    #             vec_index = leafs[t_num][leaf_index]
    #             f_csr[x_num,vec_index]=1

    # 3)合并新旧特征
    x_all = sparse.hstack([f_new, x])
    print "generate feature done......."
    return x_all


# In[108]:

def pre_model(X, y, modeltype, depth=4, treenum=5000):
    '''
     加入新特征之后进行模型训练
     ==============================
    #X,y              数据
    #modeltype        线性模型或者回归模型，lin or log
    #depth            树的深度，最好不超过5。defult=4
    #treenum          树的个数。defult=1000

    return:          预测模型
    '''

    print "4:train model using new feature set......"
    if modeltype == "lin":
        params = {
            'booster': 'gbtree',
            'objective': 'reg:linear',
            # 'scale_pos_weight': float(len(y)-sum(y))/float(sum(y)),
            'eval_metric': 'mae',
            'gamma': 0.2,
            'silent': False,
            'max_depth': depth,
            'lambda': 10,
            'reg_alpha': 0,
            'colsample_bytree': 0.3,
            'min_child_weight': 2.5,
            'eta': 0.015,
            'nthread': 18
        }
    elif modeltype == "log":
        params = {
            'booster': 'gbtree',
            'objective': 'binary:logistic',
            'scale_pos_weight': 484130 / 7183,
            'eval_metric': 'auc',
            'gamma': 0.2,
            'silent': 1,
            'max_depth': depth,
            'lambda': 1000,
            'subsample': 0.7,
            'colsample_bytree': 0.3,
            'min_child_weight': 2.5,
            'eta': 0.016,
            'nthread': 18
        }

    train_X, val_X, train_Y, val_Y = train_test_split(X, y,
                                                      test_size=0.01, random_state=3)
    dval = xgb.DMatrix(val_X, label=val_Y)
    dtrain = xgb.DMatrix(train_X, label=train_Y)
    watchlist = [(dtrain, 'train'), (dval, 'val')]
    evals_result = {}
    model_aft = xgb.train(params, dtrain, num_boost_round=treenum, early_stopping_rounds=100,
                          # feval=feval_ks_calc,
                          evals=watchlist,
                          verbose_eval=200,
                          evals_result=evals_result)
    # 得到树模型
    return model_aft


# In[ ]:

def run(path):
    '''
    代码流程
    '''
    x, y = load(path)  # 加载数据

    model = gbdt_model(x.toarray(), y, modeltype="lin")  # 训练特征构造模型

    x_all = generate_feature(model, x)  # 构造新特征并与原特征合并

    new_model = pre_model(x_all, y, modeltype="lin")  # 使用新特征集合训练预测模型
    return new_model


if __name__ == '__main__':
    data_path = './test_fr.libsvm'
    new_model = run(data_path)






