# coding: utf-8
__author__="cwl"

import warnings
warnings.filterwarnings("ignore")
from feature_cross import *
import pandas as pd
import numpy as np
import matplotlib as mpl

mpl.rcParams["font.sans-serif"] = ["FangSong"]
mpl.rcParams["axes.unicode_minus"] = False
mpl.style.use('ggplot')


def run(path1, path2):
    print "正在读取文件，请稍后......"
    featds = pd.read_csv(path1)
    featwb = pd.read_csv(path2)
    feat = pd.merge(featds, featwb, left_on='tel', right_on="t1.tel", how='left')

    y = np.mat(feat.loc[:, "label"]).T

    featk = feat.drop(["t1.tel", "label"], axis=1)

    X = np.mat(featk.iloc[:, 1:])

    head = np.mat(featk.columns[1:])

    print "特征机器人已经被唤醒，开始工作！"

    robot = FeatureRobot(X, y, header=head,
                      funs=["x^2+y^2", "1/x", "*", "/", "log", "1/x+1/y"],
                      domain=[range(49), range(49, 54)],
                      nullnum=0.90, xgbnum=1,
                      feature2labelcorr=0.001, corrtype="spearman", modeltype="log",
                      block=10, topk=400, path="/mnt/hdfs/data3/data_cwl")
    return robot


def see(see):
    see.feature_importance()
    see.ks()
    see.rocplot()


if __name__=="__main__":
    path1 = "....."#电商特征
    path2 = "....."#微博特征
    #运行
    robot = run(path1, path2)
    #可视化
    see(robot)

