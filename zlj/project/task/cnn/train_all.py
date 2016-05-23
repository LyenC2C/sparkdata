
#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

# from __future__ import print_function
from keras.datasets import cifar10
from keras.preprocessing.image import ImageDataGenerator
from keras.models import Sequential, Graph
from keras.layers.core import Dense, Dropout, Activation, Flatten, MaxoutDense
# from keras.layers.convolutional import Convolution2D, MaxPooling2D, AveragePooling2D
from keras.layers.convolutional import Convolution2D, MaxPooling2D,AveragePooling2D
from keras.optimizers import SGD, Adadelta, Adagrad, adam,Adam
from keras.utils import np_utils, generic_utils
import numpy as np
# import matplotlib.pyplot as plt
import theano
from keras.layers.normalization import BatchNormalization
# from data_utils import *
nb_classes = 5
nb_epoch = 100

import h5py
import  os
import  cv2

img_size=32
letter=['0','1','2','3','4','5','6','7','8','9','a','c','d','e','j','y']
# letter=['0','1','2','3','4','5','6','7','8','9','a','c','e','j']
letter_map={}
map_letter = {}
batch_size = 2
nb_classes = 16
nb_epoch = 50
for i,w in enumerate(letter):
    letter_map[w] = i
    map_letter[i] = w
#

def load_data(path,size):
    label=[]
    # print path
    pics= os.listdir(path)
    imgs=[]
    for i in xrange(size):
        imgs.extend(pics)
    import random
    random.shuffle(imgs)
    num = len(imgs)
    # print num ,path
    data = np.empty((num,3,img_size,img_size),dtype="float32")
    flag='test' in path
    lable1=[]
    lable2=[]
    lable3=[]
    lable4=[]
    lable5=[]
    for i in xrange(num):
        name=imgs[i]
        #img = 255- cv2.medianBlur(cv2.imread(path+name),3)
        #img = 255- cv2.imread(path+name)[4:-4, :, :]
        # print 
        img = 255- cv2.imread(path+name)
        #r_im=cv2.resize(img, (img_size, img_size),interpolation=cv2.INTER_ARER).transpose()
        r_im=cv2.resize(img, (img_size, img_size)).transpose()
        arr = np.asarray(r_im,dtype="float32")
        data[i,:] = r_im
        # print r_im.shape
        # if '_' not in name:continue
        # print name ,name.split('_')[0]
        w1,w2,w3 =[i for  i in name.split('_')[0].strip()]
        lable1.append(letter_map.get(w1,letter_map['e']))
        lable2.append(letter_map.get(w2,letter_map['e']))
        lable3.append(letter_map.get(w3,letter_map['e']))
        lable4.append(letter_map.get('d'))
        lable5.append(letter_map.get('y'))
        # index=letter_map.get(w,letter_map['e'])
        # label.append(index)
        # if flag>0:  label1.append((index,name))
    return (data,np.array(lable1),np.array(lable2),np.array(lable3),np.array(lable4),np.array(lable5))

# def try_load_data(path):
#     try:
#         return load_data(path)
#     except:
#         pass 

data_path='test_allmarked_v1'
# data_path='test_all'
X_train, y_train_1, y_train_2,y_train_3,y_train_4,y_train_5 = load_data('./image/'+data_path+'/train1/',3)
X_test, y_test_1, y_test_2,y_test_3,y_test_4,y_test_5 = load_data('./image/'+data_path+'/test1/',1)

print('X_train shape:', X_train.shape)
print(X_train.shape[0], 'train samples')

f = h5py.File('./model/type4_train_mean_std.h5', 'r')
x_mean = f['x_mean'][:]
x_std = f['x_std'][:][0]
f.close()

X_train = (X_train-x_mean)/x_std

X_test =  (X_test-x_mean)/x_std


Y_train_1 = np_utils.to_categorical(y_train_1, nb_classes)
Y_train_2 = np_utils.to_categorical(y_train_2, nb_classes)
Y_train_3 = np_utils.to_categorical(y_train_3, nb_classes)
Y_train_4 = np_utils.to_categorical(y_train_4, nb_classes)
Y_train_5 = np_utils.to_categorical(y_train_5, nb_classes)

Y_test_1 = np_utils.to_categorical(y_test_1, nb_classes)
Y_test_2 = np_utils.to_categorical(y_test_2, nb_classes)
Y_test_3 = np_utils.to_categorical(y_test_3, nb_classes)
Y_test_4 = np_utils.to_categorical(y_test_4, nb_classes)
Y_test_5 = np_utils.to_categorical(y_test_5, nb_classes)

# dense = Sequential()
# dense.add(Dense(5, input_shape=(10,), activation='relu'))

model = Graph()
model.add_input('input',input_shape=(3,32,32))

kernel_size=4
model.add_node(layer=Convolution2D(32,kernel_size,kernel_size, activation='relu'),name='conv1',input='input')
model.add_node(layer=MaxPooling2D(pool_size=(2, 2)),name='pool1',input='conv1')
# model.add_node(layer=AveragePooling2D(pool_size=(2, 2)),name='pool1',input='conv1')

model.add_node(layer=Dropout(0.25), name='drop1',input='pool1')

model.add_node(layer=Convolution2D(64, kernel_size, kernel_size,activation='relu'),name='conv2',input='drop1')

model.add_node(layer=Convolution2D(64, kernel_size, kernel_size,activation='relu'),name='conv3',input='conv2')

model.add_node(layer=MaxPooling2D(pool_size=(2, 2)),name='pool2',input='conv3')
# model.add_node(layer=AveragePooling2D(pool_size=(2, 2)),name='pool2',input='conv2')

model.add_node(layer=Dropout(0.25), name='drop2',input='pool2')

# model.add_node(layer=Convolution2D(64, 3, 3,activation='relu'),name='conv3',input='pool2')
# # model.add_node(layer=MaxPooling2D(pool_size=(2, 2)),name='pool2',input='conv2')
# model.add_node(layer=AveragePooling2D(pool_size=(2, 2)),name='pool3',input='conv3')


model.add_node(layer=Flatten(),name='flatten',input='drop2')

model.add_node(layer=Dense(64 * 8 * 8, activation='relu'),name='dense1',input='flatten')
model.add_node(layer=Dropout(0.5), name='drop3',input='dense1')


model.add_node(layer=Dense(512, activation='relu'),name='dense2',input='drop3')

model.add_node(Dense(nb_classes, activation='softmax'), name='output_dense1', input='dense2')
model.add_node(Dense(nb_classes, activation='softmax'), name='output_dense2', input='dense2')
model.add_node(Dense(nb_classes, activation='softmax'), name='output_dense3', input='dense2')
# model.add_node(Dense(nb_classes, activation='softmax'), name='output_dense4', input='dense2')
# model.add_node(Dense(nb_classes, activation='softmax'), name='output_dense5', input='dense2')
model.add_output(name='output1', input='output_dense1')
model.add_output(name='output2', input='output_dense2')
model.add_output(name='output3', input='output_dense3')
# model.add_output(name='output4', input='output_dense4')
# model.add_output(name='output5', input='output_dense5')
# model.summary()

# let's train the model using SGD + momentum (how original).
# 
sgd = SGD(lr=0.005, decay=1e-6, nesterov=True)
adam=Adam(0.001)
model.compile(optimizer=sgd,metrics=["accuracy"],loss={'output1': 'categorical_crossentropy',
 'output2': 'categorical_crossentropy', 'output3': 'categorical_crossentropy'})
 # ,'output4': 'categorical_crossentropy', 'output5': 'categorical_crossentropy'})

print('Not using data augmentation or normalization')
model.fit({'input':X_train, 'output1':Y_train_1, 'output2':Y_train_2,'output3':Y_train_3}, verbose=1,
    batch_size=batch_size, nb_epoch=nb_epoch,
    validation_data={'input':X_test, 'output1':Y_test_1, 'output2':Y_test_2,'output3':Y_test_3})

# model.save_weights('my_model_train_all_weights.h5')


# model.load_weights('my_model_train_all_weights.h5')

# score = model.evaluate(X_test, Y_test_1,Y_test_2,Y_test_3, verbose=1,show_accuracy=True)
score = model.evaluate({'input':X_test, 'output1':Y_test_1, 'output2':Y_test_2,'output3':Y_test_3,})

# get_predict_score = theano.function([model.layers[0].input],
#                                     model.layers[-1].get_output(train=False),
#                                     allow_input_downcast=True)
# print  score, type(score)
print('Test score:', score[0])
print('Test accuracy:', score[1])


data={'input':X_test}
classes = model.predict(data, verbose=0)

print classes.keys()
# print classes

# out1=classes.get('output1')
# out2=classes.get('output2')
# out3=classes.get('output3')

num=len(Y_test_1)

# out1=classes.get('output1')
# out2=classes.get('output2')
# out3=classes.get('output3')
# print(type(out1))
# num=len(Y_test_1)
# for i in xrange(num):
#     c1=[t for t in out1[i]]
#     c2=[t for t in out2[i] ]
#     c3=[t for t in out3[i] ]
#     # print max(c1),max(c2),max(c3)
#     # print type(c1),c1,[i for i  in c1]
#     yt='_'.join([map_letter[c1.index(max(c1))],map_letter[c2.index(max(c2))], map_letter[c3.index(max(c3))]])
#     yr='_'.join([map_letter[y_train_1[i]],map_letter[y_train_2[i]],map_letter[y_train_3[i]] ] )
#     rs= 1 if yt.strip()==yr.strip() else 0
#     print yr, yt, rs