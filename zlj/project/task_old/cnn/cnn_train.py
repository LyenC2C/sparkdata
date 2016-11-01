# coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

import cv2
import cPickle
import numpy as np
import codecs
import h5py
import theano
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Flatten
from keras.layers.convolutional import Convolution2D, MaxPooling2D
from keras.layers.normalization import BatchNormalization



def word_simialr_score(s1, s2):
    score = 0
    for j in range(len(s1)):
        if s1[j] == s2[j]:
            score += 1
    return score


def words_simmilar_score(word, words):
    word_score = {}
    for Word in words:
        ws = word_simialr_score(word, Word)
        if ws not in word_score.keys():
            word_score[ws] = [Word]
        else:
            word_score[ws].append(Word)
    return word_score


np.random.seed(123)
model_path = './model/type4_model.d5'

chars = cPickle.load(open('model/chars_type4.pkl', 'rb'))
words = cPickle.load(open('model/words_type4.pkl', 'rb'))
cPickle.dump()

chars.append('A')

char_map = {}
for index, w in enumerate(chars):
    char_map[w] = index

f = h5py.File('./model/type4_train_mean_std.h5', 'r')
x_mean = f['x_mean'][:]
x_std = f['x_std'][:][0]
f.close()
model = Sequential()
model.add(Convolution2D(32, 3, 4, 4, border_mode='full', activation='relu'))
model.add(Convolution2D(32, 32, 4, 4, activation='relu'))
model.add(MaxPooling2D(poolsize=(2, 2)))
model.add(Dropout(0.25))
model.add(Convolution2D(64, 32, 4, 4, border_mode='full', activation='relu'))
model.add(Convolution2D(64, 64, 4, 4, activation='relu'))
model.add(MaxPooling2D(poolsize=(2, 2)))
model.add(Dropout(0.25))
model.add(Flatten())
model.add(Dense(64 * 8 * 8, 512, activation='relu'))
model.add(Dropout(0.5))
model.add(Dense(512, 1250, activation='softmax'))
model.load_weights(model_path)
model.compile(loss='categorical_crossentropy', optimizer='adagrad')

batch_size = 128
nb_classes = 10
nb_epoch = 12

import os


def load_data(path, limit):
    imgs = os.listdir(path)
    num = len(imgs)
    data = np.empty((num, 3, 32, 32), dtype="float32")
    label = np.empty((num,), dtype="uint8")
    for i in range(num):
        img = 255 - cv2.imread(path + imgs[i])
        r_im = cv2.resize(img, (32, 32)).transpose()
        arr = np.asarray(r_im, dtype="float32")
        data[i, :] = arr
        label[i] = int(imgs[i].split('-')[0])
    return (data, np.array(label))


(X_train, y_train) = load_data('D:\workdata\code\o')
(X_test, y_test) = load_data('test')

from keras.utils import np_utils

Y_train = np_utils.to_categorical(y_train, nb_classes)
Y_test = np_utils.to_categorical(y_test, nb_classes)


# input image dimensions
img_rows, img_cols = 28, 28
# number of convolutional filters to use
nb_filters = 32
# size of pooling area for max pooling
nb_pool = 2
# convolution kernel size
nb_conv = 3

from keras.datasets import mnist

(X_train, y_train), (X_test, y_test), (m, n) = mnist.load_data()
#
# X_train = X_train.reshape(X_train.shape[0], 1, img_rows, img_cols)
# X_test = X_test.reshape(X_test.shape[0], 1, img_rows, img_cols)
# X_train = X_train.astype('float32')
# X_test = X_test.astype('float32')
# X_train /= 255
# X_test /= 255
# print('X_train shape:', X_train.shape)
# print(X_train.shape[0], 'train samples')
# print(X_test.shape[0], 'test samples')
#
# # convert class vectors to binary class matrices
# Y_train = np_utils.to_categorical(y_train, nb_classes)
Y_test = np_utils.to_categorical(y_test, nb_classes)

model.fit(X_train, Y_train, batch_size=batch_size, nb_epoch=nb_epoch,
          verbose=1, validation_data=(X_test, Y_test))
score = model.evaluate(X_test, Y_test, verbose=0)
print('Test score:', score[0])
print('Test accuracy:', score[1])

model.save_weights('my_model_weights.h5')
