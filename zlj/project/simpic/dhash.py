__author__ = 'zlj'
from PIL import Image

import imagehash
import distance
"""
Demo of hashing
"""


def is_image(filename):
	f = filename.lower()
	return f.endswith(".png") or f.endswith(".jpg") or \
			f.endswith(".jpeg") or f.endswith(".bmp") or f.endswith(".gif")
# def find_similar_images(userpath, hashfunc = imagehash.average_hash):
# 	image_filenames = [os.path.join(userpath, path) for path in os.listdir(userpath) if is_image(path)]
# 	images={}
#     for img in sorted(image_filenames):
#         hash = hashfunc(Image.open(img))
#         itemid=img.split('/')[-1].split('_')[0]
#         print itemid
#         images[hash] = images.get(hash, []) + [img]
#
#     for k, img_list in images.iteritems():
#         if len(img_list) > 1:
#         print(" ".join(img_list))
def find_sim(userpath, hashfunc = imagehash.average_hash):
    image_filenames = [os.path.join(userpath, path) for path in os.listdir(userpath) if is_image(path)]
    images={}
    f=open(sys.argv[2])
    shopitem_dic={}
    list=[]
    for line in f:
        dv=line.split()
        shopid=dv[1]
        itemid=dv[2]
        shopitem_dic[itemid]=shopid
    for img in sorted(image_filenames):
        hash = hashfunc(Image.open(img))
        itemid=img.split('/')[-1].split('_')[0]
        list.append((itemid,shopitem_dic.get(shopitem_dic),hash))
        # itemid=img.split('/')[-1].split('_')[0]
        # print itemid
        # images[hash] = images.get(hash, []) + [img]
    num=len(list)
    for i in xrange(num):
        item1=list[i]
        for  j in xrange(num-i):
            item2=list[i+j]
            if item1[1]==item2[1]:continue
            sim=distance.hamming(item1[2],item2[2])
            if sim<4:
                print "_".join(item1),"_".join(item2),sim

    # for k, img_list in images.iteritems():
    #     if len(img_list) > 1:
    #         s=set([i.split('/')[-1].split('_')[0] for i in img_list])
    #         if len(s>1):
    #             print(" ".join(s))
if __name__ == '__main__':
    import sys, os
    def usage():
        sys.stderr.write("""SYNOPSIS: %s [ahash|phash|dhash] [<directory>]
Identifies similar images in the directory.
Method:
  ahash: Average hash
  phash: Perceptual hash
  dhash: Difference hash
(C) Johannes Buchner, 2013
""" % sys.argv[0])
        sys.exit(1)

    hashmethod = sys.argv[1] if len(sys.argv) > 1 else usage()
    if hashmethod == 'ahash':
        hashfunc = imagehash.average_hash
    elif hashmethod == 'phash':
        hashfunc = imagehash.phash
    elif hashmethod == 'dhash':
        hashfunc = imagehash.dhash
    else:
        usage()
    userpath = sys.argv[2] if len(sys.argv) > 2 else "."
    find_sim(userpath=userpath, hashfunc=hashfunc)