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
def test_dhash(image, hash_size = 16):
    # Grayscale and shrink the image in one step.
    image = image.convert('L').resize(
        (hash_size + 1, hash_size),
        Image.ANTIALIAS,
    )
    pixels = list(image.getdata())
    # Compare adjacent pixels.
    difference = []
    for row in xrange(hash_size):
        for col in xrange(hash_size):
            pixel_left = image.getpixel((col, row))
            pixel_right = image.getpixel((col + 1, row))
            difference.append(pixel_left > pixel_right)
    # Convert the binary array to a hexadecimal string.
    decimal_value = 0
    hex_string = []
    for index, value in enumerate(difference):
        if value:
            decimal_value += 2**(index % 8)
        if (index % 8) == 7:
            hex_string.append(hex(decimal_value)[2:].rjust(2, '0'))
            decimal_value = 0
    return ''.join(hex_string)

def find_sim(userpath, hashfunc = imagehash.average_hash):
    image_filenames = [os.path.join(userpath, path) for path in os.listdir(userpath) if is_image(path)]
    images={}
    f=open(sys.argv[3])
    shopitem_dic={}
    list=[]
    for line in f:
        dv=line.split()
        shopid=dv[1]
        itemid=dv[2]
        shopitem_dic[itemid]=shopid
    for img in sorted(image_filenames):
        # hash = hashfunc(Image.open(img))
        hash = test_dhash(Image.open(img))
        itemid=img.split('/')[-1].split('_')[0]
        list.append((img.split('/')[-1],shopitem_dic.get(itemid),str(hash)))
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
                print "-".join(item1),"-".join(item2),sim


def just_sim(userpath):
    images={}
    f=open(sys.argv[3])
    shopitem_dic={}
    list=[]
    for line in f:
        dv=line.split()
        shopid=dv[1]
        itemid=dv[2]
        shopitem_dic[itemid]=shopid
    fr=open(userpath)
    for img in fr:
        # hash = hashfunc(Image.open(img))
        # hash = test_dhash(Image.open(img))
        item,hash=img.split()
        itemid=item.split('_')[0]
        list.append((item,shopitem_dic.get(itemid),str(hash)))
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
                print "-".join(item1),"-".join(item2),sim
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
 python dhash.py  dhash  /home/zlj/data/pic_cat/suit/  /home/zlj/data/tmallint_item >log_test_dhash_16
""" % sys.argv[0])
        sys.exit(1)

    hashmethod = sys.argv[1] if len(sys.argv) > 1 else usage()
    if hashmethod == 'ahash':
        hashfunc = imagehash.average_hash
    elif hashmethod == 'phash':
        hashfunc = imagehash.phash
    elif hashmethod == 'dhash':
        hashfunc = imagehash.dhash
    elif hashmethod=='justhash':
        userpath = sys.argv[2] if len(sys.argv) > 2 else "."
        just_sim(userpath=userpath)
        sys.exit(1)
    else:
        usage()
    userpath = sys.argv[2] if len(sys.argv) > 2 else "."
    find_sim(userpath=userpath, hashfunc=hashfunc)