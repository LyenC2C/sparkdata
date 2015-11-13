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


'''
sim type  cat_file  hash_file
'''
def find_sim():
    # image_filenames = [os.path.join(userpath, path) for path in os.listdir(userpath) if is_image(path)]
    # images={}
    f=open(sys.argv[2])
    shopitem_dic={}
    list=[]
    type_item_set=set()
    for line in f:
        dv=line.split()
        type=dv[0]
        shopid=dv[1]
        itemid=dv[2]
        shopitem_dic[itemid]=shopid
        if type=='':
            type_item_set.add(itemid)
    for line in open(sys.argv[3]):
        # hash = hashfunc(Image.open(img))
        # hash = test_dhash(Image.open(img))
        img,hash=line.split()
        if 'home' in img:
            itemid=img.split('/')[-1].split('_')[0]
        else:
            itemid=img.split('_')[0]
        if itemid in type_item_set:
            list.append((img,shopitem_dic.get(itemid),str(hash)))
    num=len(list)
    print num
    for i in xrange(num):
        item1=list[i]
        for  j in xrange(num-i):
            item2=list[i+j]
            if item1[1]==item2[1]:continue
            sim=distance.hamming(item1[2],item2[2])
            if sim<4:
                print "-".join(item1),"-".join(item2),sim

'''
gen ahash  prex  file
'''
def gen():
    hashmethod = sys.argv[2]
    if hashmethod == 'ahash':
        hashfunc = imagehash.average_hash
    elif hashmethod == 'phash':
        hashfunc = imagehash.phash
    elif hashmethod == 'dhash':
        hashfunc = imagehash.dhash
    else:
        print 'error'
    prex=sys.argv[3]
    path=sys.argv[4]
    item=''
    if( prex in path):
        item=path.split('/')[-1]
    else :
        path=prex+path
        item=path

    if is_image(path):
        hash=hashfunc(Image.open(path))
        if '00000000' not in hash:
            print item ,hash


if __name__ == '__main__':
    import sys

    method=sys.argv[1]
    if method=='gen':
        gen()
    elif method=='sim':
        find_sim()
