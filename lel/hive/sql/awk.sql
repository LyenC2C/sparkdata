awk

解释：当NR和FNR相同时,这就说明在对第一个文件进行操作，
a[$1]=$2
表示，建立一个数组，以第一个字段为下标，第二个字段为值。
当NR!=FNR时,说明在对第二个文件进行操作，注意：
这个时候的$1和前面的$1不是同一个东西了，前面的$1表示的是a.txt的第一个字段，
而后面的$1表示的是b.txt的第一个字段。a[$1]表示以b.txt中第一个字段的为下标的值，如果a[$1]有值的话，说明也存在于a.txt文件中，这样就把数据print出来就行了。


aa 12           aa cool d
bb 34           bb wonder s
cc 56           dd good m

awk -v OFS=',' 'NR==FNR{a[$1]=$2;}NR!=FNR && a[$1]{print $1,a[$1],$2,$3}' 1 2




awk 'NR==FNR{a[$1]=$2;}NR!=FNR {if(!($1 in a))print $1}' 1 2

awk 'NR==FNR{a[$1]=$2;}NR!=FNR {if(!($1 in a))print $1}' 2 1

awk -F ',' 'NR==FNR{print $1"\t"$2}' t2 >itemid



awk -vFS='\n' -vORS=',' '$1=$1'
awk -F '=' '{print "\047"$1":""\047"$2"\047"}'