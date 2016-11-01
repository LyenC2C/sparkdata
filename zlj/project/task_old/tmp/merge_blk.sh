
# 迁移

awk -F "\t" 'NR==FNR {a[$2]=$1;next}{b[$2]=$1} END{for(k in a) if(k in b) print a[$2],$2,path"\t"$0}'   file1  file2

