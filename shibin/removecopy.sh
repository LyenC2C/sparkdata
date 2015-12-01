#!/bin/bash
dirname=$1
echo 'pre pare to clean the copy in '$dirname

#list -R the dir&file
hadoop fs -ls -R $1|

#exclude the dirs
grep -v '^d'|

#get the size datetime filepath
awk '{print $5"\t"$6"-"$7"\t"$8}'|

#sort by filepath reserved  and datetime and size reserved
sort -k3r -k2 -k1nr|

#add file's father
awk '{
      printf("%s\t%s\t%s\t",$1,$2,$3);
      sub(/_copy_[0-9]+/,"",$3);
      printf("%s\n",$3);
     }'|

#awk del the copy file
# which some copy has same father and same size just keep only one,
#It should be the latest.but there use the oldest one 
#if you want use the latest,just remove the 'r' in sort first choice

awk '{
       
       file=$3;
       fatherfileAndsize=$4"\t"$1;
       
       all_file[file]=fatherfileAndsize;

       file_samesize_count[fatherfileAndsize]+=1;
       
       #file in this should not be delete. because this is the latest copy or the raw
       file_samesize_latestfile[fatherfileAndsize]=file;
     }
     END{
        for (file  in all_file)
        {  
           fatherfileAndsize=all_file[file];
           if(file_samesize_count[fatherfileAndsize]>1)
           {
              latestfile=file_samesize_latestfile[fatherfileAndsize]          
              if (file!=latestfile)
              {
                 print file;
              }
           }
        }
     }'|

#list they
xargs -I {} hadoop fs -ls {}

#or remove they
#xargs -I {} hadoop fs -rm {}

