#!/bin/bash

echo " Start uploading depending on new mapped (and harvested) records available"

lhost=$(hostname)
WORK=$PWD
TODAY=`date +\%F`
YESTERDAY=`date --date='1 day ago' +\%F`
TWODAGO=`date --date='2 day ago' +\%F`
WEEK=`date +\%U`
upload_list="upload_list_${TODAY}"
##upload_list="upload_list_datacite_${TODAY}"
##HEW-!!! upload_list="${WORK}/upload_list_${YESTERDAY}"

##epiccheck=''
##ckancheck="--ckan_check='True'"
case $lhost in
  eudat-b1.dkrz.de)
   rhost=eudat-b1.dkrz.de:8080
##
   epiccheck='--epic_check credentials_11098'
   ;;
  eudatmd1.dkrz.de) ## b2find.eudat.eu :8000
   rhost=b2find.eudat.eu:8080
   epiccheck='--epic_check credentials_11098'
   ;;
  *)
   rhost=eudat6b.dkrz.de
   ;;
esac
msgu="[INFO] |-+Upload from $lhost to $rhost ..."



cd ${WORK}
if [ -r ${upload_list} ]
then
  nthreads=25 # maximal number of parallel job threads

  total_lines=$(cat ${upload_list} | wc -l)
  if [ $total_lines -ge $nthreads ]
  then
    echo "[INFO] Split ${upload_list} in $nthreads parts to run upload in parallel"
    ((lines_per_file = 1 + total_lines / nthreads))
  else
    lines_per_file=1 
  fi
  echo "lines_per_file=${lines_per_file}"
  split --lines=${lines_per_file} ${upload_list} ${upload_list}.
else
  echo "[WARNING] Can not acess ${upload_list} => processing stopped"
fi


ntotrec=0
nsucc=0
nfiles=0
errout=''
for file in $(ls ${upload_list}.*)
do
  (( nfiles = nfiles + 1))
  npfrec=0
  while read community oaiurl dir mdprefix set 
  do
    setdir="${dir}/${set}"
    ndirrec=$(ls ${setdir}/json/*.json 2>/dev/null | wc -l)
    echo -e "\t\t$ndirrec json records provided in directory ${setdir}/json"
    ((npfrec = npfrec + ndirrec)) 
  done < "$file"
  echo -e "\tProcess $npfrec json records provided in directories of $file"
  ((ntotrec = ntotrec + npfrec ))

  nohup ./eudat_jmd_manager.py -l $file --mode u -i $rhost $ckancheck $epiccheck 2>log/${file}.out >log/${file}.err && mv $file DONE/ &
done
echo " $ntotrec json records provided in dirs of $upload_list "
wait
if [ $nfiles -eq 0 ]
then
  echo "[ERROR] : No processing lists provided"
  exit -1
elif [ $nsucc -ne $nfiles ]
then
  echo -e "[WARNING] : Only $nsucc records (out of $nfiles ) uploaded "
  exit -1
fi

mv ${upload_list} DONE/
echo "[INFO] End of parallel processing of DONE/${upload_list} at $(date)"

exit 0
