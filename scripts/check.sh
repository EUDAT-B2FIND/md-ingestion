#!/bin/bash

############################################################################### 
#     File       : ${md-ingestion}/scripts/check.sh
#
#     Heinrich Widmann, DKRZ                        Feb 2014
#      updated (adapt to GitHUB structure)          Apr 2014
#
#     This script is used to check B2FIND metadata mapping and upload
#
#     Usage :
#     -------
#     cron_check.sh [OPTION]... community
#
#        community is a supported B2FIND community name
#
#        Options are
#           --help, -h                  display built-in help text and exit. 
#           --mode, -m MODE             processing mode, one of b,c or u or a for all (c is default)
#           --joblist, -j JOBLIST       file where the job list is specified (default is check_list)
#
#     Version:  1.1
#     --------
#

#----- function usage() --------------------
#
#    display short help text and exit
#

err_strg=''
b=\\033[1m  ; it=\\033[3m  ; u=\\033[4m ; n=\\033[0m
usage() {
  printf "${b}err_strg${n}\n"
  printf "${b}NAME${n}\n\t`basename $0` - check mapping of B2FIND\n\n" 
  printf "${b}SYNOPSIS ${n}\n\t${b}`basename $0`${n}  [${u}OPTION${n}]...  "
  printf "${u}COMMUNITY${n}\n\n" 
  printf "${b}DESCRIPTION ${n}\n\t checks mapping on sample XML records and upload sucessfully mapped JSON records to test CKAN (VM). This script has to be called from root directory 'md-ingestion'.\n\n" 
  printf "\t${u}COMMUNITY${n}\n\t\tB2FIND community (by default all communities found in job_file will be processed).\n\n"
  printf "${b}OPTIONS${n}\n"
  printf "\t${b}--help, -h${n}\n\t\tdisplay this built-in help text and exit.\n" 
  printf "\t${b}--mode, -m${n} ${u}MODE${n}\n\t\tprocessing mode, one of b, c or u or a for all (c is default)\n"
  printf "\t${b}--joblist, -j JOBLIST${n} ${u}ID${n}\n\t\tfile where the job list is specified (default is check_list).\n"
  exit 1
}

WORK=$PWD

# test working directory:
BASE_DIR=$(basename $WORK)

if [ ! $BASE_DIR = 'md-ingestion' ]
then
    printf "ERROR: This script has to be called from root directory 'md-ingestion' by\n\t md-ingestion > scripts/check.sh\n"
    exit
fi

## ----------- get options and community name -------------

job_list="${WORK}/check_list"
mode='c'
comm=''
while [[ -n $1 ]] ; do
  case $1 in
        --help       | -h) usage ;;
        --mode       | -m) mode=$2; shift ;;
        --joblist    | -j) job_list=$2; shift ;;
        -*               ) err_strg="\nERROR: invalid option $1 !\n"
                          [[ "$1" != $( echo "$1" | tr -d = ) ]] && \
                          err_strg=$err_strg"  Please use blank insted of '=' to separate option's argument.\n"
                          usage ;;
        *                ) comm=$1 ;;
  esac
  shift
done

if [ -n "$comm" ]
then
 grep ^$comm ${job_list} > ${WORK}/check_${comm}_list
 job_list="${WORK}/check_${comm}_list"
fi 

lhost=$(hostname)
TODAY=`date +\%F`
WEEK=`date +\%U`
logfile="/tmp/jmd_check_$$.log"

[ -d ${WORK} ] || { echo "[ERROR] Can not access working directory ${WORK} !"; exit -1;}
cd ${WORK}

##cd target/current
## if new test data available perform converting and upload
actionreq="yes"
msg=''


ntotrec=0
nsucc=0
nfiles=0
errout=''
msgc="[INFO] |-+Convert ..."
epiccheck=''
case $lhost in
  eudat-b1.dkrz.de)
   export JAVA_HOME=/usr/local/jdk1.7.0_25
   rhost=eudat-b1.dkrz.de
##   epiccheck="--epic_check credentials_11098"
   ckancheck="--ckan_check \"True\""
   ;;
  eudatmd1.dkrz.de) ## b2find.eudat.eu :8000
   export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.51.x86_64/jre
   rhost=b2find.eudat.eu:8080
   ckancheck="--ckan_check 'True'"
   epiccheck="--epic_check credentials_11098"
   ;;
  *)
#   export JAVA_HOME=/usr/local/jdk1.7.0_25
#   rhost=eudat6a.dkrz.de
   rhost=eudat6b.dkrz.de
   ckancheck="--ckan_check 'True'"
   ;;
esac


msgu="[INFO] |-+Upload from $lhost to $rhost ..."
echo "[INFO] Update and deploy mapping depending on new sources available in SVN"

if [ $mode = 'a' ] || [ $mode = 'c' ] || [ $mode = 'b' ] 
then
  cd ${WORK}/../md-mapper
  if [ $mode = 'b' ] && [ -d target/current ]
  then
    rm -rf target/current
  fi
  ## check for changed SVN sources
  ## mapfiles=`svn list https://svn.eudat.eu/EUDAT/Services/MetaData/converter/java/src/main/resources/mapfiles`
  ##oldmf_svn_res=`svn info src/main/resources/mapfiles | grep 'Revision:' | cut -f2 -d' ' | cut -c-4`
  ##general_map_changes=`svn diff | grep Index | grep -v src/main/resources/mapfiles | grep 'Index:' | cut -f2 -d' '`
  ##mapfiles_changed=`svn diff | grep Index | grep src/main/resources/mapfiles | grep 'Index:' | cut -f2 -d' '`
  ## update and deploy, if new version
  ./update-deploy.pl -f 
  ##[ -d target/current ] || { echo "ERROR Sub directory target/current can not accessed in $PWD"; exit -1;}
fi

if [ $mode = 'b' ]
then
  exit 0
fi

cd ${WORK}

if [ -s ${job_list} ]
then
  echo -e "\n[INFO] ++++ Loop over jobs specified in ${job_list}"
  nj=0
  while read comm oaiurl mdformatpath mdformat subset
  do
    (( nj = nj + 1 ))
    ucomm=$(echo "$comm" | tr '[:lower:]' '[:upper:]')
    ## mdformat=$(basename $mdformatpath)
    echo -e "\n[INFO] + %%%%% $ucomm / $mdformat %%%% " ## $(cat $file)"
    echo -e "\n[INFO] |- Process data in ${pwd}/${mdformatpath}/${subset}"
    ## Status check
    nxml=0; totnxml=0
    njson=0; totnjson=0
    nckan=0; totnckan=0
    echo -e "\n[INFO] |- Status of harvested records before processing"
##    for set in $(ls $mdformatpath )
##    do
    nxml=$(ls ${mdformatpath}/${subset}/xml/*.xml | wc -l)
    (( totnxml=totnxml+nxml )) 
##    done
    echo -e "[INFO]  No. of harvested xml files in\n ${mdformatpath}/${subset}/xml \t $totnxml"

    ## Jobs
    echo -e "[INFO] Processing ... \n [INFO] \t"
    echo "$comm $oaiurl $proc $mdformatpath $mdformat $subset" | tee  jobfile_$nj
    coutfile="log/converttest_${TODAY}_${nj}.out"
    cerrfile="log/converttest_${TODAY}_${nj}.err"
    convertJob="./eudat_jmd_manager.py -l jobfile_$nj --mode c >${coutfile} 2>${cerrfile} && echo '$(date) : test convert job finished' >> $logfile || status=$? ;"
    uoutfile="log/uploadtest_${TODAY}_${nj}.out"
    uerrfile="log/uploadtest_${TODAY}_${nj}.err"    
    uploadJob="nohup ./eudat_jmd_manager.py -l jobfile_$nj --mode u -i $rhost $ckancheck $epiccheck >${uoutfile} 2>${uerrfile} && echo -e \"$(date) : test upload job finished\" >> $logfile || status=$? ;"
    ## Processing
    status=0

    if [ $mode = 'a' ] || [ $mode = 'c' ] 
    then

      echo -e "$msgc \n $convertJob \n"
      if [ -d $mdformatpath/${subset}/json ]
      then
        rm -rf $mdformatpath/${subset}/json/*.json 2> /dev/null
      else
        mkdir $mdformatpath/${subset}/json
      fi

      eval $convertJob
      echo "Status $?"
      ## Status check after converting
      njson=0; totnjson=0
      echo -e "\n[INFO] |- Status after converting with mapfile\n ../mapper/current/mapfiles/${comm}-${mdformat}.xml"
      njson=$(ls $mdformatpath/${subset}/json/*.json 2>/dev/null | wc -l)
      (( totnjson=totnjson+njson ))
      echo -e "[INFO]  No. of converted json files in\n $mdformatpath/${subset}/json \t $njson"

      cerrors=$(grep -i ERROR ${cerrfile})
      if [ $status != 0 ] && [ -n "$cerrors" ]
      then
        echo -e "[INFO] | | |   Job $convertJob exits with status $status and results in errors\n$cerrors\n"
        echo -e "[INFO] | | |   Please fix these errors before subsequent upload can be performed !!\n"
        continue
      elif [ $njson == 0 ]
      then
        echo -e "[INFO] | | |   No records converted\n"
        continue
      fi

      cp ../mapper/current/stats.log ${WORK}/log/stats_${comm}.log
      echo -e "[INFO] | |-+ Check mapping statistics :"
      while read f1 Fac facet xpath endline
      do
        if [ "$Fac" == 'Facet:' ]
        then
          ##facetmsg="[INFO] | | |-+ $Fac $facet :"
	  echo -e "\b[INFO]  |-+ $facet :"
        elif [ "$xpath" == 'XPath' ]
        then
          ##echo -e "\b[INFO]    |- $facetmsg :"
	  echo -e "\b[INFO]     |- Mapped:\t$Fac $facet"
          continue
        elif [ "$xpath" == 'unmapped' ]
        then
          echo -e "\b[WARNING]  |- Unmapped:\t$Fac $facet"
        else
          continue
        fi
      done < ../mapper/current/stats.log
    
      echo -e "[INFO] | No errors found during converting\n"
    fi

    if [ $mode = 'a' ] || [ $mode = 'u' ] 
    then
      echo -e "$msgu"
      eval $uploadJob
      ## Status check after upload
      nckan=0 ; totnckan=0
      echo -e "\n[INFO] |- Status after uploading"
      echo $PWD
      nckan=$(egrep -c 'Uploaded' ${uerrfile})
      nupl=$(egrep -c "No upload required" "${uerrfile}" )
      nfailed=$(egrep -c "failed" "${uerrfile}" )
      (( totnckan=totnckan+nckan+nupl ))
      echo -e "[INFO]  No. of uploaded datasets             \t $nckan"
      echo -e "[INFO]  No. of 'No upload required' datasets \t $nupl"
      echo -e "[INFO]  No. of failed uploads                \t $nfailed"

      uerrors=$(grep -i ERROR ${uoutfile} ${uerrfile} )
      if [ $status != 0 ] || [ -n "$uerrors" ] || [ $totnckan == 0 ]
      then
        echo -e "[ERROR] | | |   Job\n $uploadJob \n exits with\n\tstatus $status \n\tresults in errors\n$uerrors and \n\tuploaded $totnckan records .\n"
        echo -e "[ERROR] | | |   Please fix these errors before upload to $rhost can be performed !!\n"
        exit -1
      else
        echo -e "[INFO] | Processing finished for community $comm \n"
        continue
      fi
    fi
  done < ${job_list}
else
    echo -e "[INFO] No action required : ${job_list} is empty ?"
fi
rm jobfile_*

exit 0
