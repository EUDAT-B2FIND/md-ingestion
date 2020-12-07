#!/bin/bash

## Cronjob for B2FIND ingestion

syntax="Syntax: cron_test.sh community [fromdays targethost authkey]"
B2F="${HOME}/miniconda3/envs/b2f/bin/b2f"


# check arguments
if [[ $# -lt 1 ]];
then
    printf "Missing argument:\n${syntax}\n"
    exit -1
else
    community=$1
    if [ $community == '-h' ] || [ $community == '--help' ];
    then
	printf "${syntax}\n"
	exit 0
    fi
fi
if [[ -z $2 ]];
then
    fromdateset=''
else
    daysago=$2
    if [ $daysago == 'all' ];
    then
	fromdateset=""
    else
	NDAYSAGO="${daysago} day ago"
	fromdateval=`date --date="${NDAYSAGO}" +%Y-%m-%d`
	fromdateset="${fromdateval}"
    fi
fi
if [[ -z $3 ]];
then
    targethost='eudatmd2.dkrz.de:8080'
else
    targethost=$3
fi

authkey=$4

# handle generation only for productive host (currently eudatmd2* )
# handlecheck=''
# if [[ $targethost == eudatmd1* ]]; 
# then
    # handlecheck='--handle_check=credentials_11098'
# fi

# set work directory
# if [[ $(hostname) == centos* ]]; 
# then
    #WORK="${HOME}/Projects/EUDAT/EUDAT-B2FIND/md-ingestion"
# else
    #WORK="${HOME}/md-ingestion"
#fi
  
WORK="${HOME}/md-ingestion"    
TODAY=`date +\%F`
cd $WORK
set -x
$B2F combine -c $community -i $targethost --auth $authkey --fromdate $fromdateset --clean >log/ingest_${community}_${TODAY}.out 2> >(tee -a log/ingest_${community}_${TODAY}.err >&2)

exit 0
