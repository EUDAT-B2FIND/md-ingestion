#!/bin/bash

## Cronjob for B2FIND ingestion

syntax="Syntax: cron_test.sh community [fromdays targethost]"
MGR="${HOME}/miniconda3/envs/python3.6/bin/manager"


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
	fromdateset="--fromdate ${fromdateval}"
    fi
fi
if [[ -z $3 ]];
then
    targethost='eudatmd1.dkrz.de:8080'
else
    targethost=$3
fi

# handle generation only for productive host (currently eudatmd1* )
handlecheck=''
if [[ $targethost == eudatmd1* ]]; 
then
    handlecheck='--handle_check=credentials_11098'
fi

# set work directory
if [[ $(hostname) == centos* ]]; 
then
    WORK="${HOME}/Projects/EUDAT/EUDAT-B2FIND/md-ingestion"
else
    WORK="${HOME}/md-ingestion"
fi
    
TODAY=`date +\%F`
cd $WORK
set -x
$MGR -c $community -i $targethost $handlecheck $fromdateset >log/ingest_${community}_${TODAY}.out 2> >(tee -a log/ingest_${community}_${TODAY}.err >&2)

exit 0
