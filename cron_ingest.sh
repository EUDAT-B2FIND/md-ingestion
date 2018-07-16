#!/bin/bash

## Cronjob for B2FIND ingestion

# check arguments
if [[ $# -lt 1 ]];
then
    echo "Missing arguments:/n Syntax: cron_test.sh community [fromdays targethost]"
    exit -1
else
    community=$1
fi
if [[ -z $2 ]];
then
    fromdateset=''
else
    daysago=$2
    NDAYSAGO="${daysago} days ago"
    fromdateset="--fromdate " + `date -d "$NDAYSAGO" '+%Y-%m-%d'`
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
./manager.py -c $community -i $targethost $handlecheck $fromdateset >log/ingest_${community}_${TODAY}.out 2>log/ingest_${community}_${TODAY}.err

exit 0
