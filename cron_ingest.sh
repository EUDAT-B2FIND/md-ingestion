#!/bin/bash

## Cronjob for B2FIND ingestion

# check arguments
if [[ $# -lt 2 ]];
then
    echo "Missing arguments:/n Syntax: cron_test.sh community fromdays [targethost]"
    exit -1
else
    community=$1
    daysago=$2
fi
if [[ -z $3 ]];
then
    targethost='eudat-b1.dkrz.de:8080'
else
    targethost=$3
fi

# handle generation only for productive host (currently eudat-b1*
handlecheck=''
if [[ $targethost == eudat-b1* ]]; 
then
    handlecheck='--handle_check=credentials_11098'
fi

# set work directory
if [[ $(hostname) == centos* ]]; 
then
    WORK="${HOME}/Projects/EUDAT/EUDAT-B2FIND/md-ingestion"
else
    WORK="${HOME}/EUDAT-B2FIND/md-ingestion"
fi
    
TODAY=`date +\%F`
NDAYSAGO="${daysago} days ago"
YESTERDAY=`date -d "$NDAYSAGO" '+%Y-%m-%d'`
cd $WORK
set -x
./manager.py -c $community -i $targethost $handlecheck --fromdate $YESTERDAY >log/ingest_${community}_${TODAY}.out 2>log/ingest_${community}_${TODAY}.err

exit 0
