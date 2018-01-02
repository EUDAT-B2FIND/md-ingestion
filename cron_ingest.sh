#!/bin/bash

## Cronjob for B2FIND ingestion

community=$1
##daysago=$2

if [[ $(hostname) == centos* ]]; 
then
    WORK="${HOME}/Projects/EUDAT/EUDAT-B2FIND/md-ingestion"
else
    WORK="${HOME}/EUDAT-B2FIND/md-ingestion"
fi
    
TODAY=`date +\%F`
NDAYSAGO="$2 days ago"
YESTERDAY=`date -d "$NDAYSAGO" '+%Y-%m-%d'`
cd $WORK
set -x
./manager.py -c $community -i eudat-b1.dkrz.de:8080 --handle_check=credentials_11098 --fromdate $YESTERDAY >log/ingest_${community}_${TODAY}.out 2>log/ingest_${community}_${TODAY}.err

exit 0
