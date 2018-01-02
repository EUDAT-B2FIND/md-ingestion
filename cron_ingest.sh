#!/bin/bash

## Cronjob for B2FIND ingestion

community=$1
##daysago=$2

WORK='/home/dkrz/k204019/EUDAT-B2FIND/md-ingestion'
TODAY=`date +\%F`
NDAYSAGO="$2 days ago"
YESTERDAY=`date -d "$NDAYSAGO" '+%Y-%m-%d'`
cd $WORK
set -x
./manager.py -c $community -i eudat-b1.dkrz.de --handle_check=credentials_11098 --fromdate $YESTERDAY >log/ingest_${community}_${TODAY}.out 2>log/ingest_${community}_${TODAY}.err

exit 0
