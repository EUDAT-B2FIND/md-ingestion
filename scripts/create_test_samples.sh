#!/bin/sh

echo "[INFO] Start creating test directory of (erronous) xml files"

usage="Usage : ./create_test_samples.sh [USER@]HOST COMMUNITY MDFORMAT SUBSET LOGFILE"

if [ $# -lt 5 ]
then
   echo -e "Error: Arguments missing !\n $usage"
   exit 1
else
    uhost="${1}"
    communtiy=$2 ### "${1}"
    echo "XXX ${community} ${1}"
    mdformat=$3
    origsubset=$4
fi
testsubset='test_1'

commpath="oaidata/${2}-${3}"
origxmlpath="${commpath}/${4}_1/xml"
testxmlpath="${commpath}/${testsubset}/xml"
logoutfile="log/${5}.out"
logerrfile="log/${6}.err"

if [ -d $commpath ];
then
    echo -e "Main metadata path:\t${commpath}"
else
    echo "ERROR : metadata path must exist:\t${commpath}"
fi

if [ ! -d $testxmlpath ];
then
    mkdir -p "${testxmlpath}"
fi

if [ -f $logoutfile ];
then
    echo -e "Log out file:\t${logoutfile}"
else
    echo -e "ERROR : Log out file can not accessed:\t${logoutfile}"
    scp ${uhost}:/home/dkrz/eudat_data/EUDAT-B2FIND/md-ingestion/$logoutfile log
fi

ids=$(grep -B1 CRITICAL $logoutfile | grep '| u |' | awk '{ print $12 }')

if [ -d $origxmlpath ];
then
    echo -e "Original xml path:\t${origxmlpath}"
    for id in $ids
    do
	cp ${origxmlpath}/${id}.xml ${testxmlpath}/
    done
else
    echo -e "ERROR : Original xml path can not accessed:\t${origxmlpath}"
    echo -e " Do a fresh harvest by\n\t./manager.py --mode h -c $1 --mdsubset ${3}\n\t or retrieve XML files from eudatmd1 (see see below).\n\t. I try now the latter via scp ..."
    sleep 10
    for id in $ids
    do
	scp ${uhost}:/home/dkrz/eudat_data/EUDAT-B2FIND/md-ingestion/${origxmlpath}/${id}.xml ${testxmlpath}/
    done

fi


origrequest=$(grep ^$1 convert_list_total | grep $2 | grep ${3}_1)
testrequest=$(grep ^$1 convert_list_total | grep $2 | grep $testsubset)
if [ -z "$testrequest" ]
then
    testrequest="$(echo "$origrequest" | cut -f1-4) $testsubset"
    echo $testrequest >> convert_list_total
fi

echo -e "Test request $testrequest\n\t in convert_list_total "
echo -e " You can now map by\n\t./manager.py --mode m -c $2 --mdsubset $testsubset -l convert_list_total\n and upload by\n\t./manager.py --mode u -c $2 --mdsubset $testsubset -l convert_list_total -i eudatmd1.dkrz.de --handle_check=credentials_11098\n\t or any other CKAN server you like."

exit 0

