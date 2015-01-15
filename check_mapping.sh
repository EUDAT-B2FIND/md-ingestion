#!/bin/bash

## ----------- get options and community name -------------

err_strg=''
usage() {
    printf "${b}err_strg${n}\n"
    printf "${b}NAME${n}\n\t`basename $0` - check field mapping of B2FIND\n\n" 
    printf "${b}SYNOPSIS ${n}\n\t${b}`basename $0`${n}  [${u}OPTION${n}]...  "
    printf "${b}DESCRIPTION ${n}\n\t checks field mapping of B2FIND ...\n" 
    printf "${b}OPTIONS${n}\n"
    printf "\t${b}--help, -h${n}\n\t\tdisplay this built-in help text and exit.\n" 
    printf "\t${b}--mdformat, -m${n} MDFORMAT${n}  OAI ${u}MDFORMAT${n} (default is oai_dc)\n"
    printf "\t${b}--community, -c COMMUNITY${n} B2FIND ${u}COMMUNITY${n} to check (default is TheEuropeanLibrary).\n"
    printf "\t${b}--set, -s OAISET${n} ${u}COMMUNITY${n} to check (default is TheEuropeanLibrary).\n"
    printf "\t${b}--field, -f FIELD${n} B2FIND ${u}FIELD${n}\n\t\t to check (default is DISCIPLINE).\n"
    printf "\t${b}--node, -n NODE${n} XML ${u}NODE${n}\n\t\t to check (default is dc:subject).\n"
    exit 0
}

while [[ -n $1 ]] ; do
  case $1 in
        --help       | -h) usage ;;
        --mdformat   | -m) mdformat=$2; shift ;;
        --field      | -f) field=$2; shift ;;
        --node       | -n) node=$2; shift ;;
        --community  | -c) comm=$2; shift ;;
        --set        | -s) oaisets=$2; shift ;;
        -*               ) err_strg="\nERROR: invalid option $1 !\n"
                          [[ "$1" != $( echo "$1" | tr -d = ) ]] && \
                          err_strg=$err_strg"  Please use blank insted of '=' to separate option's argument.\n"
                          usage ;;
  esac
  shift
done

if [ -z "$field" ]
then
  field='Discipline'
fi

if [ -z "$comm" ]
then
  comm='theeuropeanlibrary'
fi

if [ -z "$mdformat" ]
then
  mdformat='oai_dc'
fi

if [ -z "$node" ]
then
  node='dc:subject'
fi

if [ -z "$oaisets" ]
then
  #oaisets='a0005_1 a0005_2 a0005_3 a0005_4 a0005_5'
  oaisets='a0338_1 a0336_1'
  oaisets='a1058_1 a0337_1 a0633_1 a0237_1 a0442_1 a0341_1 a1025_1 a1057_1 a0552_1 a0552_2 a0340_1 a0005_1 a0005_2 a0005_3 a0005_4 a0005_5  a0338_1 a0336_1 a1025_1'
##  oaisets='a0341_1'
fi

echo -e "\n-Field    \t $field"
echo -e "\n-Community\t $comm"
for oaiset in $oaisets
do
  echo -e "\n|-OAI set >> $oaiset << ----"
  echo -e " |- Total # of json files     \t$(ls oaidata/${comm}-${mdformat}/${oaiset}/json/* | wc -l)"
  echo -e " |- Total # of node \"$node\" \t$(grep -c $node oaidata/${comm}-${mdformat}/${oaiset}/xml/* | cut -d: -f2 | awk '{total = total + $1}END{print total}')"
  echo -e " | #rec | with value .."
  grep $node  oaidata/${comm}-${mdformat}/${oaiset}/xml/*.xml | cut -d'>' -f2 |cut -d'<' -f1 | sort | uniq -c | sort -rn | head -10
##  echo -e " |- Files with node \"$node\" \t$(grep $node oaidata/${comm}-${mdformat}/${oaiset}/xml/*)"
  echo -e " |- Total # of mapped field  ${field} \t$(grep -c "\"key\": \"${field}\"" oaidata/${comm}-${mdformat}/${oaiset}/json/* | cut -d: -f2 | awk '{total = total + $1}END{print total}')"
  echo -e " | #rec | mapped on .."
  deflist="author title"
  if [[ $deflist =~ (^| )${field}($| ) ]]
  then
    grep "\"${field}\"" oaidata/${comm}-${mdformat}/${oaiset}/json/* | cut -d':' -f2- | sed 's|[,.]||g' | sort -nr | uniq -c | sort -nr 
  else
    grep -A1 "\"key\": \"${field}\"" oaidata/${comm}-${mdformat}/${oaiset}/json/* | grep value | cut -d':' -f2- | sed 's|[,.]||g' | sort | uniq -c | sort -nr
  fi
 ## instead awk '{print $3}'
  ## grep -A1 '"key": "${field}"' oaidata/${comm}-${mdformat}/${oaiset}/json/* | grep value 
##| awk '{print $3}' | sed 's|[,.]||g' | tr ' ' '\n' | sort | uniq -c
done
