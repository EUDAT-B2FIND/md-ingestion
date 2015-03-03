#!/bin/bash
LC_NUMERIC=C LC_COLLATE=C

## ----------- checks mapping of B2FIND records -------------

err_strg=''
usage() {
    printf "${b}${err_strg}${n}\n"
    printf "${b}NAME${n}\n\t`basename $0` - check field mapping of B2FIND\n" 
    printf "${b}SYNOPSIS ${n}\n\t${b}`basename $0`${n}  [${u}OPTION${n}]...  \n"
    printf "${b}DESCRIPTION ${n}\n\t checks field mapping of B2FIND ...\n" 
    printf "${b}OPTIONS${n}\n"
    printf "\t${b}--help, -h${n}\t\tdisplay this built-in help text and exit.\n" 
    printf "\t${b}--community, -c COMMUNITY\tB2FIND ${u}community${n} to check (required).\n"
    printf "\t${b}--mdformat, -m MDFORMAT  \tOAI ${u}metadata format{n} (default is oai_dc)\n"
    printf "\t${b}--set, -s OAISET         \t${u}OAI sets${n} to check (if not given, all subsets (subdirs) of community are checked.)\n"
    printf "\t${b}--field, -f FIELD        \t${n}B2FIND ${u}field${n} to check.\n"
    printf "\t${b}--node, -n NODE          \t${n}XML/JSON ${u}node${n} to check (optional).\n"
    exit 0
}

while [[ -n $1 ]] ; do
  case $1 in
        --help       | -h) err_strg=''; usage ;;
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

##if [ -z "$field" ]
##then
##  field='Discipline'
##fi

if [ -z "$comm" ]
then
  err_strg=' Community is mandatory\n'
  usage
fi

if [ -z "$mdformat" ]
then
  mdformat='oai_dc'
fi

if [ -z "$oaisets" ]
then
  oaisets=$(ls oaidata/${comm}-${mdformat})
fi

echo -e "\n-Community\t $comm"
echo -e "-MDformat \t $mdformat"
echo -e "-OAI sets \t $oaisets"
echo -e "-Field    \t $field"

if [ -n "$node" ]
then
  echo -e "-Node     \t $node"
fi

if [ $mdformat = 'json' ]
then
    hdir='hjson'
    hext='json'
    nline=''
else
    hdir='xml'
    hext='xml'
    nline='-A1'
fi

if [ $mdformat = 'marcxml' ]
then
    tagchar='tag="'
elif [ $mdformat = 'json' ]
then
    tagchar='"'
else
    tagchar='<'
fi


for oaiset in $oaisets
do
  echo -e "\n|-OAI set >> $oaiset << ----"
  njson=$(ls oaidata/${comm}-${mdformat}/${oaiset}/json/* | wc -l)
  echo -e " |- Total # of json files     \t$njson "
  if [ -n "$node" ]
  then
    echo -e "  |- Node $node "
    echo -e "   |- Total # of node >>$node<< \t$(grep -c ${tagchar}${node} oaidata/${comm}-${mdformat}/${oaiset}/${hdir}/* | cut -d: -f2 | awk '{total = total + $1}END{print total}')"
    echo -e "   | #rec | with node value .."
set -x
    grep  -A1 ${tagchar}${node}  oaidata/${comm}-${mdformat}/${oaiset}/${hdir}/*.${hext} | cut -d'>' -f2 | cut -d'<' -f1 | cut -d':' -f3 | sort | uniq -c | sort -rn | head -10
##  echo -e " |- Files with node \"$node\" \t$(grep $node oaidata/${comm}-${mdformat}/${oaiset}/xml/*)"
  fi
  printf '   |- %-15s | %+6s | %+6s |\n' "Fieldname" "# of matches" "Coverage [%}" 
  if [ -n "$field" ]
  then
    echo -e "  |- Field $field "
    deflist="author title url"
    if [[ $deflist =~ (^| )${field}($| ) ]]
    then
      searchstr=''
    else
      searchstr=' -e "key": '
    fi
    set -x
    nmapped=$(grep $searchstr -e "$field" oaidata/${comm}-${mdformat}/${oaiset}/json/* | cut -d: -f2 | awk '{total = total + $1}END{print total}')
    printf '   |- Total # of mapped entities %s' "$nmapped"
    cov=$( echo "$nmapped / $njson * 100" | bc -l)
    printf '   |- Coverage %3.0f %% \n' "$cov"
    ##  nmapped=$(grep -c -h "\"key\": \"${field}\"" oaidata/${comm}-${mdformat}/${oaiset}/json/* | cut -d: -f2 | awk '{total = total + $1}END{print total}')
    echo -e " | #rec | vaules $field is mapped on .. (show max. first 20 entries) "
    if [[ $deflist =~ (^| )${field}($| ) ]]
    then
      grep -h "\"${field}\"" oaidata/${comm}-${mdformat}/${oaiset}/json/* | cut -d':' -f2- | sort -nr | uniq -c | sort -nr | head -20 
    else
      grep -h -F -A1 "$searchstr" oaidata/${comm}-${mdformat}/${oaiset}/json/* | grep value | cut -d':' -f2- | sort | uniq -c | sort -nr | head -20
      #set +x
    fi
  fi
 ## instead awk '{print $3}'
  ## grep -A1 '"key": "${field}"' oaidata/${comm}-${mdformat}/${oaiset}/json/* | grep value 
##| awk '{print $3}' | sed 's|[,.]||g' | tr ' ' '\n' | sort | uniq -c
done
