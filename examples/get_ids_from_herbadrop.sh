#!/bin/bash
URL=https://opendata.cines.fr/herbadrop-api/rest/data/search
SIZE=100
MAX_PAGES=10
OUT='list-ids.txt'

# remove previous out file
rm -f $OUT

# grep depositIdentifier ...
PAGE=0
while [  $PAGE -lt $MAX_PAGES ]; do
  let PAGE=PAGE+1
  curl -k -H 'Content-Type: application/json' -X POST -d "{'text' : 'herbarium', 'strictCharacterSearch' : false, 'searchTextInAdditionalData' : true, 'searchTextInMetadata' : true, 'page' : $PAGE, 'size' : $SIZE}" $URL | grep -i 'depositIdentifier' | grep -v aip | cut -d ':' -f 2 | tr -d [:blank:] | tr -d [:punct:] >> $OUT
done

# count depositIdentifier
TOTAL=$(cat $OUT | sort | uniq | wc -l)
echo "Total: $TOTAL"
