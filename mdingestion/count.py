import os

count=0
for root, _, files in os.walk('/home/eudat_data/md-ingestion/oaidata/clarin-oai_dc'):
    for file in files:
        openfile = open(os.path.join(root,file), 'r')
        if 'metashare' in openfile.read():
           count+=1
        openfile.close()
print count