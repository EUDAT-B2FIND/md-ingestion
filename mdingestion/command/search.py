import os
from tqdm import tqdm
import json
from urllib import parse
from ckanapi import RemoteCKAN, NotFound, NotAuthorized
from requests.exceptions import ConnectionError

from .base import Command

import logging

agent = 'b2f'


def ckan_search(iphost, community, limit, pattern):
    
    # create pattern for CKAN:
    ckan_pattern = ""
    if community:
        ckan_pattern = f"groups:{community}"
    if pattern:
        if ckan_pattern:
            ckan_pattern += " AND "
        ckan_pattern += pattern

    with RemoteCKAN(f"http://{iphost}", user_agent=agent) as ckan:
        answer = ckan.action.package_search(q=ckan_pattern, rows=limit)
        
        # print results:
        # print(f"{answer}")
        print("Results on CKAN (%s), %d dataset(s), show max. %d:" % (iphost, answer['count'], limit))
        for ds in answer['results']:
            print('[%s]' % ds['name'])
            print('    title: %s' % ds['title'])
        
            if (len(ds['groups'])):
                print('    group: %s' % ds['groups'][0]['name'])
                    


class Search(Command):
    def run(self, iphost=None, limit=20, pattern=None, verify=True,
            silent=False):
        ckan_search(community=self.community, iphost=iphost, limit=limit, pattern=pattern)
