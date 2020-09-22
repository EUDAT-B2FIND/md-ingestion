import os
from tqdm import tqdm
import json
from urllib import parse
from ckanapi import RemoteCKAN, NotFound

from .base import Command
from ..walker import Walker
from ..community import community

import logging


class Upload(Command):
    def run(self, iphost=None, auth=None, target=None, limit=None, verify=True):
        self.upload_to_ckan(iphost=iphost, auth=auth, limit=limit, verify=verify)

    def upload_to_ckan(self, iphost, auth, limit=None, verify=True):
        self.walker = Walker(self.outdir)
        limit = limit or -1
        count = 0
        requests_kwargs = None
        if not verify:
            requests_kwargs = {'verify': False}
        with RemoteCKAN(f'http://{iphost}', apikey=auth) as ckan:
            for filename in tqdm(self.walk(), ascii=True, desc=f"Uploading {self.community}",
                                 unit=' records', total=limit):
                if limit > 0 and count >= limit:
                    break
                logging.info(f"uploading {filename}")
                with open(filename, 'rb') as fp:
                    data = json.load(fp)
                    try:
                        # ckan.action.package_update(**data)
                        ckan.call_action('package_update', data, requests_kwargs=requests_kwargs)
                    except NotFound:
                        # ckan.action.package_create(**data)
                        ckan.call_action('package_create', data, requests_kwargs=requests_kwargs)
                count += 1

    def walk(self):
        _community = community(self.community)
        path = os.path.join(_community.identifier, 'ckan')
        for filename in self.walker.walk(path=path, ext='.json'):
            yield filename
