import os
from tqdm import tqdm
import json
from urllib import parse
from ckanapi import RemoteCKAN, NotFound

from .base import Command
from ..walker import Walker
from ..community import community

import logging


def upload(data, action=None, host=None, apikey=None, verify=True):
    action = action or 'package_update'
    requests_kwargs = None
    if not verify:
        requests_kwargs = {'verify': False}
    try:
        with RemoteCKAN(f'http://{host}', apikey=apikey) as ckan:
            ckan.call_action(action, data, requests_kwargs=requests_kwargs)
    except NotFound:
        upload(data, action='package_create', host=host, apikey=apikey, verify=verify)


class Upload(Command):
    def run(self, iphost=None, auth=None, target=None, limit=None, verify=True):
        self.upload_to_ckan(iphost=iphost, auth=auth, limit=limit, verify=verify)

    def upload_to_ckan(self, iphost, auth, limit=None, verify=True):
        self.walker = Walker(self.outdir)
        limit = limit or -1
        count = 0
        for filename in tqdm(self.walk(), ascii=True, desc=f"Uploading {self.community}",
                             unit=' records', total=limit):
            if limit > 0 and count >= limit:
                break
            logging.info(f"uploading {filename}")
            with open(filename, 'rb') as fp:
                data = json.load(fp)
                upload(data=data, host=iphost, apikey=auth, verify=verify)
            count += 1

    def walk(self):
        _community = community(self.community)
        path = os.path.join(_community.identifier, 'ckan')
        for filename in self.walker.walk(path=path, ext='.json'):
            yield filename
