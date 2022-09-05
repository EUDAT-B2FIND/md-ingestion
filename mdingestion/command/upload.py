import os
from tqdm import tqdm
import json
from urllib import parse
from ckanapi import RemoteCKAN, NotFound, NotAuthorized
from requests.exceptions import ConnectionError

from .base import Command
from ..walker import Walker
from ..community import community, communities

import logging


def upload(data, host=None, apikey=None, no_update=False, verify=True, https=False):
    requests_kwargs = {'verify': verify}
    proto = 'https' if https else 'http'
    try:
        with RemoteCKAN(f'{proto}://{host}', apikey=apikey) as ckan:
            if no_update:
                ckan.call_action('package_show', {'id': data['name']}, requests_kwargs=requests_kwargs)
                logging.info("upload skip update")
            else:
                ckan.call_action('package_update', data, requests_kwargs=requests_kwargs)
                logging.info("upload update")
    except NotFound:
        # TODO: clean up code ...
        with RemoteCKAN(f'{proto}://{host}', apikey=apikey) as ckan:
            ckan.call_action('package_create', data, requests_kwargs=requests_kwargs)
            logging.info("upload create")


class Upload(Command):
    def run(self, iphost=None, auth=None, target=None, from_=None, limit=None, no_update=False, verify=True,
            silent=False, https=False):
        # TODO: refactor community loop
        _communities = communities(self.community)
        for identifier in tqdm(_communities,
                               ascii=True,
                               desc=f"Uploading {self.community}",
                               # position=0,
                               unit=' community',
                               total=len(_communities),
                               disable=len(_communities) == 1 or silent):
            self._community = community(identifier)
            self.upload_to_ckan(iphost=iphost, auth=auth, from_=from_, limit=limit,
                                no_update=no_update, verify=verify,
                                silent=silent, https=https)

    def upload_to_ckan(self, iphost, auth, from_=None, limit=None, no_update=False, verify=True,
                       silent=False, https=False):
        self.walker = Walker(self.datadir)
        limit = limit or -1
        count = 0
        success = True
        for filename in tqdm(self.walk(), ascii=True, desc=f"Uploading {self._community.identifier}",
                             unit=' records', total=limit, disable=silent):
            if from_ and count < from_:
                logging.info(f"skipping {filename}")
                count += 1
                continue
            if limit > 0 and count >= limit:
                break
            logging.info(f"uploading {filename}")
            with open(filename, 'rb') as fp:
                data = json.load(fp)
                try:
                    upload(data=data, host=iphost, apikey=auth, no_update=no_update, verify=verify, https=https)
                except ConnectionError:
                    logging.exception('upload connection error.')
                    raise
                except NotAuthorized:
                    logging.exception('upload not authorized.')
                    raise
                except Exception:
                    logging.exception(f'upload failed: {filename}.')
                    success = False
            count += 1
        if not success:
            raise Exception(f'upload of some files failed. community={self._community.identifier}')

    def walk(self):
        path = os.path.join(self._community.identifier, 'ckan')
        for filename in self.walker.walk(path=path, ext='.json'):
            yield filename
