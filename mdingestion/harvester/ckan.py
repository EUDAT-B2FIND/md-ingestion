import requests
import json


from .base import Harvester

import logging


class CKANHarvester(Harvester):
    def __init__(self, repo, url, filter, fromdate, clean, limit, outdir, verify):
        super().__init__(
            repo=repo,
            url=url,
            fromdate=fromdate,
            clean=clean,
            limit=limit,
            outdir=outdir,
            verify=verify)
        self.ext = 'json'
        self._query = None
        self.filter = filter
        self.headers = {'content-type': 'application/json'}
        logging.captureWarnings(True)

    def identifier(self, record):
        return f"ckan-{record['id']}"

    def get_records(self):
        url = f'{self.url}/api/action/package_list'
        response = requests.get(url)
        items = response.json()
        for id in items["result"]:
            urlds = f'{self.url}/api/action/package_show?id={id}'
            response = requests.get(urlds)
            yield response.json()['result']

    def _write_record(self, fp, record, pretty_print=True):
        json.dump(record, fp, indent=4, sort_keys=True, ensure_ascii=False)