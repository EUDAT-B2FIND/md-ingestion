import requests
import json


from .base import Harvester

import logging


class BlueCloudHarvester(Harvester):
    def __init__(self, community, url, filter, fromdate, clean, limit, outdir, verify):
        super().__init__(
            community=community,
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
        return f"bluecloud-{record['Identifier']}"

    def get_records(self):
        response = requests.get(self.url)
        items = response.json()
        for url in items["urls"]:
            response = requests.get(url)
            yield response.json()

    def _write_record(self, fp, record, pretty_print=True):
        json.dump(record, fp, indent=4, sort_keys=True, ensure_ascii=False)
