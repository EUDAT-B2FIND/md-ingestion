#
# https://guides.dataverse.org/en/latest/api/search.html
#
# https://demo.dataverse.org/api/search?q=*&type=dataset&per_page=1&metadata_fields=citation:dsDescription&metadata_fields=citation:author


import requests
import json


from .base import Harvester

import logging


class EdmondHarvester(Harvester):
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

    @property
    def query(self):
        if not self._query:
            self._query = {
                "where": f"{self.filter}",
                "outFields": "*",
                "returnGeometry": True,
                "f": "geojson",
                # 'returnIdsOnly': False,
            }
        return self._query

    def identifier(self, record):
        return f"arcgis-{self.community}-{record['id']}"

    def matches(self):
        query = {
            "returnCountOnly": True,
        }
        query.update(self.query)
        response = requests.get(self.url, params=query, headers=self.headers, verify=self.verify)
        return int(response.json()['count'])

    def get_records(self):
        query = {
            "resultOffset": 0,
            "resultRecordCount": 200
        }
        query.update(self.query)
        ok = True
        while ok:
            response = requests.get(self.url, params=query, headers=self.headers, verify=self.verify)
            fc = response.json()
            features = fc.get('features', [])
            for feature in features:
                yield feature
            if len(features) == 0:
                ok = False
            query['resultOffset'] += query['resultRecordCount']

    def _write_record(self, fp, record, pretty_print=True):
        json.dump(record, fp, indent=4, sort_keys=True, ensure_ascii=False)
