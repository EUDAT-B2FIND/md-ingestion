import requests
import json


from .base import Harvester

import logging


class HerbadropHarvester(Harvester):
    def __init__(self, community, url, mdprefix, mdsubset, fromdate, limit, outdir, verify):
        super().__init__(community, url, 'json', mdsubset, fromdate, limit, outdir, verify)
        self.format = 'hjson'
        self.ext = 'json'
        self._query = None
        self.headers = {'content-type': 'application/json'}
        logging.captureWarnings(True)

    @property
    def query(self):
        if not self._query:
            self._query = {
                "text": "Herbarium",
                "searchTextInMetadata": True,
                "searchTextInAdditionalData": True,
            }
            # add filter
            self._query["metadataCriteria"] = []
            # filter: ids
            # self._query["metadataCriteria"].append(
            #     {
            #         "field": "aip.meta.depositIdentifier",
            #         "operator": "MATCHES",
            #         "not": "false",
            #         "values": ['P03068284'],
            #     }
            # )
            # filter: aip.meta.archivingDate > fromdate
            if self.fromdate:
                self._query["metadataCriteria"].append(
                    {
                        "field": "aip.meta.archivingDate",
                        "operator": "AFTER",
                        "not": "false",
                        "values": [self.fromdate],
                    }
                )
        return self._query

    def identifier(self, record):
        return record['depositIdentifier']

    def matches(self):
        data = {
            "page": 1,
            "size": 1,
        }
        data.update(self.query)
        response = requests.post(self.url, headers=self.headers, data=json.dumps(data), verify=self.verify)
        return int(response.json()['total'])

    def get_records(self):
        data = {
            "page": 1,
            "size": 100,
        }
        data.update(self.query)
        ok = True
        while ok:
            response = requests.post(self.url, headers=self.headers, data=json.dumps(data), verify=self.verify)
            records = response.json().get('result', [])
            for record in records:
                yield record
            if len(records) == 0:
                ok = False
            data['page'] += 1

    def _write_record(self, fp, record, pretty_print=True):
        json.dump(record, fp, indent=4, sort_keys=True, ensure_ascii=False)
