# Using ArcGIS Rest API to retrieve features from a specific layer
#
# This is our REST-API for sites and monuments:
# https://kart.ra.no/arcgis/rest/services/Distribusjon/Kulturminner20180301/MapServer/3
#
# If you go to lokaliteter -> query you could try these options:
#
# Where: lokalid='22228'
# Out fields: *
# Returns a well-known grave field at Borre.
#
# Enkeltminner -> query
# Where: lokalid='22228-7'
# Out fields: *
# Returns a single burial mound at Borre.
#
# Where: kulturminneDatering='Vikingtid' and kommune='Horten'
# Out fields: *
# Returns single monuments form viking age in Horten municipality.
#
#
# Example for single object request:
# https://kart.ra.no/arcgis/rest/services/Distribusjon/Kulturminner20180301/MapServer/7/query?where=+lokalid%3D%2722228%27&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&f=geojson
#
# arcgis docu:
# https://kart.ra.no/arcgis/sdk/rest/index.html#//02ss0000000r000000
#
#
# OR USE WFS
# ----------
# https://kartkatalog.geonorge.no/metadata/cultural-heritage-sites/c6896f24-71f9-4203-9b6f-faf3bfe1f5ed
# https://wfs.geonorge.no/skwms1/wfs.lokaliteter?service=wfs&request=getcapabilities
# https://gis.stackexchange.com/questions/299567/reading-data-to-geopandas-using-wfs
#

import requests
import json


from .base import Harvester

import logging


class ArcGISHarvester(Harvester):
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
            records = response.json().get('features', [])
            for record in records:
                yield record
            if len(records) == 0:
                ok = False
            query['resultOffset'] += query['resultRecordCount']

    def _write_record(self, fp, record, pretty_print=True):
        json.dump(record, fp, indent=4, sort_keys=True, ensure_ascii=False)
