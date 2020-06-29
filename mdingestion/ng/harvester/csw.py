from owslib.csw import CatalogueServiceWeb
from owslib.namespaces import Namespaces
from owslib import fes
from owslib.util import Authentication
from lxml import etree

from .base import Harvester

import logging


class CSWHarvester(Harvester):
    """
    OWSLib csw: https://geopython.github.io/OWSLib/#csw
    """
    def __init__(self, community, url, mdprefix, mdsubset, fromdate, limit, outdir, verify):
        super().__init__(community, url, mdprefix, mdsubset, fromdate, limit, outdir, verify)
        logging.captureWarnings(True)
        self.csw = CatalogueServiceWeb(self.url, auth=Authentication(verify=self.verify))
        self._schema = None
        self._constraints = None

    def identifier(self, record):
        return record.identifier

    def matches(self):
        self.csw.getrecords2(
            maxrecords=0,
            constraints=self.constraints,
            outputschema=self.schema)
        return self.csw.results['matches']

    @property
    def schema(self):
        if not self._schema:
            if 'iso19139' in self.mdprefix:
                ns_name = 'gmd'
            else:
                ns_name = 'csw'
            self._schema = Namespaces().get_namespace(ns_name)
        return self._schema

    @property
    def constraints(self):
        if not self._constraints:
            self._constraints = []
            if self.fromdate:
                from_filter = fes.PropertyIsGreaterThan('dct:modified', self.fromdate)
                self._constraints.append(from_filter)
        return self._constraints

    def get_records(self):
        """get_records implementation with paging"""
        startposition = 0
        ok = True
        while ok:
            self.csw.getrecords2(
                constraints=self.constraints,
                startposition=startposition,
                outputschema=self.schema)
            for rec in self.csw.records:
                yield self.csw.records[rec]
            startposition = self.csw.results['nextrecord']
            if startposition == 0:
                ok = False

    def _write_record(self, fp, record, pretty_print=True):
        xml = etree.tostring(etree.fromstring(record.xml), pretty_print=pretty_print).decode('utf8')
        fp.write(xml)
