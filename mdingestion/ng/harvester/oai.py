from sickle import Sickle
from sickle.oaiexceptions import (
    NoRecordsMatch,
    NoSetHierarchy,
    CannotDisseminateFormat,
)
from lxml import etree

from .base import Harvester
from ..exceptions import HarvesterError

import logging


class OAIHarvester(Harvester):
    def __init__(self, community, url, mdprefix, mdsubset, fromdate, limit, outdir, verify):
        super().__init__(community, url, mdprefix, mdsubset, fromdate, limit, outdir, verify)
        logging.captureWarnings(True)
        self.sickle = Sickle(self.url, max_retries=3, timeout=300, verify=self.verify)

    def identifier(self, record):
        return record.header.identifier

    @property
    def oai_set(self):
        """
        TODO: refactor this code ... how to handle oai sets
        """
        oai_set = None
        if self.mdsubset in self.oai_sets():
            oai_set = self.mdsubset
            logging.warning(f"OAI does not support set {self.mdsubset}.")
        return oai_set

    def oai_sets(self):
        oai_sets = []
        try:
            oai_sets = [s.setSpec for s in self.sickle.ListSets()]
        except NoSetHierarchy:
            logging.warning("OAI does not support sets.")
        except Exception:
            logging.warning("OAI does not support ListSets request.")
        return oai_sets

    def check_metadata_format(self):
        md_formats = None
        try:
            md_formats = [f.metadataPrefix for f in self.sickle.ListMetadataFormats()]
        except Exception:
            logging.warning("OAI does not support ListMetadataFormats request.")
        if md_formats and self.mdprefix not in md_formats:
            raise HarvesterError(
                f'The metadata format {self.mdprefix} is not supported by the OAI repository. Formats={md_formats}')

    def get_records(self):
        self.check_metadata_format()
        # NOTE: use dict args to pass "from" parameter
        # https://sickle.readthedocs.io/en/latest/tutorial.html#using-the-from-parameter
        try:
            records = self.sickle.ListRecords(**{
                'metadataPrefix': self.mdprefix,
                'set': self.oai_set,
                'ignore_deleted': True,
                'from': self.fromdate,
            })
            for record in records:
                yield record
        except NoRecordsMatch:
            raise HarvesterError(f'No records match the OAI query. from={self.fromdate}, set={self.mdsubset}')
        except CannotDisseminateFormat:
            raise HarvesterError(f'The metadata format {self.mdprefix} is not supported by the OAI repository.')

    def _write_record(self, fp, record, pretty_print=True):
        xml = etree.tostring(record.xml, pretty_print=pretty_print).decode('utf8')
        fp.write(xml)
