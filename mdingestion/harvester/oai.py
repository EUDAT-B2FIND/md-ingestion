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

    def __init__(self, repo, url, oai_metadata_prefix, oai_set, fromdate, clean, limit, outdir, verify,
                 username, password):
        super().__init__(repo, url, fromdate, clean, limit, outdir, verify,
                         username, password)
        logging.captureWarnings(True)
        self.mdprefix = oai_metadata_prefix
        self.oai_set = oai_set
        if self.username:
            auth = (self.username, self.password)
        else:
            auth = None
        self.sickle = Sickle(self.url, max_retries=3, timeout=120, verify=self.verify,
                             auth=auth)

    def identifier(self, record):
        return record.header.identifier

    def matches(self):
        try:
            records = self.sickle.ListIdentifiers(**{
                'metadataPrefix': self.mdprefix,
                'set': self.oai_set,
                'ignore_deleted': True,
                'from': self.fromdate,
            })
            # TODO: complete_list_size is not always set by OAI
            matches = int(records.resumption_token.complete_list_size)
        except Exception:
            logging.warning('Could not get complete list size from OAI.')
            matches = super().matches()
        return matches

    def check_metadata_format(self):
        md_formats = None
        try:
            md_formats = [f.metadataPrefix for f in self.sickle.ListMetadataFormats()]
        except Exception:
            logging.warning("OAI does not support ListMetadataFormats request.")
        if md_formats and self.mdprefix not in md_formats:
            logging.error(
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
            logging.warning(f'No records match the OAI query. from={self.fromdate}')
        except CannotDisseminateFormat:
            raise HarvesterError(f'The metadata format {self.mdprefix} is not supported by the OAI repository.')

    def _write_record(self, fp, record, pretty_print=True):
        xml = etree.tostring(record.xml, pretty_print=pretty_print).decode('utf8')
        fp.write(xml)
