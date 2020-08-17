import shapely

from .base import XMLReader
from ..sniffer import CSWSniffer

import logging


class ISO19139Reader(XMLReader):
    SNIFFER = CSWSniffer

    def parse(self, doc):
        doc.title = self.find('title')
        doc.description = self.find('abstract')
        doc.keywords = self.find('keyword')
        doc.creator = self.find('individualName')
        # TODO: check whether <publisher> is generic in <contact>
        doc.publisher = self.find('contact.organisationName')
        doc.publication_year = self.find('CI_Citation.Date')
        doc.rights = self.find('useLimitation')
        doc.contact = self.find('contact.electronicMailAddress')
        doc.language = self.find('LanguageCode')
        doc.resource_type = self.find('MD_ScopeCode')
        doc.format = self.find('hierarchyLevelName')
        # TODO: fix temporal coverage
        # doc.temporal_coverage_begin = self.parser.doc.find('CI_Citation').find('Date').text
        # doc.temporal_coverage_end = doc.temporal_coverage_begin
        doc.geometry = self.geometry()

    def geometry(self):
        geometry = None
        try:
            if self.parser.doc.find('EX_GeographicBoundingBox'):
                bbox = self.parser.doc.find('EX_GeographicBoundingBox').text.split()
                geometry = shapely.geometry.box(float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))
        except Exception:
            logging.warning("could not read geometry.")
        return geometry
