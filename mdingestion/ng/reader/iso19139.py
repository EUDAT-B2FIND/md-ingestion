import shapely

from .base import XMLReader, CSWSniffer

import logging


class ISO19139Reader(XMLReader):
    SNIFFER = CSWSniffer

    def parse(self, doc):
        doc.title = self.parser.find('title')
        doc.description = self.parser.find('abstract')
        doc.tags = self.parser.find('keyword')
        doc.creator = self.parser.find('individualName')
        # TODO: check whether <publisher> is generic in <contact>
        doc.publisher = self.parser.find('contact.organisationName')
        doc.publication_year = self.parser.find('CI_Citation.Date')
        doc.rights = self.parser.find('useLimitation')
        doc.contact = self.parser.find('contact.electronicMailAddress')
        doc.open_access = ''
        doc.language = self.parser.find('LanguageCode')
        doc.resource_type = self.parser.find('MD_ScopeCode')
        doc.format = self.parser.find('hierarchyLevelName')
        # TODO: fix temporal coverage
        # doc.temporal_coverage_begin = self.parser.doc.find('CI_Citation').find('Date').text
        # doc.temporal_coverage_end = doc.temporal_coverage_begin
        doc.geometry = self.geometry(doc)

    def geometry(self, doc):
        geometry = None
        try:
            if self.parser.doc.find('EX_GeographicBoundingBox'):
                bbox = self.parser.doc.find('EX_GeographicBoundingBox').text.split()
                geometry = shapely.geometry.box(float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))
        except Exception:
            logging.warning("could not read geometry.")
        return geometry
