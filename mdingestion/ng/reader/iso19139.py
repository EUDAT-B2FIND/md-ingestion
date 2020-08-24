import shapely

from .base import XMLReader
from ..sniffer import CSWSniffer
from ..format import format_value

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
        doc.rights = self.find('MD_LegalConstraints')
        doc.contact = self.find('contact.electronicMailAddress')
        doc.language = self.find('LanguageCode')
        doc.resource_type = self.find('hierarchyLevel.MD_ScopeCode')
        doc.format = self.find('distributionFormat')
        # TODO: fix temporal coverage
        doc.temporal_coverage_begin_date = self.find('EX_TemporalExtent.beginPosition')
        doc.temporal_coverage_end_date = self.find('EX_TemporalExtent.endPosition')
        doc.geometry = self.geometry()

    def geometry(self):
        geometry = None
        try:
            if self.find('EX_GeographicBoundingBox'):
                # we get from iso: west, east, south, north
                west = format_value(self.find('EX_GeographicBoundingBox.westBoundLongitude'), type='float', one=True)
                east = format_value(self.find('EX_GeographicBoundingBox.eastBoundLongitude'), type='float', one=True)
                south = format_value(self.find('EX_GeographicBoundingBox.southBoundLatitude'), type='float', one=True)
                north = format_value(self.find('EX_GeographicBoundingBox.northBoundLatitude'), type='float', one=True)
                # bbox: minx=west, miny=south, maxx=east, maxy=north
                geometry = shapely.geometry.box(west, south, east, north)
                # bbox = self.parser.doc.find('EX_GeographicBoundingBox').text.split()
                # geometry = shapely.geometry.box(float(bbox[0]), float(bbox[2]), float(bbox[1]), float(bbox[3]))
        except Exception:
            logging.warning("could not read geometry.")
        return geometry
