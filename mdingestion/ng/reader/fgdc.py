import shapely

from .base import XMLReader
from ..sniffer import OAISniffer
from ..format import format_value

import logging


class FGDCReader(XMLReader):
    SNIFFER = OAISniffer

    def parse(self, doc):
        doc.title = self.find('title')
        doc.description = self.find('abstract')
        doc.keywords = self.find('themekey')
        # doc.discipline = format_value(self.find('subject'), type='string_words')
        doc.source = self.find('onlink')
        # doc.related_identifier = self.find('relatedIdentifier')
        doc.creator = self.find('origin')
        doc.publisher = self.find('distinfo.cntorg')
        doc.contributor = self.find('idinfo.cntorg')
        # doc.funding_reference = self.find('fundingReference.funderName')
        doc.publication_year = self.find('pubdate')
        doc.rights = self.find('distliab')
        doc.contact = self.find('cntemail')
        # doc.language = self.find('language')
        # doc.resource_type = self.find('resourceType')
        doc.format = self.find('geoform')
        # doc.size = self.find('size')
        # doc.version = self.find('metadata.version')
        doc.temporal_coverage_begin_date = self.find('begdate')
        doc.temporal_coverage_end_date = self.find('enddate')
        doc.geometry = self.find_geometry()
        # doc.places = self.find('placekt')

    def geometry(self):
        if self.parser.doc.find('bounding'):
            west = format_value(self.find('bounding.westbc'), type='float', one=True)
            east = format_value(self.find('bounding.eastbc'), type='float', one=True)
            north = format_value(self.find('bounding.northbc'), type='float', one=True)
            south = format_value(self.find('bounding.southbc'), type='float', one=True)
            try:
                # bbox: minx=west, miny=south, maxx=east, maxy=north
                geometry = shapely.geometry.box(west, south, east, north)
            except Exception:
                logging.warning('could not create geometry')
                geometry = None
        else:
            geometry = None
        return geometry
