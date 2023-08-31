import shapely

from .base import XMLReader
from ..sniffer import CSWSniffer
from ..format import format_value
from ..util import convert_to_lon_180
from ..service_types import SchemaType

import logging


class ISO19139Reader(XMLReader):
    SNIFFER = CSWSniffer
    SCHEMA = SchemaType.ISO19139

    def parse(self, doc):
        # 'identifier' always defined in community mapfile!
        doc.related_identifier = self.find('linkage')
        doc.title = self.find('MD_DataIdentification.CI_Citation.title.CharacterString')
        doc.description = self.find('abstract')
        doc.keywords = self.find('MD_Keywords.keyword')
        doc.creator = self._creator(doc)
        # doc.instrument = self.find('')
        doc.publisher = self._publisher(doc)
        doc.contributor = self.find('MD_DataIdentification.credit')
        doc.publication_year = self.find('MD_DataIdentification.CI_Citation.Date')
        doc.rights = self.find('MD_LegalConstraints')
        doc.contact = self.find('contact.electronicMailAddress.CharacterString')
        # doc.funding_reference = self.find('MD_DataIdentification.supplementalInformation.CharacterString') #always check with each community
        doc.language = self.find('MD_Metadata.language')
        doc.resource_type = self.find('contentInfo.contentType')
        doc.format = self.find('MD_Format.name.CharacterString')
        doc.size = self.find('MD_DigitalTransferOptions.transferSize.Real')
        doc.version = self.find('distributionFormat.version')
        doc.temporal_coverage_begin_date = self.find('EX_TemporalExtent.beginPosition')
        doc.temporal_coverage_end_date = self.find('EX_TemporalExtent.endPosition')
        doc.geometry = self.find_geometry()

    def _creator(self, doc):
        selected_creators = []
        try:
            creators = self.reader.parser.doc.MD_DataIdentification.CI_Citation.find_all('citedResponsibleParty')
            for creator in creators:
                try:
                    name = creator.individualName.CharacterString.text
                    codetype = creator.role.CI_RoleCode['codeListValue']
                    if codetype in ['owner', 'originator', 'pointOfContact', 'principalInvestigator', 'author']:
                        selected_creators.append(name)
                except Exception:
                    pass
        except Exception:
            pass
        return selected_creators

    def _publisher(self, doc):
        selected_publishers = []
        try:
            publishers = self.reader.parser.doc.MD_DataIdentification.CI_Citation.citedResponsibleParty.find_all('CI_ResponsibleParty')
            for publisher in publishers:
                try:
                    name = publisher.organisationName.CharacterString.text
                    codetype = publisher.role.CI_RoleCode['codeListValue']
                    if codetype in ['publisher', 'resourceProvider']:
                        selected_publishers.append(name)
                except Exception:
                    pass
        except Exception:
            pass
        return selected_publishers

    def geometry(self):
        geometry = None
        try:
            if self.find('EX_GeographicBoundingBox'):
                # we get from iso: west, east, south, north
                west = format_value(self.find('EX_GeographicBoundingBox.westBoundLongitude'), type='float', one=True)
                # west = convert_to_lon_180(west)
                east = format_value(self.find('EX_GeographicBoundingBox.eastBoundLongitude'), type='float', one=True)
                # east = convert_to_lon_180(east)
                if east > 180:
                    west = west - 180.0
                    east = east - 180.0
                south = format_value(self.find('EX_GeographicBoundingBox.southBoundLatitude'), type='float', one=True)
                north = format_value(self.find('EX_GeographicBoundingBox.northBoundLatitude'), type='float', one=True)
                # bbox: minx=west, miny=south, maxx=east, maxy=north
                geometry = shapely.geometry.box(west, south, east, north)
                # bbox = self.parser.doc.find('EX_GeographicBoundingBox').text.split()
                # geometry = shapely.geometry.box(float(bbox[0]), float(bbox[2]), float(bbox[1]), float(bbox[3]))
        except Exception:
            logging.warning("could not read geometry.")
        return geometry
