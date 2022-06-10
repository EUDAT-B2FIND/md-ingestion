import shapely

from .base import XMLReader
from ..sniffer import OAISniffer
from ..util import convert_to_lon_180


class DublinCoreReader(XMLReader):
    SNIFFER = OAISniffer

    def parse(self, doc):
        doc.title = self.find('title')
        doc.description = self.find('description')
        doc.keywords = self.find('subject')
        doc.discipline = self.discipline(doc)
        doc.doi = self.find_doi('metadata.identifier')
        doc.pid = self.find_pid('metadata.identifier')
        doc.source = self.find_source('metadata.identifier')
        doc.related_identifier = self.related_identifier()
        doc.creator = self.find('creator')
        doc.publisher = self.find('publisher')
        doc.contributor = self.find('contributor')
        doc.publication_year = self.find('date')
        doc.rights = self.find('rights')
        doc.funding_reference = self.funding_reference()
        doc.contact = doc.publisher
        doc.language = self.find('language')
        doc.resource_type = self.find('type')
        doc.format = self.find('format')
        # doc.temporal_coverage_begin = ''
        # doc.temporal_coverage_end = ''
        doc.geometry = self.find_geometry()
        doc.places = self.places()
        doc.size = self.find('extent')
        doc.version = self.find('hasVersion')

    def related_identifier(self):
        urls = self.find('relation')
        urls.extend(self.find('source'))
        return urls

    def funding_reference(self):
        funding_refs = [info for info in self.find('relation') if 'info:eu-repo/grantAgreement' in info]
        return funding_refs

    def places(self):
        places = [s.text.strip() for s in self.parser.doc.find_all('spatial') if not s.attrs]
        return places

    def geometry(self):
        if self.parser.doc.find('spatial', attrs={'xsi:type': 'dcterms:POINT'}):
            # <dcterms:spatial xsi:type="dcterms:POINT">9.811246,56.302585</dcterms:spatial>
            point = self.parser.doc.find('spatial', attrs={'xsi:type': 'dcterms:POINT'}).text.split(',')
            lon = float(point[0])
            lon = convert_to_lon_180(lon)
            lat = float(point[1])
            # point: x=lon, y=lat
            geometry = shapely.geometry.Point(lon, lat)
        elif self.parser.doc.find('spatial', attrs={'xsi:type': 'DCTERMS:Box'}):
            # <dc:coverage>North 37.30134, South 37.2888, East -32.275618, West -32.27982</dc:coverage>
            # <dcterms:spatial xsi:type="DCTERMS:Box">37.2888 -32.27982 37.30134 -32.275618</dcterms:spatial>
            bbox = self.parser.doc.find('spatial', attrs={'xsi:type': 'DCTERMS:Box'}).text.split()
            south = float(bbox[0])
            east = float(bbox[1])
            east = convert_to_lon_180(east)
            north = float(bbox[2])
            west = float(bbox[3])
            west = convert_to_lon_180(west)
            # bbox: minx=west, miny=south, maxx=east, maxy=north
            geometry = shapely.geometry.box(west, south, east, north)
        else:
            geometry = None
        return geometry
