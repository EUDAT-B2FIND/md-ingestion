from shapely.geometry import shape
from .nordicar import BaseNordicar
from ..service_types import SchemaType, ServiceType


class Slks(BaseNordicar):
    IDENTIFIER = 'slks'
    TITLE = 'SLKS'
    URL = 'https://www.archaeo.dk/ff/oai-pmh/'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2020-09-23'

    def update(self, doc):
        # doc.open_access = True
        record_id = self.find('header.identifier')[0]
        record_id = record_id.split('/')
        record_id = record_id[-2]
        oai_id = f'urn:repox.www.kulturarv.dkSites:http://www.kulturarv.dk/fundogfortidsminder/site/{record_id}'
        doc.metadata_access = f'http://www.kulturarv.dk/ffrepox/OAIHandler?verb=GetRecord&metadataPrefix=ff&identifier={oai_id}'
        doc.discipline = 'Archaeology'
        doc.publication_year = self.find('header.datestamp')
        doc.contact = self.find('publisher')
        doc.keywords = self.keywords_append(doc)
        doc.temporal_coverage = self.temporal_coverage(doc)

    def temporal_coverage(self, doc):
        temporal = self.find('temporal')
        period = temporal[0]
        year = temporal[1]
        from_year = int(year.split(',')[0])
        doc.temporal_coverage_begin_date = f"{from_year}"
        if from_year < 0:
            from_year = f"{abs(from_year)} BC"
        else:
            from_year = f"{from_year} AD"
        to_year = int(year.split(',')[1])
        doc.temporal_coverage_end_date = f"{to_year}"
        if to_year < 0:
            to_year = f"{abs(to_year)} BC"
        else:
            to_year = f"{to_year} AD"
        if len(temporal) >= 3:
            main_period = temporal[2]
            coverage = f"{from_year} - {to_year}; {period}; {main_period}"
        else:
            coverage = f"{from_year} - {to_year}; {period}"
        return coverage
