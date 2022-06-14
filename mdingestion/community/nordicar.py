from shapely.geometry import shape
import json
import pandas as pd
import os
import copy

from .base import Community
from ..service_types import SchemaType, ServiceType

from ..format import format_value
CFG_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', '..', 'etc', 'Community')
FNAME = os.path.join(CFG_DIR, 'NORDICAR_MappingKeywords.csv')
TL = pd.read_csv(FNAME, sep=';', encoding='ISO-8859-1')


class BaseNordicar(Community):
    NAME = 'nordicar'
    TITLE = 'Nordic Archaeology'
    PRODUCTIVE = False

    def keywords_append(self, doc):
        keywords = copy.copy(doc.keywords)
        for keyword in doc.keywords:
            # print(keyword, self.IDENTIFIER)
            result = TL.loc[TL[self.IDENTIFIER] == keyword]
            # print(result, self.IDENTIFIER)
            if result.values.any():
                found = result.values[0].tolist()
                # print(found)
                found = [val for val in found if not pd.isnull(val)]
                keywords.extend(found)
        return keywords


class Slks(BaseNordicar):
    GROUP = 'slks'
    GROUP_TITLE = 'SLKS'
    IDENTIFIER = GROUP
    URL = 'https://www.archaeo.dk/ff/oai-pmh/'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = True

    def update(self, doc):
        # doc.open_access = True
        record_id = self.find('header.identifier')[0]
        # print(record_id)
        record_id = record_id.split('/')
        # print(record_id)
        record_id = record_id[-2]
        # print(record_id)
        oai_id = f'urn:repox.www.kulturarv.dkSites:http://www.kulturarv.dk/fundogfortidsminder/site/{record_id}'
        # print(oai_id)
        doc.metadata_access = f'http://www.kulturarv.dk/ffrepox/OAIHandler?verb=GetRecord&metadataPrefix=ff&identifier={oai_id}'
        doc.discipline = 'Archaeology'
        doc.publication_year = self.find('header.datestamp')
        doc.contact = self.find('publisher')
        # keywords = doc.keywords
        # keywords.append('EOSC Nordic')
        # keywords.append('Viking Age')
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


class Askeladden(BaseNordicar):
    GROUP = 'askeladden'
    GROUP_TITLE = 'Askeladden'
    IDENTIFIER = GROUP
    URL = 'https://kart.ra.no/arcgis/rest/services/Distribusjon/Kulturminner20180301/MapServer/7/query'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.ArcGIS
    FILTER = "kulturminneKategori='Arkeologisk minne'"
    PRODUCTIVE = True

    def update(self, doc):
        doc.discipline = ['Archaeology']
        doc.description = self.find('properties.informasjon')
        doc.source = self.find('properties.linkKulturminnesok')
        doc.relatedIdentifier = self.find('linkAskeladden')
        doc.publisher = ['Askeladden']
        doc.publication_year = self.find('properties.forsteDigitaliseringsdato')
        doc.language = ['Norwegian']
        doc.contact = ['askeladden.hjelp@ra.no']
        doc.creator = self.find('properties.opphav')
        doc.rights = ['NLOD (https://data.norge.no/nlod/en/2.0/)']
        doc.places = self.find('properties.kommune')
        doc.version = self.find('properties.versjonId')
        doc.title = self.title()
        doc.keywords = self.keywords()
        doc.keywords = self.keywords_append(doc)
        doc.geometry = self.geometry()

    def title(self):
        title = self.find('properties.navn')
        if not title:
            title = 'Uten navn'
        elif len(title[0]) < 4:
            title = 'Uten navn'

        return title

    def keywords(self):
        keywords = []
        keyword = self.find('properties.kulturminneOpprinneligfunksjon')
        if keyword:
            keywords.append(keyword[0])
        keyword = self.find('properties.kulturminneKategori')
        if keyword:
            keywords.append(keyword[0])
        keyword = self.find('properties.kulturminneLokalitetArt')
        if keyword:
            keywords.append(keyword[0])
        return keywords

    def geometry(self):
        geom = shape(self.reader.parser.doc['geometry'])
        # return geom.centroid
        return geom
