from .base import Community
from ..service_types import SchemaType, ServiceType
from ..format import format_value
import pandas as pd
import os


CFG_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', '..', 'etc', 'Community')
FNAME = os.path.join(CFG_DIR, 'INRAE_MappingSubject__Discipline.csv')
DF = pd.read_csv(open(FNAME))


class DataverseNODatacite(Community):
    NAME = 'inrae'
    IDENTIFIER = 'inrae'
    URL = 'https://data.inrae.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'NoGeneticResource'

    def update(self, doc):
        handle = format_value(self.find('resource.identifier', identifierType="Handle"), one=True)
        if handle:
            urls = self.reader.pid()
            urls.append(f'http://hdl.handle.net/{handle}')
            doc.pid = urls
        if not doc.publisher:
            doc.publisher = 'INRAE'
        doc.discipline = self.discipline(doc, self.discipline_mapping(doc.keywords))

    def discipline_mapping(self, subjects):
        values = []
        for subject in subjects:
            result = DF.loc[DF.Subject == subject]
            result_disciplines = list(result.Discipline.to_dict().values())
            if result_disciplines:
                values.extend(result_disciplines[0].split(';'))
        return values

