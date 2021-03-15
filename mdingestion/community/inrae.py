from .base import Community
from ..service_types import SchemaType, ServiceType
from ..format import format_value
import pandas as pd


CFG_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', '..', 'etc', 'Community')


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
        fname = os.path.join(CFG_DIR, 'INRAE_MappingSubject__Discipline.csv')
        df = pd.read_csv(open(fname))
        values = []
        for subject in subjects:
            result = df.loc[df.Subject == subject]
            result_discipline = list(result.Discipline.to_dict().values())[0]
            values.extend(result_discipline.split(';'))
        return values

