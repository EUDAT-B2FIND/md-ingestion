from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value
import pandas as pd
import os


CFG_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', '..', 'etc', 'Community')
FNAME = os.path.join(CFG_DIR, 'INRAE_MappingSubject__Discipline.csv')
DF = pd.read_csv(open(FNAME))


class INRAEDatacite(Repository):
    IDENTIFIER = 'inrae'
#   URL = 'https://data.inrae.fr/oai'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'INRAE'
#    OAI_SET = 'INRAE_PNDB'
    GROUP = 'recherchedatagouv'
    PRODUCTIVE = True
    DATE = '2023-03-31'
    REPOSITORY_ID = 're3data:r3d100012673'
    REPOSITORY_NAME = 'Data INRAE'

    """def update(self, doc):
        handle = format_value(self.find('resource.identifier', identifierType="Handle"), one=True)
        if handle:
            urls = self.reader.pid()
            urls.append(f'http://hdl.handle.net/{handle}')
            doc.pid = urls
        if not doc.publisher:
            doc.publisher = 'INRAE'
        doc.discipline = self.discipline_mapping(doc, doc.keywords)

    def discipline_mapping(self, doc, subjects):
        values = []
        _subjects = subjects.copy()
        if "Health and Life Sciences" in _subjects:
            if "Medicine" in _subjects:
                _subjects.remove("Medicine")
        for subject in _subjects:
            result = DF.loc[DF.Subject == subject]
            result_disciplines = list(result.Discipline.to_dict().values())
            if result_disciplines:
                values.extend(result_disciplines[0].split(';'))
            else:
                values.extend(self.discipline(doc, [subject]))
        return values"""
