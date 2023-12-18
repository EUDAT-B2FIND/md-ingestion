from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..core.doc import OriginalProv
import pandas as pd
import os

CFG_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', '..', 'etc', 'Community')
FNAME = os.path.join(CFG_DIR, 'CESSDA_repoID_mapping.csv')
DF = pd.read_csv(FNAME, sep=';', encoding='ISO-8859-1')


class CessdaDDI25(Repository):
    IDENTIFIER = 'cessda'
    URL = 'https://datacatalogue.cessda.eu/oai-pmh/v0/oai'
    SCHEMA = SchemaType.DDI25
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_ddi25'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2022-10-12'
    REPOSITORY_ID = 're3data:r3d100010202'
    REPOSITORY_NAME = 'CESSDA'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Social Sciences')
        doc.publisher = self.publisher()
        doc.original_prov = self.origin_prov()

    def publisher(self):
        publisher = []
        publisher.extend(self.find('distrbtr'))
        if not publisher:
            publisher.append('CESSDA')
        return publisher

    def origin_prov(self):
        about = OriginalProv()
        orig_descr = self.reader.parser.doc.find('originDescription')
        if orig_descr:
            about.harvest_date = orig_descr.get('harvestDate')
            about.altered = orig_descr.get('altered')
            about.base_url = orig_descr.baseURL.text
            about.identifier = orig_descr.identifier.text
            about.datestamp = orig_descr.datestamp.text
            about.metadata_namespace = orig_descr.metadataNamespace.text
            about.repository_id, about.repository_name = self.map_repoid(about.base_url)
        return about.as_string()

    def map_repoid(self, base_url):
        repoid = reponame = ''
        results = DF.loc[DF.baseURL == base_url]
        if not results.empty:
            repoid = list(results.repository_id)[0]
            reponame = list(results.repository_name)[0]
        return repoid, reponame
