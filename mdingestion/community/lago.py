from .base import Repository
from ..service_types import SchemaType, ServiceType

# TODO: orcid-ids in creator
class LagoDublinCore(Repository):
    IDENTIFIER = 'lago'
    URL = 'http://datahub.egi.eu/oai_pmh'
    OAI_SET = '986fe2ab97a6b749fac17eb9e9b38c37chb045'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    PRODUCTIVE = True
<
    def update(self, doc):
        doc.contributor = ['EGI Datahub']
        doc.instrument = ['LAGO Observatory']
        doc.contact = ['lago-eosc@lagoproject.net']
        doc.discipline = ['Astrophysics and Astronomy']
        doc.publisher = ['LAGO Collaboration']
        doc.pid = self.find_pid('identifier')
        doc.keywords = self.keywords()

    def keywords(self):
        _keywords = self.find('subject')
        _keywords = [kw for kw in _keywords if 'http' not in kw]
        _keywords = [kw for kw in _keywords if '#' not in kw]
        return _keywords
