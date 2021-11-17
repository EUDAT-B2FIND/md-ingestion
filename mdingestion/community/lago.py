from .base import Community
from ..service_types import SchemaType, ServiceType


class LagoDublinCore(Community):
    NAME = 'lago'
    IDENTIFIER = 'lago'
    URL = 'http://datahub.egi.eu/oai_pmh'
    OAI_SET = '986fe2ab97a6b749fac17eb9e9b38c37chb045'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    PRODUCTIVE = True

    def update(self, doc):
        doc.contributor = ['EGI Datahub']
        doc.instrument = ['LAGO Observatory']
        doc.contact = ['lago-eosc@lagoproject.net']
        doc.discipline = ['Astrophysics and Astronomy']
        doc.publisher = ['LAGO Collaboration']
        doc.pid = self.find_pid('identifier')
