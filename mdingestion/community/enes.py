from .base import Community
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class Enes(Community):
    NAME = 'enes'
    IDENTIFIER = 'enes'
    URL = 'http://c3grid1.dkrz.de:8080/oai/provider'
    SCHEMA = SchemaType.ISO19139
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'iso'
    OAI_SET = 'iso-old-doi'

    def update(self, doc):
        doc.doi = self.find_doi('linkage')
        doc.contact = self.find('CI_ResponsibleParty.organisationName')
        doc.contributor = 'World Data Center for Climate (WDCC)'
        doc.rights = 'For scientific use only'
        doc.discipline = self.discipline(doc, 'Earth System Research')
        doc.size = self.size(doc)

    def size(self, doc):
        number = format_value(self.find('transferSize'), one=True)
        unit = format_value(self.find('unitsOfDistribution'), one=True)
        return f"{number} {unit}"
