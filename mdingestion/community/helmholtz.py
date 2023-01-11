from .base import Repository
from ..service_types import SchemaType, ServiceType


class HelmholtzEudatcore(Repository):
    NAME = 'helmholtz'
    TITLE = 'Helmholtz'
    IDENTIFIER = NAME
    URL = 'https://b2share.fz-juelich.de/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '5bbb7b0d-51ed-410a-865b-69d6334a3f0e'  # Helmholtz
    PRODUCTIVE = False

#    def update(self, doc):
#        doc.discipline = 'Particles, Nuclei and Fields'
#        doc.publication_year = self.publicationyear(doc)
#        doc.temporal_coverage = self.find('dates.date', dateType="Collected")
#        doc.keywords = self.keywords(doc)
#        doc.publisher = 'ESRF (European Synchrotron Radiation Facility)'
#
#    def keywords(self, doc):
#        keywords = doc.keywords
#        keywords.append('PaN')
#        return keywords
#
#    def publicationyear (self, doc):
#        pubyear = doc.publication_year
#        if not pubyear:
#            pubyear = self.find('dates.date', dateType="Available")
#        if not pubyear:
#            pubyear = self.find('dates.date', dateType="Accepted")
#        return pubyear
