from .panosc import BasePanosc
from ..service_types import SchemaType, ServiceType


class ESRFDatacite(BasePanosc):
    NAME = 'esrf'
    TITLE = 'ESRF'
    IDENTIFIER = NAME
    URL = 'https://icatplus.esrf.fr/oaipmh/request'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None

    PRODUCTIVE = True
    DATE = '2022-07-14'
    DESCRIPTION = "The ESRF (European Synchrotron Radiation Facility) is the world's most intense X-ray source and a centre of excellence for fundamental and innovation-driven research in condensed and living matter science. Located in Grenoble, France, the ESRF owes its success to the international cooperation of 22 partner nations, of which 13 are Members and 9 are Associates."
    LOGO = ''

    def update(self, doc):
        doc.discipline = 'Particles, Nuclei and Fields'
        doc.publication_year = self.publicationyear(doc)
        doc.temporal_coverage = self.find('dates.date', dateType="Collected")
        doc.keywords = self.keywords(doc)
        doc.publisher = 'ESRF (European Synchrotron Radiation Facility)'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('PaN')
        return keywords

    def publicationyear (self, doc):
        pubyear = doc.publication_year
        if not pubyear:
            pubyear = self.find('dates.date', dateType="Available")
        if not pubyear:
            pubyear = self.find('dates.date', dateType="Accepted")
        return pubyear
