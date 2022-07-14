from .panosc import BasePanosc
from ..service_types import SchemaType, ServiceType


class EssDatacite(BasePanosc):
    NAME = 'ess'
    TITLE = 'ESS'
    IDENTIFIER = NAME
#    URL = 'https://scicat.esss.se/openaire/oai'
    URL = 'https://oai.panosc.ess.eu/openaire/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2020-11-13'
    DESCRIPTION = "The European Spallation Source ESS is a European Research Infrastructure Consortium (ERIC), a multi-disciplinary research facility based on the world’s most powerful neutron source. Our vision is to build and operate the world’s most powerful neutron source, enabling scientific breakthroughs in research related to materials, energy, health and the environment, and addressing some of the most important societal challenges of our time. SciCat is the Metadata Catalogue at European Spallation Source."
    LOGO = ''
    # harvesting with: b2f harvest -c ess -k

    def update(self, doc):
        doc.doi = self.find_doi('identifier', identifierType="URL")
        doc.discipline = 'Particles, Nuclei and Fields'
        doc.keywords = self.keywords(doc)
        # doc.open_access = True

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('PaN')
        return keywords
