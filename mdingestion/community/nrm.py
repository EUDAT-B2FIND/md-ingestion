from .base import Repository
from ..service_types import SchemaType, ServiceType


class NrmEudatcore(Repository):
    IDENTIFIER = 'nrm'
    TITLE = 'NRM'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '4ba7c0fd-1435-4313-9c13-4d888d60321a'
    PRODUCTIVE = True
    DATE = '2023-05-23'
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
    CRON_DAILY = False
    LOGO = "https://upload.wikimedia.org/wikipedia/commons/thumb/7/7a/Naturhistoriska_riksmuseet_logo.svg/1181px-Naturhistoriska_riksmuseet_logo.svg.png"
    LINK = "https://herbarium.nrm.se/search/specimens/"
    DESCRIPTION = """The Swedish Museum of Natural History is a government agency that reports to the Ministry of Culture. Our task is to promote interest in, and knowledge and research on, the origins and development of the universe and Earth, on the plant and animal worlds and on the biology and natural environment of human beings. the botanical collection database of the Swedish Museum of Natural History, with information on specimens and species of fungi, lichens, algae, bryophytes and vascular plants."""

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.resource_type:
            doc.resource_type = 'Dataset'
        if not doc.discipline:
            doc.discipline = 'Ecology'
        doc.contact = 'herbarium@nrm.se'
