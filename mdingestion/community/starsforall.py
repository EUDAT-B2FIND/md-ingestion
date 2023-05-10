from .base import Repository
from ..service_types import SchemaType, ServiceType


class StarsforallEudatcore(Repository):
    IDENTIFIER = 'starsforall'
    TITLE = 'STARS4ALL'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = 'a204df9b-dfa4-445e-82e6-a88f8754b6b7'
    PRODUCTIVE = True
    DATE = '2023-05-10'
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
    CRON_DAILY = False
    LOGO = "https://avatars.githubusercontent.com/u/16410737?s=280&v=4"
    LINK = "https://stars4all.eu/"
    DESCRIPTION = """The STARS4ALL foundation is a general-interest foundation, as a mechanism to provide sustainability to the main results obtained in the context of the H2020 EU project STARS4ALL, especially in relation to the production and maintenance of the TESS photometer network and the provision of open data from such network to researchers and civil society organisations. Furthermore, the STARS4ALL foundation organizes and participates often in outreach events to encourage the protection of night skies and provide information about light pollution and its negative effects."""

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'STARS4ALL'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.discipline:
            doc.discipline = 'Astronomy'
        if not doc.resource_type:
            doc.resource_type = 'Dataset'
        if not doc.contact:
            doc.contact = 'contact@stars4all.eu'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('light pullution')
        return keywords
