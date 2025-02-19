from .base import Repository
from ..service_types import SchemaType, ServiceType


class BbmriEudatcore(Repository):
    IDENTIFIER = 'bbmri'
    TITLE = 'BBMRI'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '99916f6f-9a2c-4feb-a342-6552ac7f1529'
    PRODUCTIVE = True
    DATE = '2023-05-10'
    REPOSITORY_ID = 're3data:r3d100011394'
    REPOSITORY_NAME = 'B2SHARE'
    CRON_DAILY = False
    LOGO = ""
    LINK = ""
    DESCRIPTION = """The main vision of the project was to increase the scientific excellence and efficacy of European research in the biomedical sciences with the aim of expanding and securing competitiveness in a global context as well as attracting investments in pharmaceutical and biomedical research facilities. BBMRI reached the goals forecasted by creating a pan-European research infrastructure and by developing biobanks and biomolecular resources. Furthermore, BBMRI provided European researchers with effective access to resources and facilities, along with common services that include a joint IT service and scientific, technical, ethical, and legal expertise. Through a European framework, researchers could gain support at all stages of the biomedical R&D process. The preparatory phase of BBMRI came to an end in January 2011. And in its last year, BBMRI grew into a 54-member consortium with more than 225 associated organisations from over 30 countries, making it one of the largest research infrastructure projects in Europe. It transformed again to become a more permanent initiative, now BBMRI-ERIC."""

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.resource_type:
            doc.resource_type = 'Dataset'
        if not doc.discipline:
            doc.discipline = 'Biological and Medical Research'
