from .base import Repository
from ..service_types import SchemaType, ServiceType


class CompbiomedDublincore(Repository):
    IDENTIFIER = 'compbiomed'
    TITLE = 'CompBioMed'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = '03b22301-60a8-4885-86fa-8c92cbf57bb0'  # CompBioMed Set from CSC
    PRODUCTIVE = True
    DATE = '2023-01-23'
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
    CRON_DAILY = False
    LOGO = "https://www.compbiomed.eu/wp-content/uploads/2016/09/compbiomed_long_logo.png"
    DESCRIPTION = """CompBioMed is a European Commission H2020 funded Centre of Excellence focused on the use and development of computational methods for biomedical applications. We have users within academia, industry and clinical environments and are working to train more people in the use of our products and methods. Below you will find links to the user pages, which will give you more information on the work we are doing, relevant to your field, and our services which may provide further information on our work."""

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        doc.discipline = 'Biological and Medical Research'
        doc.resource_type = 'Dataset'
        doc.contact = 'Use our contact form: https://www.compbiomed.eu/contact/'
