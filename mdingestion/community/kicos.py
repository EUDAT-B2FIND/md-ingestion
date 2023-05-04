from .base import Repository
from ..service_types import SchemaType, ServiceType


class KicosDublincore(Repository):
    IDENTIFIER = 'kicos'
    TITLE = 'KiCoS'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = 'ffc8b583-ebe7-41f5-88e0-65ba59fbe672'  # KiCoS Set from CSC
    PRODUCTIVE = True
    DATE = '2023-05-04'
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
    CRON_DAILY = False
    LOGO = "https://b2share.eudat.eu/img/communities/KiCoS.jpg"
    LINK = "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Projekte_RKI/KiCoS.html"
    DESCRIPTION = """The KiCoS (Kinder Corona Studies) Community is an exchange platform for studies on COVID-19 among children in Germany curated by the Robert Koch Institute. The aim is to provide researchers a space to share information on ongoing projects and to stimulate cooperation between experts. KiCoS is part of the Corona-KiTa study, a project between the German Youth Institute and the Robert Koch Institute, funded by the Federal Ministry of Health and the Federal Ministry for Family, Seniors, Women and Youth."""

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        doc.discipline = self.discipline(doc, 'Medicine')
        doc.resource_type = 'Dataset'
    #   doc.contact = '
