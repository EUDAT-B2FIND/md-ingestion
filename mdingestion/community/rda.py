from .base import Repository
from ..service_types import SchemaType, ServiceType


class RdaEudatcore(Repository):
    IDENTIFIER = 'rda'
    TITLE = 'RDA'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '8d963a29-5e19-492b-8cfe-97da4f54fad2'
    PRODUCTIVE = True
    DATE = '2023-05-10'
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
    CRON_DAILY = False
    LINK = ""
    LOGO = ""
    DESCRIPTION = """The Research Data Alliance (RDA) builds the social and technical bridges to enable the open sharing and re-use of data. The Research Data Alliance (RDA) was launched as a community-driven initiative in 2013 by the European Commission, the United States Government's National Science Foundation and National Institute of Standards and Technology, and the Australian Government’s Department of Innovation with the goal of building the social and technical infrastructure to enable open sharing and re-use of data.
    RDA has a grass-roots, inclusive approach covering all data lifecycle stages, engaging data producers, users and stewards, addressing data exchange, processing, and storage. It has succeeded in creating the neutral social platform where international research data experts meet to exchange views and to agree on topics including social hurdles on data sharing, education and training challenges, data management plans and certification of data repositories, disciplinary and interdisciplinary interoperability, as well as technological aspects. The RDA Foundation provides the core business operations of RDA and represents RDA globally."""

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.resource_type:
            doc.resource_type = 'Dataset' 
