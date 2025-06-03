from .base import Repository
from ..service_types import SchemaType, ServiceType


class Toar(Repository):
    IDENTIFIER = 'toar'
    TITLE = 'TOAR'
    GROUP = 'b2share'
    URL = 'https://b2share.fz-juelich.de/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '381a24f1-18d4-405d-af36-c76ba199a754'
    PRODUCTIVE = True
    DATE = '2023-02-09'
    REPOSITORY_ID = 're3data:r3d100013118'
    REPOSITORY_NAME = 'B2SHARE Server Forschungszentrum Jülich'
    LOGO = "https://igacproject.org/sites/default/files/2019-11/TOAR_Logo.png"
    LINK = 'https://igacproject.org/activities/TOAR'
    DESCRIPTION = """The "Tropospheric Ozone Assessment Report" (TOAR) has been created by ~220 scientists from 36 countries to produce the first tropospheric ozone assessment report based on the peer-reviewed literature and new analyses, and to generate easily accessible, documented data on ozone exposure and dose metrics at hundreds of measurement sites around the world (urban and non-urban), freely accessible for research on the global-scale impact of ozone on climate, human health and crop/ecosystem productivity."""

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'Tropospheric Ozone Assessment Report (TOAR)'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.resource_type:
            doc.resource_type = 'Dataset'
        if not doc.discipline:
            doc.discipline = 'Atmospheric Chemistry'
