from .recherchedatagouv import BaseRDG
from ..service_types import SchemaType, ServiceType


class EdppDatacite(BaseRDG):
    IDENTIFIER = 'edpp'
    TITLE = 'École des Ponts-ParisTech'
    URL = 'https://entrepot.recherche.data.gouv.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'ecoledesponts'               # Set from entrepot edpp
    PRODUCTIVE = True
    DATE = ''
    DESCRIPTION = "The ESRF (European Synchrotron Radiation Facility) is the world's most intense X-ray source and a centre of excellence for fundamental and innovation-driven research in condensed and living matter science. Located in Grenoble, France, the ESRF owes its success to the international cooperation of 22 partner nations, of which 13 are Members and 9 are Associates."
    LOGO = ''
    LINK = ''
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''

