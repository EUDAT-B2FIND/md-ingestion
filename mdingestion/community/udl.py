from .base import Repository
from ..service_types import SchemaType, ServiceType


class UdLDatacite(Repository):
    IDENTIFIER = 'ul'
    URL = 'https://dorel.univ-lorraine.fr/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2022-08-31'
    LOGO = "http://b2find.dkrz.de/images/communities/udl_logo.png"
    DESCRIPTION = """The institutional data repository DOREL - DOnnées de REcherche Lorraines - is a tool for referencing the scientific production of the University of Lorraine as well as a space for publishing data sets produced within its research units. The repository is managed by a transversal team composed of coworkers from the University Libraries, the Research services, the IT services and the MSH Lorraine (Maison des Sciences de l'Homme)."""
    LINKS = ["https://dorel.univ-lorraine.fr/dataverse/univ-lorraine","https://www.univ-lorraine.fr/en/univ-lorraine/"]
