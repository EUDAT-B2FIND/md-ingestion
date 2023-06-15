from .base import Repository
from ..service_types import SchemaType, ServiceType


class TuwDatacite(Repository):
    NAME = 'tuw'
    IDENTIFIER = NAME
    URL = 'https://researchdata.tuwien.ac.at/oai2d'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2023-06-15'
    REPOSITORY_ID = 're3data:r3d100013557'
    REPOSITORY_NAME = 'TU Wien Research Data'
    LOGO = "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a1/TU_Wien-Logo.svg/2000px-TU_Wien-Logo.svg.png"
    LINK = 'https://researchdata.tuwien.ac.at/'
    DESCRIPTION = """TU Wien Research Data is an institutional repository of TU Wien to enable storing, sharing and publishing of digital objects, in particular research data. It facilitates the funders' requirements for open access to research data and the FAIR principles by making research output findable, accessible, interoperable and re-usable. This service is developed by the TU Wien Center for Research Data Management and hosted by TU.it."""
