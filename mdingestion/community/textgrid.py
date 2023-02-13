from .base import Repository
from ..service_types import SchemaType, ServiceType


class TextGrid(Repository):
    IDENTIFIER = 'textgrid'
    URL = 'https://textgridlab.org/1.0/tgoaipmh/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = False
    DATE = '2023-02-09'
    CRON_DAILY = False
    LOGO = "http://b2find.dkrz.de/images/communities/textgrid_logo.png"
    DESCRIPTION = """

    https://textgrid.de/en/

    TextGrid

    The TextGrid Repository is a long-term archive for humanities research data. It provides an extensive, searchable, and reusable repository of texts and images. Aligned with the principles of Open Access and FAIR, the TextGrid Repository was awarded the CoreTrustSeal in 2020. For researchers, the TextGrid Repository offers a sustainable, durable, and secure way to publish their research data in a citable manner and to describe it in an understandable way through required metadata. Read more about sustainability, FAIR and Open Access in the Mission Statement of the TextGrid Repository.
    """
    REPOSITORY_ID = 're3data:r3d100011365'
    REPOSITORY_NAME = 'TextGrid'

    def update(self, doc):
        doc.discipline = doc.discipline = self.discipline(doc, 'Humanities')
