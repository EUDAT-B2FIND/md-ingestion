from .base import Repository
from ..service_types import SchemaType, ServiceType


class TextGrid(Repository):
    IDENTIFIER = 'textgrid'
    URL = 'https://textgridlab.org/1.0/tgoaipmh/oai'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2023-06-23'
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
        doc.pid = self.find_pid('identifier')
        doc.discipline = self.discipline(doc, 'Humanities')
        doc.description = self.description(doc)
        doc.publisher = 'TextGrid'

    def description(self, doc):
        result = doc.description
        sources = self.find('source')
        found = ''
        for source in sources:
            if len(source) > len(found):
                found = source
        if found:
            result.append(f' Source: {found}')
        return result
