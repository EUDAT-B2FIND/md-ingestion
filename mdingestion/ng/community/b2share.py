from .base import Community
from ..service_types import SchemaType, ServiceType


class BaseB2Share(Community):
    NAME = 'b2share'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'

    def update(self, doc):
        pass

    def keywords(self, doc, keywords):
        # TODO: clean up code
        if not isinstance(keywords, list):
            keywords = [keywords]
        _keywords = doc.keywords
        _keywords.extend(keywords)
        return _keywords

class B2ShareCSC(BaseB2Share):
    IDENTIFIER = 'b2share_csc'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    OAI_SET = 'e9b9792e-79fb-4b07-b6b4-b9c2bd06d095' # EUDAT

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc, 'B2SHARE CSC')
        if not doc.publisher:
            doc.publisher = 'B2SHARE CSC'


class B2ShareFZJ(BaseB2Share):
    IDENTIFIER = 'b2share_fzj'
    URL = 'https://b2share.fz-juelich.de/api/oai2d'
    OAI_SET = 'e9b9792e-79fb-4b07-b6b4-b9c2bd06d095' # EUDAT

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc, 'B2SHARE FZJ')
        if not doc.publisher:
            doc.publisher = 'B2SHARE FZJ'
