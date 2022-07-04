from .base import Community
from ..service_types import SchemaType, ServiceType


class BaseEudat(Community):
    NAME = 'eudat'
    TITLE = 'EUDAT'
    PRODUCTIVE = False

    def update(self, doc):
        for pub in doc.publisher:
            if '@' in pub or 'http' in pub:
                doc.publisher = ''

    def keywords(self, doc, keywords):
        # TODO: clean up code
        if not isinstance(keywords, list):
            keywords = [keywords]
        _keywords = doc.keywords
        _keywords.extend(keywords)
        return _keywords


class B2ShareCSC(BaseEudat):
    GROUP = 'b2share'
    GROUP_TITLE = 'B2SHARE'
    IDENTIFIER = 'b2share_csc'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = 'e9b9792e-79fb-4b07-b6b4-b9c2bd06d095'  # EUDAT

    def update(self, doc):
        super().update(doc)
        # doc.keywords = self.keywords(doc, 'whatever')
        if not doc.publisher:
            doc.publisher = 'EUDAT'


class B2ShareFZJ(BaseEudat):
    GROUP = 'b2share'
    GROUP_TITLE = 'B2SHARE'
    IDENTIFIER = 'b2share_fzj'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    URL = 'https://b2share.fz-juelich.de/api/oai2d'
    OAI_SET = 'e9b9792e-79fb-4b07-b6b4-b9c2bd06d095'  # EUDAT

    def update(self, doc):
        super().update(doc)
        # doc.keywords = self.keywords(doc, 'whichever')
        if not doc.publisher:
            doc.publisher = 'EUDAT'


# class SecureB2Share(BaseEudat):
#    IDENTIFIER = 'secure_b2share'
#    URL = 'https://secure-b2share-test.uio.no/api/oai2d'
#    OAI_SET = 'EUDAT'

#    def update(self, doc):
#        super().update(doc)
        # TODO: mapped to test records, clean up when ready
        # doc.keywords = self.keywords(doc, 'whatever')
#        if not doc.publisher:
#            doc.publisher = 'EUDAT'
#        doc.discipline = self.discipline(doc, 'Unknown'
