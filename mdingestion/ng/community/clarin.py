from .base import Community
from .._types import SchemaType
from ..harvester import ServiceType


class BaseClarin(Community):
    NAME = 'clarin'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None

    def update(self, doc):
        doc.discipline = 'Linguistics'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.publisher:
            doc.publisher = 'CLARIN'


class ClarinOne(BaseClarin):
    IDENTIFIER = 'clarin_one'
    URL = 'http://clarin.eurac.edu/repository/oai/request'

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = 'CLARIN one'
        doc.contact = 'clarinone@something.eu'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('Clarin ONE')
        return keywords


class ClarinTwo(BaseClarin):
    IDENTIFIER = 'clarin_two'
    URL = 'http://dspace-clarin-it.ilc.cnr.it/repository/oai/request'

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = 'CLARIN two'
        doc.contact = 'clarintwo@something.eu'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('Clarin TWO')
        return keywords


class ClarinThree(BaseClarin):
    IDENTIFIER = 'clarin_three'
    URL = 'http://repository.clarin.dk/repository/oai/request'

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = 'CLARIN three'
        doc.contact = 'clarinthree@something.eu'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('Clarin THREE')
        return keywords
