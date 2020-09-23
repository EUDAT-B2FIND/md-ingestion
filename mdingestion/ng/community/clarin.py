from .base import Community
from ..service_types import SchemaType, ServiceType


class BaseClarin(Community):
    NAME = 'clarin'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    #OAI_SET = None

    def update(self, doc):
        doc.discipline = 'Linguistics'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.publisher:
            doc.publisher = 'CLARIN'

    def keywords(self, doc, keywords):
        # TODO: clean up code
        if not isinstance(keywords, list):
            keywords = [keywords]
        keywords = doc.keywords
        keywords.extend(keywords)
        return keywords


class ClarinOne(BaseClarin):
    IDENTIFIER = 'clarin_one'
    URL = 'http://clarin.eurac.edu/repository/oai/request'

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc, 'clarin ONE')
        doc.doi = self.find_doi('metadata.relation')
        doc.pid = self.find_pid('metadata.relation')
        if not doc.publisher:
            doc.publisher = 'CLARIN one'
        doc.contact = 'clarinone@something.eu'


class ClarinTwo(BaseClarin):
    IDENTIFIER = 'clarin_two'
    URL = 'http://dspace-clarin-it.ilc.cnr.it/repository/oai/request'

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc, 'clarin TWO')
        if not doc.publisher:
            doc.publisher = 'CLARIN two'
        doc.contact = 'clarintwo@something.eu'

class ClarinThree(BaseClarin):
    IDENTIFIER = 'clarin_three'
    URL = 'http://repository.clarin.dk/repository/oai/request'

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc, 'clarin THREE')
        if not doc.publisher:
            doc.publisher = 'CLARIN three'
        doc.contact = 'clarinthree@something.eu'

class ClarinFour(BaseClarin):
    IDENTIFIER = 'clarin_four'
    URL = 'http://lindat.mff.cuni.cz/repository/oai/request'

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc, 'clarin FOUR')
        if not doc.publisher:
            doc.publisher = 'CLARIN four'
        doc.contact = 'clarinfour@something.eu'

class ClarinFromB2SatCSC(BaseClarin):
    IDENTIFIER = 'clarin_b2s'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    OAI_SET = '0afede87-2bf2-4d89-867e-d2ee57251c62'

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc, 'clarin B2SHARE')
        if not doc.publisher:
            doc.publisher = 'CLARIN B2SHARE'
        doc.contact = 'clarinb2s@something.eu'
