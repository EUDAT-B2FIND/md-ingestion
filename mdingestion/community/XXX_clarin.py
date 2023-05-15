from .base import Repository
from ..service_types import SchemaType, ServiceType


class BaseClarin(Repository):
    # GROUP = 'clarin'
    NAME = 'clarin'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    # OAI_SET = None
    PRODUCTIVE = True
    REPOSITORY_ID = 're3data:r3d100010386'
    REPOSITORY_NAME = 'CLARIN'

    def update(self, doc):
        doc.discipline = 'Linguistics'
        # doc.doi = self.find_doi('metadata.relation')
        # doc.pid = self.find_pid('metadata.relation')
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.publisher:
            doc.publisher = 'CLARIN'

    def keywords(self, doc, keywords):
        # TODO: clean up code
        if not isinstance(keywords, list):
            keywords = [keywords]
        _keywords = doc.keywords
        _keywords.extend(keywords)
        return _keywords


class ClarinOne(BaseClarin):
    IDENTIFIER = 'clarin_one'
    URL = 'http://clarin.eurac.edu/repository/oai/request'

    def update(self, doc):
        super().update(doc)
        # doc.keywords = self.keywords(doc, 'clarin ONE')
        # doc.contact = 'clarinone@something.eu'


class ClarinTwo(BaseClarin):
    IDENTIFIER = 'clarin_two'
    URL = 'http://dspace-clarin-it.ilc.cnr.it/repository/oai/request'


class ClarinThree(BaseClarin):
    IDENTIFIER = 'clarin_three'
    URL = 'http://repository.clarin.dk/repository/oai/request'


class ClarinFour(BaseClarin):
    IDENTIFIER = 'clarin_four'
    URL = 'http://lindat.mff.cuni.cz/repository/oai/request'


class ClarinFive(BaseClarin):
    IDENTIFIER = 'clarin_five'
    URL = 'http://www.clarin.si/repository/oai/request'


# class ClarinSix(BaseClarin):
#  IDENTIFIER = 'clarin_six'
#  URL = 'https://repo.spraakbanken.gu.se/oai/request'


class ClarinSeven(BaseClarin):
    IDENTIFIER = 'clarin_seven'
    URL = 'http://fedora.clarin-d.uni-saarland.de/oaiprovider/'


class ClarinEight(BaseClarin):
    IDENTIFIER = 'clarin_eight'
    URL = 'https://repo.clarino.uib.no/oai/request'


class ClarinNine(BaseClarin):
    IDENTIFIER = 'clarin_nine'
    URL = 'https://portulanclarin.net/repository/oaipmh/'
    OAI_SET = 'hdl_11321_1'


class ClarinTen(BaseClarin):
    IDENTIFIER = 'clarin_ten'
    URL = 'https://clarin-pl.eu/oai/request'
    OAI_SET = 'hdl_11321_1'


class ClarinEleven(BaseClarin):
    IDENTIFIER = 'clarin_eleven'
    URL = 'https://clarin-pl.eu/oai/request'
    OAI_SET = 'hdl_11321_2'


class ClarinTwelve(BaseClarin):
    IDENTIFIER = 'clarin_twelve'
    URL = 'https://clarin-pl.eu/oai/request'
    OAI_SET = 'hdl_11321_3'


class ClarinThirteen(BaseClarin):
    IDENTIFIER = 'clarin_thirteen'
    URL = 'https://clarin-pl.eu/oai/request'
    OAI_SET = 'hdl_11321_4'


# class ClarinFourteen(BaseClarin): --> doesn work properly
#  IDENTIFIER = 'clarin_fourteen'
#  URL = 'https://metashare.ut.ee/oai_pmh/'
#  PRODUCTIVE = False


class ClarinFifteen(BaseClarin):
    IDENTIFIER = 'clarin_fifteen'
    URL = 'https://clarin.vdu.lt/oai/request'

class ClarinSixteen(BaseClarin):
    IDENTIFIER = 'clarin_sixteen'
    URL = 'https://kielipankki.fi/md_api/que'


class ClarinFromB2SatCSC(BaseClarin):
    IDENTIFIER = 'clarin_b2s'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    OAI_SET = '0afede87-2bf2-4d89-867e-d2ee57251c62'  # CLARIN Subset

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc, 'B2SHARE')
        if not doc.publisher:
            doc.publisher = 'B2SHARE'
