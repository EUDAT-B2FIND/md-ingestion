from .base import Repository
from ..service_types import SchemaType, ServiceType


class BaseClarin(Repository):
    NAME = 'clarin'
    TITLE = 'CLARIN'
    PRODUCTIVE = True
    DATE = '2023-06-17'
    LINK = 'https://www.clarin.eu/'
    LOGO = ""
    DESCRIPTION = """CLARIN stands for Common Language Resources and Technology Infrastructure. It is a research infrastructure that was initiated from the vision that all digital language resources and tools from all over Europe and beyond are accessible through a single sign-on online environment for the support of researchers in the humanities and social sciences."""

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
        _keywords = doc.keywords
        _keywords.extend(keywords)
        return _keywords


class ClarinOne(BaseClarin):
    IDENTIFIER = 'clarin_one'
    URL = 'http://clarin.eurac.edu/repository/oai/request'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    REPOSITORY_ID = 're3data:r3d100012860'
    REPOSITORY_NAME = 'Eurac Research CLARIN Centre'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'clarin@eurac.edu'


class ClarinTwo(BaseClarin):
    IDENTIFIER = 'clarin_two'
    URL = 'http://dspace-clarin-it.ilc.cnr.it/repository/oai/request'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    REPOSITORY_ID = 're3data:r3d100012262'
    REPOSITORY_NAME = 'ILC-CNR for CLARIN-IT repository'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'dspace-clarin-it-ilc-help@ilc.cnr.it'


class ClarinThree(BaseClarin):
    IDENTIFIER = 'clarin_three'
    URL = 'http://repository.clarin.dk/repository/oai/request'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    REPOSITORY_ID = 're3data:r3d100010387'
    REPOSITORY_NAME = 'CLARIN-DK-UCPH Repository'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'info@clarin.dk'


class ClarinFour(BaseClarin):
    IDENTIFIER = 'clarin_four'
    URL = 'http://lindat.mff.cuni.cz/repository/oai/request'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    REPOSITORY_ID = 're3data:r3d100010386'
    REPOSITORY_NAME = 'LINDAT/CLARIAH-CZ repository'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'lindat-help@ufal.mff.cuni.cz'


class ClarinFive(BaseClarin):
    IDENTIFIER = 'clarin_five'
    URL = 'http://www.clarin.si/repository/oai/request'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    REPOSITORY_ID = 're3data:r3d100011922'
    REPOSITORY_NAME = 'CLARIN.SI repository'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'info@clarin.si'


class ClarinSix(BaseClarin):
    IDENTIFIER = 'clarin_six'
    URL = 'https://repo.spraakbanken.gu.se/oai/request'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    REPOSITORY_ID = 're3data:r3d100011013'
    REPOSITORY_NAME = 'Språkbanken Text'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'sb-info@svenska.gu.se'


class ClarinSeven(BaseClarin):
    IDENTIFIER = 'clarin_seven'
    URL = 'http://fedora.clarin-d.uni-saarland.de/oaiprovider/'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    REPOSITORY_ID = 're3data:r3d100010384'
    REPOSITORY_NAME = 'UdS Fedora Commons Repository'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'j.knappen@mx.uni-saarland.de'


class ClarinEight(BaseClarin):
    IDENTIFIER = 'clarin_eight'
    URL = 'https://repo.clarino.uib.no/oai/request'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    REPOSITORY_ID = 're3data:r3d100012020'
    REPOSITORY_NAME = 'CLARINO Bergen Center repository'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'clarin@uib.no'


class ClarinNine(BaseClarin):
    IDENTIFIER = 'clarin_nine'
    URL = 'https://portulanclarin.net/repository/oaipmh/'
    SCHEMA = SchemaType.OLAC          # problems with identifier!
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'olac'
    REPOSITORY_ID = 're3data:r3d100013046'
    REPOSITORY_NAME = 'PORTULAN CLARIN repository'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'https://portulanclarin.net/contact/'


class ClarinTen(BaseClarin):
    IDENTIFIER = 'clarin_ten'
    URL = 'https://clarin-pl.eu/oai/request'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    REPOSITORY_ID = 're3data:r3d100011802'
    REPOSITORY_NAME = 'CLARIN-PL'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'clarin-pl@pwr.edu.pl'


class ClarinEleven(BaseClarin):
    IDENTIFIER = 'clarin_eleven'
    URL = 'https://metashare.ut.ee/oai_pmh/'
    SCHEMA = SchemaType.OLAC          # problems with identifier!
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'olac'
    REPOSITORY_ID = 're3data:r3d100011941'
    REPOSITORY_NAME = 'Center of Estonian Language Resources'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'info@keeleressursid.ee'


class ClarinTwelve(BaseClarin):
    IDENTIFIER = 'clarin_twelve'
    URL = 'https://clarin.vdu.lt/oai/request'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    REPOSITORY_ID = 're3data:r3d100012254'
    REPOSITORY_NAME = 'CLARIN-LT'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'info@clarin.vdu.lt'


"""
class ClarinThirteen(BaseClarin):
    IDENTIFIER = 'clarin_thirteen'
    URL = 'https://kielipankki.fi/md_api/que'
    URL = 'https://kielipankki.fi/md_api/que?verb=ListRecords&metadataPrefix=info' # special request
    SCHEMA = SchemaType.INFO # needs to be written and integrated!
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'info'
    REPOSITORY_ID = 're3data:r3d100011807'
    REPOSITORY_NAME = 'Kielipankki'

    def update(self, doc):
        super().update(doc)
        doc.contact = 'kielipankki @csc.fi '
"""


class ClarinFromB2SatCSC(BaseClarin):
    IDENTIFIER = 'clarin_b2s'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    OAI_SET = '0afede87-2bf2-4d89-867e-d2ee57251c62'  # CLARIN Subset
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc, 'B2SHARE')
        if not doc.publisher:
            doc.publisher = 'B2SHARE'
