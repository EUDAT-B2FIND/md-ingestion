from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class BaseUhh(Repository):
    # NAME = 'uhh'
    URL = 'https://www.fdr.uni-hamburg.de/oai2d'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    PRODUCTIVE = True

    def update(self, doc):
        pass


class UhhUhh(BaseUhh):
    IDENTIFIER = 'uhh_uhh'
    OAI_SET = 'user-uhh'  # Universität Hamburg


class UhhUke(BaseUhh):
    IDENTIFIER = 'uhh_uke'
    OAI_SET = 'user-uke'  # Universitätsklinikum Hamburg

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Medicine')


class UhhSignlang(BaseUhh):
    IDENTIFIER = 'uhh_signlang'
    OAI_SET = 'user-sign-lang'  # This collection contains all kinds of language resources for sign languages

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Linguistics')


class UhhCsmc(BaseUhh):
    IDENTIFIER = 'uhh_csmc'
    OAI_SET = 'user-csmc'  # Community for the Centre for the Studies of Manuscript Cultures

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Humanities')


class UhhHzsk(BaseUhh):
    IDENTIFIER = 'uhh_hzsk'
    OAI_SET = 'user-hzsk'  # Hamburger Zentrum für Sprachkorpora

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Linguistics')


class UhhNcac(BaseUhh):
    IDENTIFIER = 'uhh_ncac'
    OAI_SET = 'user-ncac'  # National Digital Archive of The Gambia

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Humanities')


class UhhFakew(BaseUhh):
    IDENTIFIER = 'uhh_fakew'
    OAI_SET = 'user-fak-ew'  # Fakultät für Erziehungswissenschaft

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Education Sciences')


class UhhIcdc(BaseUhh):
    IDENTIFIER = 'uhh_icdc'
    OAI_SET = 'user-icdc'  # Integrated Climate Data Center

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Earth System Research')


class UhhCen(BaseUhh):
    IDENTIFIER = 'uhh_cen'
    OAI_SET = 'user-cen'  # Center for Earth System Research and Sustainability

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Earth System Research')


class UhhCliccs(BaseUhh):
    IDENTIFIER = 'uhh_cliccs'
    OAI_SET = 'user-cliccs'  # Cluster of Excellence Climate, Climatic Change, and Society, TODO: append keyword: "CLICCS"

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Earth System Research')
