from .b2share import BaseB2Share
from ..service_types import SchemaType, ServiceType


class FMI(BaseB2Share):
    NAME = 'fmi'
    TITLE = 'FMI'
    IDENTIFIER = NAME
    GROUP = 'b2share'
    GROUP_TITLE = 'B2SHARE'
    URL = 'https://fmi.b2share.csc.fi/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '77f140b0-d4aa-437e-80d4-32c0abd3746f'

    def update(self, doc):
        doc.discipline = self._discipline(doc, 'Meteorology')
        doc.publisher = 'Finnish Meteorological Institute'
        doc.keywords = self.keywords()
        doc.language = self.language()
        self._identifier(doc)
        # doc.funding_reference = self.find('Funder')

    def _identifier(self, doc):
        for id in self.find('identifier'):
            if 'doi:' in id:
                doc.doi = id.split('doi:')[-1]
            if 'pid:' in id:
                doc.pid = id.split('pid:')[-1]
            if 'url:' in id:
                doc.source = id.split('url:')[-1]

    def _discipline(self, doc, default=None):
        disc = self.find('discipline')
        if disc:
            _disc = []
            for d in disc:
                _disc.append(d.split('→')[-1])
            disc = _disc
        else:
            disc = [default]
        return disc

    def keywords(self):
        _keywords = self.find('keyword')
        if not _keywords:
            _keywords = self.find('subject')
        return _keywords

    def language(self):
        _language = self.find('language')
        if not _language or _language[0] == '':
            _language = []
            for lang in self._reader.parser.doc.find_all('language'):
                _language.append(lang.get('language_name'))
        return _language
