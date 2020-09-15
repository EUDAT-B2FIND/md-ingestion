from ..reader import FFReader
from ..sniffer import OAISniffer


class SLKSFF(FFReader):
    NAME = 'slks-ff'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.description = 'This record describes ancient sites and monuments as well as archaeological excavations undertaken by Danish museums.'
        doc.discipline = 'Archaeology'
        doc.publisher = 'Slots- og Kulturstyrelsen'
        doc.rights = 'For scientific use'
        doc.contact = 'post@slks.dk'
        doc.language = 'Danish'
        keywords = doc.keywords
        keywords.append('EOSC Nordic')
        keywords.append('Viking Age')
