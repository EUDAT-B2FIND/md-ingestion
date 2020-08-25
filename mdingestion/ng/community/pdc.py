from mdingestion.ng import classify
from ..reader import FGDCReader
from ..sniffer import OAISniffer

import logging


class PDCFGDC(FGDCReader):
    NAME = 'pdc-fgdc'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = 'Polar Data Catalogue'
        #doc.discipline = 'Environmental Research'
        self.discipline(doc)

    def discipline(self, doc):
        classifier = classify.Classify()
        result = classifier.map_discipline(doc.keywords)
        if 'Various' not in result:
            logging.debug(f"{result} keywords={doc.keywords}")
        disc = result[0]
        if 'Various' in disc:
            disc = 'Environmental Research'
        doc.discipline = disc
        return disc
