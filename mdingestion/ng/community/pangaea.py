from mdingestion.ng import classify
from ..reader import DataCiteReader
from ..sniffer import OAISniffer

import logging


class PangaeaDatacite(DataCiteReader):
    NAME = 'pangaea-datacite4'
    SNIFFER = OAISniffer

    def update(self, doc):
        # doc.discipline = 'Earth System Research'
        doc.discipline = self.discipline(doc)

    def discipline(self, doc):
        classifier = classify.Classify()
        result = classifier.map_discipline(doc.keywords)
        if 'Various' not in result:
            logging.debug(f"{result} keywords={doc.keywords}")
        disc = result[0]
        if 'Various' in disc:
            disc = 'Earth System Research'
        return disc
