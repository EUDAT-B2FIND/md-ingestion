from mdingestion.ng import classify
from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value

import logging


class DarusDatacite(DataCiteReader):
    NAME = 'darus-oai_datacite'
    SNIFFER = OAISniffer

    def discipline(self, doc):
        classifier = classify.Classify()
        result = classifier.map_discipline(doc.keywords)
        if 'Various' not in result:
            logging.debug(f"{result} keywords={doc.keywords}")
        disc = result[0]
        # if 'Various' in disc:
        #    disc = 'Earth System Research'
        return disc
