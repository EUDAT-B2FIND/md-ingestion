from mdingestion.ng import classify
from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value

import logging


class DanseasyDatacite(DataCiteReader):
    NAME = 'danseasy-oai_datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        if not doc.doi:
            doc.doi = self.find_doi('alternateIdentifier')
        if not doc.source:
            doc.source = self.find_source('alternateIdentifier')
        doc.discipline = self.discipline(doc)

    def discipline(self, doc):
        classifier = classify.Classify()
        result = classifier.map_discipline(doc.keywords)
        if 'Various' not in result:
            logging.debug(f"{result} keywords={doc.keywords}")
        disc = result[0]
        # if 'Various' in disc:
        #    disc = 'Earth System Research'
        return disc
