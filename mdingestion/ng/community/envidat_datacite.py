from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value
from mdingestion.ng import classify

import logging


class EnvidatDatacite(DataCiteReader):
    NAME = 'envidat-datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.source = self.find_source('alternateIdentifier')
        if not doc.publisher:
            doc.publisher = 'EnviDat'
        doc.contributor = self.contributor(doc)
        doc.discipline = self.discipline(doc)

    def contributor(self, doc):
        contributor = [name for name in doc.contributor if name not in doc.contact]
        contributor.append('EnviDat')
        return contributor

    def discipline(self, doc):
        classifier = classify.Classify()
        result = classifier.map_discipline(doc.keywords)
        if 'Various' not in result:
            logging.debug(f"{result} keywords={doc.keywords}")
        disc = result[0]
        if 'Various' in disc:
            disc = 'Environmental Research'
        return disc
