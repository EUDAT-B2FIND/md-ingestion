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
        self.update_source(doc)
        doc.discipline = self.discipline(doc)

    def discipline(self, doc):
        classifier = classify.Classify()
        result = classifier.map_discipline(doc.keywords)
        if 'Various' not in result:
            logging.debug(f"{result} keywords={doc.keywords}")
        disc = result[0]
        #if 'Various' in disc:
            #disc = 'Earth System Research'
        return disc

    def update_source(self, doc):
        if not doc.source:
            # TODO: cannot resolve urn
            for url in self.find('alternateIdentifier'):
                if doc.doi not in url:
                    doc.source = url
