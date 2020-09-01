from mdingestion.ng import classify
from ..reader import ISO19139Reader
from ..sniffer import OAISniffer
from ..format import format_value

import logging


class EnviDatISO19139(ISO19139Reader):
    NAME = 'envidat-iso19139'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.doi = self.find_doi('MD_Metadata.fileIdentifier')
        doc.pid = self.find_pid('MD_Metadata.fileIdentifier')
        doc.source = self.source(doc)
        doc.discipline = self.discipline(doc)

    def source(self, doc):
        source = ''
        if not doc.source:
            for url in doc.related_identifier:
                if url.startswith('https://www.envidat.ch/dataset/'):
                    source = url
                    break
        return source

    def discipline(self, doc):
        classifier = classify.Classify()
        result = classifier.map_discipline(doc.keywords)
        if 'Various' not in result:
            logging.debug(f"{result} keywords={doc.keywords}")
        disc = result[0]
        if 'Various' in disc:
            disc = 'Environmental Research'
        return disc
