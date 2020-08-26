from mdingestion.ng import classify
from ..reader import ISO19139Reader
from ..sniffer import OAISniffer
from ..format import format_value

import logging


class EnviDatISO19139(ISO19139Reader):
    NAME = 'envidat-iso19139'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.doi = self.find_doi('linkage')
        doc.pid = self.find_pid('linkage')
        doc.discipline = 'Environmental Research'
        self.related_identifier(doc)
        self.discipline(doc)

    def related_identifier(self, doc):
        urls = []
        for url in self.find('linkage'):
            if doc.doi and doc.doi in url:
                continue
            if doc.pid and doc.pid in url:
                continue
            if doc.source and doc.source in url:
                continue
            urls.append(url)
        doc.related_identifier = urls

    def discipline(self, doc):
        classifier = classify.Classify()
        result = classifier.map_discipline(doc.keywords)
        if 'Various' not in result:
            logging.debug(f"{result} keywords={doc.keywords}")
        disc = result[0]
        if 'Various' in disc:
            disc = 'Environmental Monitoring'
        doc.discipline = disc
        return disc
