from mdingestion.ng import classify
from ..reader import ISO19139Reader
from ..sniffer import CSWSniffer
from ..format import format_value

import logging


class DeimsISO19139(ISO19139Reader):
    NAME = 'deims-iso19139'
    SNIFFER = CSWSniffer

    def update(self, doc):
        doc.doi = self.find_doi('linkage')
        doc.pid = self.find_pid('linkage')
        doc.contributor = 'DEIMS-SDR Site and Dataset registry deims.org'
        doc.discipline = 'Environmental Monitoring'
        doc.metadata_access = [url for url in self.find('linkage') if 'deims.org/api/' in url]
        self.related_identifier(doc)
        #self.discipline(doc)
        self.fix_source(doc)

    def fix_source(self, doc):
        if not doc.source:
            source = format_value(self.find('MD_Identifier'), one=True)
            if 'xlink' in source:
                doc.source = source.split()[1]
        if doc.source.startswith("https://deims.org/datasets/"):
            doc.source = doc.source.replace("https://deims.org/datasets/", "https://deims.org/dataset/")

    def related_identifier(self, doc):
        urls = []
        for url in self.find('linkage'):
            if doc.doi and doc.doi in url:
                continue
            if doc.pid and doc.pid in url:
                continue
            if doc.source and doc.source in url:
                continue
            if doc.metadata_access and doc.metadata_access in url:
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
