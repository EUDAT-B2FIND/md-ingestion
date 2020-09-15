from ..reader import DublinCoreReader
from ..sniffer import OAISniffer
from ..format import format_value

import logging


class DataverseNODublinCore(DublinCoreReader):
    NAME = 'dataverseno-oai_dc'
    SNIFFER = OAISniffer

    def update(self, doc):
        #doc.contributor = ['DataverseNO']
        doc.doi = self.find_doi('identifier')
        doc.pid = self.find_pid('identifier')
        doc.source = self.source(doc)
        # doc.discipline = self.discipline(doc)
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = ['DataverseNO']

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('EOSC Nordic')
        return keywords

    def source(self, doc):
        urls = [url for url in self.find('metadata.identifier') if 'handle' not in url]
        return urls

    ## does not work: "Error: name 'classify' is not defined"
    #def discipline(self, doc):
    #    classifier = classify.Classify()
    #    result = classifier.map_discipline(doc.keywords)
    #    if 'Various' not in result:
    #        logging.debug(f"{result} keywords={doc.keywords}")
    #        disc = result[0]
    #    if 'Various' in disc:
    #        disc = 'Earth and Environmental Science'
    #    return disc
