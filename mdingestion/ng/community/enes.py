from mdingestion.ng import classify
from ..reader import ISO19139Reader
from ..sniffer import OAISniffer
from ..format import format_value

import logging


class ENESISO19139(ISO19139Reader):
    NAME = 'enes-iso'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.doi = self.find_doi('linkage')
        doc.contact = self.find('CI_ResponsibleParty.organisationName')
        doc.contributor = 'World Data Center for Climate (WDCC)'
        doc.rights = 'For scientific use only'
        doc.discipline = self.discipline(doc)
        doc.size = self.size(doc)

    def discipline(self, doc):
        classifier = classify.Classify()
        result = classifier.map_discipline(doc.keywords)
        if 'Various' not in result:
            logging.debug(f"{result} keywords={doc.keywords}")
        disc = result[0]
        if 'Various' in disc:
            disc = 'Earth System Research'
        return disc

    def size(self, doc):
        number = format_value(self.find('transferSize'), one=True)
        unit = format_value(self.find('unitsOfDistribution'), one=True)
        return f"{number} {unit}"
