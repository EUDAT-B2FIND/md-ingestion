from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value
from mdingestion.ng import classify

import logging


class IVOADatacite(DataCiteReader):
    NAME = 'ivoa-oai_datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.source = self.find_source('alternateIdentifier', alternateIdentifierType="reference URL")
        doc.related_identifier = self.find_source('relatedIdentifier', relatedIdentifierType="bibcode")
        doc.contact = self.find('contributor', contributorType="ContactPerson")
        doc.discipline = self.discipline(doc)
        doc.contributor = self.contributor(doc)

    def contributor(self, doc):
        contributor = [name for name in doc.contributor if name not in doc.contact]
        contributor.append('International Virtual Observatory Alliance (IVOA)')
        return contributor

    def discipline(self, doc):
        classifier = classify.Classify()
        result = classifier.map_discipline(doc.keywords)
        if 'Various' not in result:
            logging.debug(f"{result} keywords={doc.keywords}")
        disc = result[0]
        if 'Various' in disc:
            disc = 'Astrophysics and Astronomy'
        return disc
