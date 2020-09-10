from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value


class IVOADatacite(DataCiteReader):
    NAME = 'ivoa-oai_datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.source = self.find_source('alternateIdentifier', alternateIdentifierType="reference URL")
        doc.related_identifier = self.find_source('relatedIdentifier', relatedIdentifierType="bibcode")
        doc.contact = self.find('contributor', contributorType="ContactPerson")
        doc.discipline = self.discipline(doc, 'Astrophysics and Astronomy')
        doc.contributor = self.contributor(doc)

    def contributor(self, doc):
        contributor = [name for name in doc.contributor if name not in doc.contact]
        contributor.append('International Virtual Observatory Alliance (IVOA)')
        return contributor
