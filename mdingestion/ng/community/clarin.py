from ..reader import DublinCoreReader
from ..sniffer import OAISniffer


class ClarinDublinCore(DublinCoreReader):
    """
    new folderstructure:
    community/schema/uuidFORurl/set/raw
    community/schema/uuidFORurl/set/ckan
    """
    NAME = 'clarin-oai_dc'
    SNIFFER = OAISniffer
    URL = 'http://clarin.eurac.edu/repository/oai/request'


    def update(self, doc):
        #doc.related_identifier = self.find('references')
        doc.discipline = 'Linguistics'
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = 'CLARIN'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('CLARIN')
        return keywords
