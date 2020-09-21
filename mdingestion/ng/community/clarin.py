from ..reader import DublinCoreReader
from ..sniffer import OAISniffer


class BaseClarin(DublinCoreReader):
    """
    new folderstructure:
    community/schema/uuidFORurl/set/raw
    community/schema/uuidFORurl/set/ckan
    """
    COMMUNITY = 'clarin'
    NAME = ''
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = ''
    SNIFFER = OAISniffer
    URL = ''


    def update(self, doc):
        doc._community = self.COMMUNITY
        doc.discipline = 'Linguistics'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        if not doc.publisher:
            doc.publisher = 'CLARIN'


class ClarinOne(BaseClarin):
    """
    new folderstructure:
    community/schema/uuidFORurl/set/raw
    community/schema/uuidFORurl/set/ckan
    """
    #NAME = 'clarin-oai_dc'
    NAME = 'clarin_one'
    URL = 'http://clarin.eurac.edu/repository/oai/request'

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = 'CLARIN one'
        doc.contact = 'clarinone@something.eu'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('Clarin ONE')
        return keywords

class ClarinTwo(BaseClarin):
    """
    new folderstructure:
    community/schema/uuidFORurl/set/raw
    community/schema/uuidFORurl/set/ckan
    """
    #NAME = 'clarin-oai_dc'
    NAME = 'clarin_two'
    URL = 'http://dspace-clarin-it.ilc.cnr.it/repository/oai/request'

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = 'CLARIN two'
        doc.contact = 'clarintwo@something.eu'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('Clarin TWO')
        return keywords

class ClarinThree(BaseClarin):
    """
    new folderstructure:
    community/schema/uuidFORurl/set/raw
    community/schema/uuidFORurl/set/ckan
    """
    NAME = 'clarin-oai_dc'
    # new: 'clarin_three'
    URL = 'http://repository.clarin.dk/repository/oai/request'

    def update(self, doc):
        super().update(doc)
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = 'CLARIN three'
        doc.contact = 'clarinthree@something.eu'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('Clarin THREE')
        return keywords
