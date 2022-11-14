from .base import Community
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class Sextant(Community):
    NAME = 'sextant'
    IDENTIFIER = NAME
#    URL = 'https://sextant.ifremer.fr/geonetwork/srv/fre/csw'
    URL = 'https://sextant.ifremer.fr/geonetwork/GEOCATALOGUE/fre/csw'
    SCHEMA = SchemaType.ISO19139
    #SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.CSW
    PRODUCTIVE = False

    def update(self, doc):
        doc.doi = self.find_doi('linkage')
        doc.pid = self.find_pid('linkage')
        self.source(doc)
        self.publisher(doc)
        self.discipline(doc)


    def source(self, doc):
        doc.source = self.find('MD_Identifier')
        if not doc.source:
            file_id = self.find('fileIdentifier.CharacterString')
            if file_id:
                doc.source = f'https://sextant.ifremer.fr/geonetwork/srv/ger/catalog.search#/metadata/{file_id[0]}'

    def publisher(self, doc):
        if not doc.publisher:
            doc.publisher = 'Ifremer'

    def discipline(self, doc):
        if not doc.discipline:
            doc.discipline = 'Oceanography/Marine Science'

#        # TODO: why do we not use csw-metadataccess? It points to dc metadata though...fix it, Carsten!
#        doc.metadata_access = [url for url in self.find('linkage') if 'deims.org/api/' in url]
#        doc.discipline = self.discipline(doc, 'Environmental Monitoring')
#        self.fix_source(doc)

#    def fix_source(self, doc):
#        if not doc.source:
#            source = format_value(self.find('MD_Identifier'), one=True)
#            if 'xlink' in source:
#                doc.source = source.split()[1]
#        if doc.source.startswith("https://deims.org/datasets/"):
#            doc.source = doc.source.replace("https://deims.org/datasets/", "https://deims.org/dataset/")
