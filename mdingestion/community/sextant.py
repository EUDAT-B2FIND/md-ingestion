from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class Sextant(Repository):
    IDENTIFIER = 'sextant'
    # URL = 'https://sextant.ifremer.fr/geonetwork/srv/fre/csw'
    URL = 'https://sextant.ifremer.fr/geonetwork/GEOCATALOGUE/fre/csw'
    SCHEMA = SchemaType.ISO19139
    # SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.CSW
    PRODUCTIVE = False

    def update(self, doc):
        doc.doi = self.find_doi('CI_OnlineResource.linkage')
        doc.pid = self.find_pid('CI_OnlineResource.linkage')
        doc.discipline = 'Oceanography/Marine Science'
        doc.keywords = self.find('MD_Keywords.keyword.PT_FreeText.textGroup')
        doc.creator = self.find('pointOfContact.CI_ResponsibleParty.individualName.CharacterString')
        # print(self.find('pointOfContact.CI_ResponsibleParty.individualName.CharacterString'))
        self.source(doc)
        self.publisher(doc)
        self.discipline(doc)
        self.publication_year(doc)
        self.title(doc)
        self.rights(doc)

    def source(self, doc):
        # doc.source = self.find('MD_Identifier') # not useful
        # if not doc.source:
        file_id = self.find('fileIdentifier.CharacterString')
        # if file_id:
        doc.source = f'https://sextant.ifremer.fr/eng/Data/Catalogue#/metadata/{file_id[0]}'

    def publisher(self, doc):
        if not doc.publisher:
            doc.publisher = 'Ifremer'
        else:
            publ = doc.publisher
            new_publ = []
            for pub in publ:
                if pub.lower() == 'ifremer':
                    new_publ.append('Ifremer')
                else:
                    new_publ.append(pub)
            doc.publisher = new_publ

    def publication_year(self, doc):
        if not doc.publication_year:
            doc.publication_year = self.find('dateStamp.DateTime')

    def title(self, doc):
        if not doc.title:
            doc.title = 'Untitled'
        else:
            titles = self.find('CI_Citation.title.PT_FreeText')
            if titles:
                doc.title = titles

    def rights(self, doc):
        rights_list = self.find('MD_LegalConstraints.CharacterString')
        rights_list.extend(self.find('MD_LegalConstraints.useLimitation.LocalisedCharacterString'))
        doc.rights = rights_list
