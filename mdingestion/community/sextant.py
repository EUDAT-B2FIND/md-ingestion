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
    PRODUCTIVE = True
    DATE = '2023-01-30'
    LOGO = "http://b2find.dkrz.de/images/communities/sextant_logo.png"
    DESCRIPTION = "Sextant is a marine and coastal geographic data infrastructure. It is operated by Scientific Information Systems for the Sea (SISMER) of Ifremer. Sextant aims to document, disseminate and promote a catalog of data related to the marine environment. For Ifremer's laboratories and partners, as well as for national and European actors working in the marine and coastal field, Sextant provides tools that promote and facilitate the archiving, consultation and availability of these geographical data. Data published by Sextant are available free or restricted. They can be used in accordance with the terms of the Creative Commons license selected by the author of data. Sextant infrastructure and the technologies used are in line with the implementation of the INSPIRE Directive and make it possible to follow the Open Data approach. Some data set published by Sextant has a DOI which enables it to be cited in a publication in a reliable and sustainable way. The long-term preservation of data filed in Sextant is ensured by Ifremer infrastructure."
    REPOSITORY_ID = 're3data:r3d100013962'
    REPOSITORY_NAME = 'Sextant'

    def update(self, doc):
        doc.discipline = 'Oceanography/Marine Science'
        doc.keywords = self.find('MD_Keywords.keyword.PT_FreeText.textGroup')
        doc.creator = self.find('pointOfContact.CI_ResponsibleParty.individualName.CharacterString')
        self.source(doc)
        self.doi(doc)
        self.publisher(doc)
        self.discipline(doc)
        self.publication_year(doc)
        self.title(doc)
        self.rights(doc)
        doc.contact = None

    def source(self, doc):
        file_id = self.find('fileIdentifier.CharacterString')
        if file_id:
            doc.source = f'https://sextant.ifremer.fr/eng/Data/Catalogue#/metadata/{file_id[0]}'

    def doi(self, doc):
        dois = self.find('MD_DigitalTransferOptions.CI_OnlineResource.linkage.URL')
        file_id = self.find('fileIdentifier.CharacterString')
        if file_id and dois:
            fid = file_id[0]
            selected_doi = [doi for doi in dois if fid in doi]
            doc.doi = selected_doi

    def publisher(self, doc):
        if not doc.publisher:
            doc.publisher = 'IFREMER'
        else:
            publ = doc.publisher
            new_publ = []
            for pub in publ:
                if pub.lower() == 'ifremer':
                    new_publ.append('IFREMER')
                else:
                    new_publ.append(pub)
            doc.publisher = new_publ

    def publication_year(self, doc):
        selected_pubyear = None
        try:
            if self.reader.parser.doc.SV_ServiceIdentification:
                dates = self.reader.parser.doc.SV_ServiceIdentification.CI_Citation.find_all('CI_Date')
            else:
                dates = self.reader.parser.doc.MD_DataIdentification.CI_Citation.find_all('CI_Date')
            for date in dates:
                try:
                    pubyear = date.Date.text
                    codetype = date.CI_DateTypeCode['codeListValue']
                    if codetype == 'publication':
                        selected_pubyear = pubyear
                    elif codetype == 'creation' and not selected_pubyear:
                        selected_pubyear = pubyear
                    elif codetype == 'revision' and not selected_pubyear:
                        selected_pubyear = pubyear
                except Exception:
                    pass
        except Exception:
            pass
        if not selected_pubyear:
            selected_pubyear = self.find('dateStamp.DateTime')

        doc.publication_year = selected_pubyear

    def title(self, doc):
        if not doc.title:
            doc.title = self.find('SV_ServiceIdentification.CI_Citation.title.CharacterString')
        # else:
        # titles = self.find('CI_Citation.title.PT_FreeText')
        # if titles:
        # doc.title = titles

    def rights(self, doc):
        rights_list = self.find('MD_LegalConstraints.CharacterString')
        rights_list.extend(self.find('MD_LegalConstraints.useLimitation.LocalisedCharacterString'))
        doc.rights = rights_list
