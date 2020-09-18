from ..reader import DublinCoreReader
from ..sniffer import OAISniffer


class SLKSDublinCore(DublinCoreReader):
    NAME = 'slks-oai_dc'
    SNIFFER = OAISniffer

    def update(self, doc):
        # doc.open_access = True
        record_id = self.find('header.identifier')[0]
        # print(record_id)
        record_id = record_id.split('/')
        # print(record_id)
        record_id = record_id[-2]
        # print(record_id)
        oai_id = f'urn:repox.www.kulturarv.dkSites:http://www.kulturarv.dk/fundogfortidsminder/site/{record_id}'
        # print(oai_id)
        doc.metadata_access = f'http://www.kulturarv.dk/ffrepox/OAIHandler?verb=GetRecord&metadataPrefix=ff&identifier={oai_id}'
        doc.discipline = 'Archaeology'
        doc.publication_year = self.find('header.datestamp')
        doc.description = 'This record describes ancient sites and monuments as well as archaeological excavations undertaken by Danish museums.'
        doc.publisher = 'Slots- og Kulturstyrelsen'
        doc.rights = 'For scientific use'
        doc.contact = 'post@slks.dk'
        # doc.language = 'Danish'
        keywords = doc.keywords
        keywords.append('EOSC Nordic')
        keywords.append('Viking Age')
