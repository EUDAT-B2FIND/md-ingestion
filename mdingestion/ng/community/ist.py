from ..reader import DublinCoreReader
from ..sniffer import OAISniffer


class ISTDublinCore(DublinCoreReader):
    NAME = 'ist-oai_dc'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.doi = self.doi(doc)
        doc.source = self.source(doc)
        #doc.related_identifier = self.related_identifier(doc)
        #doc.discipline = ''
        doc.contact = 'office@ist.ac.at'

    def doi(self, doc):
        dois = [id for id in self.find('metadata.source') if id.startswith('https://doi.org/')]
        if dois:
            url = f"https://doi.org/{dois[0][4:]}"
        else:
            url = ''
        return url

    def source(self, doc):
        urls = [url for url in self.find('metadata.identifier') if 'app.ist.ac.at/record' in url]
        return urls

    #def related_identifier(self, doc):
        #urls = [url for url in self.find('metadata.identifier') if 'app.ist.ac.at/download' in url]
        #return urls
