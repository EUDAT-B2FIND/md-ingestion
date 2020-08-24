from ..reader import ISO19139Reader
from ..sniffer import CSWSniffer


class DeimsISO19139(ISO19139Reader):
    NAME = 'deims-iso19139'
    SNIFFER = CSWSniffer

    def update(self, doc):
        doc.contributor = 'DEIMS-SDR Site and Dataset registry deims.org'
        doc.discipline = 'Environmental Monitoring'
        #doc.doi = self.find_doi('distributionInfo.URL')
        #doc.pid = self.find_pid('distributionInfo.URL')
        #doc.source = self.source(doc)
        #doc.related_identifier = self.related_identifier(doc)

    #def source(self, doc):
        # TODO: find with pattern
        #urls = [url for url in self.find('distributionInfo.URL') if 'deims.org/' in url]
        #return urls

    #def related_identifier(self, doc):
        #urls = []
        #for url in self.find('distributionInfo.URL'):
            #if 'deims.org/' in url:
            #    continue
            #if 'doi.org/' in url:
            #    continue
            #if 'hdl.handle.net/' in url:
            #    continue
            #urls.append(url)
        #return urls
