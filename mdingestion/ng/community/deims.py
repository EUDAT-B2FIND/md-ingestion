from ..reader import ISO19139Reader
from ..sniffer import CSWSniffer


class DeimsISO19139(ISO19139Reader):
    NAME = 'deims-iso19139'
    SNIFFER = CSWSniffer

    def update(self, doc):
        # print(f"{self.find('linkage')}")
        # print(f"{[url.strip() for url in self.find('linkage') if 'doi' in url]}")
        doc.doi = [url for url in self.find('linkage') if 'doi' in url]
        doc.pid = [url for url in self.find('linkage') if 'hdl' in url]
        doc.contributor = 'DEIMS-SDR Site and Dataset registry deims.org'
        doc.discipline = 'Environmental Monitoring'
        doc.metadata_access = [url for url in self.find('linkage') if 'deims.org/api/' in url]
        self.related_identifier(doc)
        self.fix_source(doc)

    def fix_source(self, doc):
        source = doc.source
        if source.startswith("https://deims.org/datasets/"):
            source = source.replace("https://deims.org/datasets/", "https://deims.org/dataset/")
            doc.source = source

    def related_identifier(self, doc):
        urls = []
        for url in self.find('linkage'):
            if doc.doi and doc.doi in url:
                continue
            if doc.pid and doc.pid in url:
                continue
            if doc.source and doc.source in url:
                continue
            if doc.metadata_access and doc.metadata_access in url:
                continue
            urls.append(url)
        doc.related_identifier = urls

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
