from ..reader import ISO19139Reader
from ..sniffer import CSWSniffer
from ..format import format_value


class DeimsISO19139(ISO19139Reader):
    NAME = 'deims-iso19139'
    SNIFFER = CSWSniffer

    def update(self, doc):
        doc.doi = self.find_doi('linkage')
        doc.pid = self.find_pid('linkage')
        doc.source = self.find('MD_Identifier')
        doc.contributor = 'DEIMS-SDR Site and Dataset registry deims.org'
        doc.discipline = 'Environmental Monitoring'
        doc.metadata_access = [url for url in self.find('linkage') if 'deims.org/api/' in url]
        doc.discipline = self.discipline(doc, 'Environmental Monitoring')
        self.fix_source(doc)

    def fix_source(self, doc):
        if not doc.source:
            source = format_value(self.find('MD_Identifier'), one=True)
            if 'xlink' in source:
                doc.source = source.split()[1]
        if doc.source.startswith("https://deims.org/datasets/"):
            doc.source = doc.source.replace("https://deims.org/datasets/", "https://deims.org/dataset/")
