from ..reader import DataCiteReader
from ..sniffer import OAISniffer
from ..format import format_value


class UCLDatacite(DataCiteReader):
    NAME = 'ucl-oai_datacite'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.contributor = 'Figshare'
        doc.contact = 'researchdatarepository@ucl.ac.uk'
        doc.publisher = 'University College London UCL'
        doc.language = 'eng'
