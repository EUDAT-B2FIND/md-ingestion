from ..reader import ISO19139Reader
from ..sniffer import OAISniffer
from ..format import format_value


class ENESISO19139(ISO19139Reader):
    NAME = 'enes-iso'
    SNIFFER = OAISniffer

    def update(self, doc):
        doc.doi = self.find_doi('linkage')
        doc.contact = self.find('CI_ResponsibleParty.organisationName')
        doc.contributor = 'World Data Center for Climate (WDCC)'
        doc.rights = 'For scientific use only'
        doc.discipline = self.discipline(doc, 'Earth System Research')
        doc.size = self.size(doc)

    def size(self, doc):
        number = format_value(self.find('transferSize'), one=True)
        unit = format_value(self.find('unitsOfDistribution'), one=True)
        return f"{number} {unit}"
