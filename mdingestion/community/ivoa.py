from .base import Repository
from ..service_types import SchemaType, ServiceType


# TODO: filter out records with <resourceType>='Other' and <resourceType>='Text'; check validation for 'Contributor'
class IvoaEudatcore(Repository):
    IDENTIFIER = 'ivoa'
    URL = 'http://dc.g-vo.org/rr/q/pmh/pubreg.xml'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_b2find'
    OAI_SET = None
    PRODUCTIVE = True
    REPOSITORY_ID = ''
    REPOSITORY_NAME = 'IVOA'

    def update(self, doc):
        self.filter(doc)
        doc.source = self.find_source('identifier', identifierType="URL")
        doc.related_identifier = self.find('relatedIdentifier', relatedIdentifierType="bibcode")
        doc.related_identifier = self.find('relatedIdentifier', relatedIdentifierType="URL")
        doc.discipline = self.discipline(doc, 'Astrophysics and Astronomy')
        doc.instrument = self.instrument(doc)

    def filter(self, doc):
        restypes = doc.resource_type
        if len(restypes) == 1:
            if 'Other' in restypes:
                doc.accept = None
            elif 'Text' in restypes:
                doc.accept = None

    def instrument(self, doc):
        result = []
        insts = self.reader.parser.doc.find_all('instrument')
        for inst in insts:
            inst_name = inst.text
            inst_type = inst.get('instrumentIdentifierType')
            inst_id = inst.get('instrumentIdentifier')
            if inst_type == 'DOI':
                result.append(f'{inst_name}, https://doi.org/{inst_id}')
            elif inst_type == 'Handle':
                result.append(f'{inst_name}, https://hdl.handle.net/{inst_id}')
            else:
                result.append(inst_name)
        return result
