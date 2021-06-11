from .base import Community
from ..service_types import SchemaType, ServiceType


class Deims(Community):
    NAME = 'bluecloud'
    IDENTIFIER = NAME
    URL = 'http://seadatanet.geodab.eu/gs-service/services/essi/view/seadatanet-open/csw'
    SCHEMA = SchemaType.ISO19139
    SERVICE_TYPE = ServiceType.CSW
    PRODUCTIVE = False

    # def update(self, doc):
        # doc.source = self.find('transferOptions.linkage')
        # doc.description = self.description()

    # def description(self):
        # text = []
        # text.extend(self.find('abstract'))
        # text.extend(self.find('contentInfo.attributeDescription.RecordType'))
        # return text
