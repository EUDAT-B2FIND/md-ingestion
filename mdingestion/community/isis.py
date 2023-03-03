from .base import Repository
from ..service_types import SchemaType, ServiceType


class ISISDatacite(Repository):
    IDENTIFIER = 'isis'
#   URL = 'https://icat-dev.isis.stfc.ac.uk/oaipmh/request'
    URL = 'https://icatisis.esc.rl.ac.uk/oaipmh/request'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = False
    REPOSITORY_ID = 're3data:r3d100014115'
    REPOSITORY_NAME = 'ISIS DataGateway'
#    DATE = '2023-01.17'
#    LOGO = "http://b2find.dkrz.de/images/communities/darus_logo.png"
#    DESCRIPTION = """ some description here """

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Photon- and Neutron Geosciences')
        doc.creator = self.creator(doc)
    
    def creator(self, doc):
        result = []
        if doc.creator:
            for name in doc.creator:
                if name != ':null':
                    result.append(name)
        return result