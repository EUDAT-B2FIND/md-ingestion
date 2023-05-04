from .base import Repository
from ..service_types import SchemaType, ServiceType


class Hpceuropa3Dublincore(Repository):
    IDENTIFIER = 'hpc'
    TITLE = 'HPC-Europa3'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = '8d4f0e20-8af2-4111-a816-f29a71b4439c '  # HPC-Europa3 Set from CSC
    PRODUCTIVE = False
    DATE = '2023-05-04'
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
    CRON_DAILY = False
    LOGO = "http://www.hpc-europa.eu/sites/default/files/logo_persito_trasp-02_low.png"
    LINK = "http://www.hpc-europa.eu/"
    DESCRIPTION = """HPC-Europa3 Transnational Access programme offers: access to world-class HPC systems, to academic and industrial researchers, scientific collaboration with host researchers in any field,  technical support by the HPC centres and travel and living expenses reimbursed. HPC-Europa3 Transnational Access programme goals are: more than 1098 visitors supported in the next four years, about 100 million CPU hours offered. The HPC-Europa3 programme has ended in April 2022."""

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'EUDAT B2SHARE'
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        doc.discipline = self.discipline(doc, 'Computer Science')
        doc.resource_type = 'Dataset'
        doc.contact = 'staff@hpc-europa.org'
