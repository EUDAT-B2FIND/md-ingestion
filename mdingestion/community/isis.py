from .panoscexpands import BasePanoscExpands
from ..service_types import SchemaType, ServiceType


class ISISDatacite(BasePanoscExpands):
    IDENTIFIER = 'isis'
    TITLE = 'ISIS'
#   URL = 'https://icat-dev.isis.stfc.ac.uk/oaipmh/request'
    URL = 'https://icatisis.esc.rl.ac.uk/oaipmh/request'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'doiInvs'
    PRODUCTIVE = True
    REPOSITORY_ID = 're3data:r3d100014115'
    REPOSITORY_NAME = 'ISIS DataGateway'
    DATE = '2023-04-27'
    LOGO = "https://users.facilities.rl.ac.uk/auth/Images/Isis-Logo.png"
    LINKS = "https://www.isis.stfc.ac.uk/Pages/home.aspx", "https://data.isis.stfc.ac.uk/datagateway"
    DESCRIPTION = """ISIS Neutron and Muon Source is a world-leading centre for research at the STFC Rutherford Appleton Laboratory. Our neutron and muon instruments give unique insights into the properties of materials on the atomic scale. You can browse, explore and visualise experimental data via the DataGateway."""

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Photon- and Neutron Geosciences')
        doc.creator = self.creator(doc)
        doc.contact = 'isisdata@stfc.ac.uk'

    def creator(self, doc):
        result = []
        if doc.creator:
            for name in doc.creator:
                if name != ':null':
                    result.append(name)
        return result
