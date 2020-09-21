from ..exceptions import HarvesterNotSupported

from .herbadrop import HerbadropHarvester
from .oai import OAIHarvester
from .csw import CSWHarvester


from enum import Enum


class ServiceType(Enum):
    OAI = 0
    CSW = 1
    HERBADROP = 100


def harvester(community, url, service_type, fromdate, limit, outdir, verify):
    if service_type == ServiceType.HERBADROP:
        harvester = HerbadropHarvester(community, url, fromdate, limit, outdir, verify)
    elif service_type == ServiceType.OAI:
        harvester = OAIHarvester(community, url, fromdate, limit, outdir, verify)
    elif service_type == ServiceType.CSW:
        harvester = CSWHarvester(community, url, fromdate, limit, outdir, verify)
    else:
        raise HarvesterNotSupported()
    return harvester
