from ..exceptions import HarvesterNotSupported

from .herbadrop import HerbadropHarvester
from .oai import OAIHarvester
from .csw import CSWHarvester

from ..service_types import ServiceType


def harvester(community,
              url,
              service_type,
              schema,
              oai_metadata_prefix,
              oai_set,
              fromdate,
              clean,
              limit,
              outdir,
              verify):
    if service_type == ServiceType.HERBADROP:
        harvester = HerbadropHarvester(
            community=community,
            url=url,
            fromdate=fromdate,
            clean=clean,
            limit=limit,
            outdir=outdir,
            verify=verify)
    elif service_type == ServiceType.OAI:
        harvester = OAIHarvester(community, url, oai_metadata_prefix, oai_set, fromdate, clean, limit, outdir, verify)
    elif service_type == ServiceType.CSW:
        harvester = CSWHarvester(community, url, schema, fromdate, clean, limit, outdir, verify)
    else:
        raise HarvesterNotSupported()
    return harvester
