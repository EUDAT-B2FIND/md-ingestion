from ..exceptions import HarvesterNotSupported

from .herbadrop import HerbadropHarvester
from .oai import OAIHarvester
from .csw import CSWHarvester
from .arcgis import ArcGISHarvester
from .bc import BlueCloudHarvester
from .dataverse import DataverseHarvester

from ..service_types import ServiceType


def harvester(community,
              url,
              service_type,
              schema,
              oai_metadata_prefix,
              oai_set,
              filter,
              fromdate,
              clean,
              limit,
              outdir,
              verify,
              username=None,
              password=None):
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
        harvester = OAIHarvester(community, url, oai_metadata_prefix, oai_set, fromdate, clean, limit, outdir, verify,
                                 username, password)
    elif service_type == ServiceType.CSW:
        harvester = CSWHarvester(community, url, schema, fromdate, clean, limit, outdir, verify)
    elif service_type == ServiceType.ArcGIS:
        harvester = ArcGISHarvester(
            community=community,
            url=url,
            filter=filter,
            fromdate=fromdate,
            clean=clean,
            limit=limit,
            outdir=outdir,
            verify=verify)
    elif service_type == ServiceType.Dataverse:
        harvester = DataverseHarvester(
            community=community,
            url=url,
            filter=filter,
            fromdate=fromdate,
            clean=clean,
            limit=limit,
            outdir=outdir,
            verify=verify)
    elif service_type == ServiceType.BC:
        harvester = BlueCloudHarvester(
            community=community,
            url=url,
            filter=filter,
            fromdate=fromdate,
            clean=clean,
            limit=limit,
            outdir=outdir,
            verify=verify)
    else:
        raise HarvesterNotSupported()
    return harvester
