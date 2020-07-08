from ..exceptions import HarvesterNotSupported

from .herbadrop import HerbadropHarvester
from .oai import OAIHarvester
from .csw import CSWHarvester


def harvester(community, url, verb, mdprefix, mdsubset, fromdate, limit, outdir, verify):
    if verb == 'POST':  # 'herbadrop-api'
        _harvester = HerbadropHarvester(community, url, mdprefix, mdsubset, fromdate, limit, outdir, verify)
    elif verb in ['ListRecords', 'ListIdentifiers']:
        _harvester = OAIHarvester(community, url, mdprefix, mdsubset, fromdate, limit, outdir, verify)
    elif verb == 'csw':
        _harvester = CSWHarvester(community, url, mdprefix, mdsubset, fromdate, limit, outdir, verify)
    else:
        raise HarvesterNotSupported
    return _harvester
