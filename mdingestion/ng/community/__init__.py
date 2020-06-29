from .envidat import EnvidatDatacite, EnvidatISO19139
from .ess import ESSDatacite
from .darus import DarusDatacite
from .slks import SLKSDublinCore
from .herbadrop import Herbadrop
from .radar import RadarDublinCore
from .egidatahub import EGIDatahubDublinCore
from .deims import DeimsISO19139, DeimsDublicCore

import logging

READER = {
    'envidat-datacite': EnvidatDatacite,
    'envidat-oai_datacite': EnvidatDatacite,
    'envidat-iso19139': EnvidatISO19139,
    'ess-oai_datacite': ESSDatacite,
    'darus-datacite': DarusDatacite,
    'darus-oai_datacite': DarusDatacite,
    'slks-dc': SLKSDublinCore,
    'herbadrop-hjson': Herbadrop,
    'radar-oai_dc': RadarDublinCore,
    'egidatahub-oai_dc': EGIDatahubDublinCore,
    'deims-iso19139': DeimsISO19139,
    'deims-dc': DeimsDublicCore,
}


def reader(community, mdprefix):
    logging.debug(f'community={community}, mdprefix={mdprefix}')
    return READER.get(f'{community}-{mdprefix}')
