from .base import Repository


class BaseB2Share(Repository):
    GROUP = 'b2share'
    GROUP_TITLE = 'B2SHARE'
    PRODUCTIVE = True
    DATE = '2022-07-14'
    DESCRIPTION = 'B2SHARE is a user-friendly, reliable and trustworthy way for researchers, scientific communities and citizen scientists to store, publish and share research data in a FAIR way. B2SHARE is a solution that facilitates research data storage, guarantees long-term persistence of data and allows data, results or ideas to be shared worldwide. B2SHARE supports community domains with metadata extensions, access rules and publishing workflows. EUDAT offers communities and organisations customised instances and/or access to repositories supporting large datasets.'
    LOGO = 'http://b2find.dkrz.de/images/communities/b2share_logo.png'

    # TODO: adding all repos that use b2share here for overview:
    # EUDAT = eudat_csc, eudat_fzj with EUDAT Core
    # LIFE+Respira = life-respira with EUDAT Core (but no spatial/temporal coverage)
    # RDA = rda with EUDAT Core
    # FMI = fmi with EUDAT Core
    # BBMRI = bbmri with EUDAT Core
    # CompBioMed = compbiomed with EUDAT Core
    # InGRID = ingrid with EUDAT Core
    # OpenEBench = openebench with EUDAT Core
    # Stars4ALL = starsforall with EUDAT Core
    # HPC-Europe3 = hpc with EUDAT Core
    # KiCoS = kicos with EUDAT Core
    # DRIHM = drihm with EUDAT Core
    # EUON 0 euon with EUDAT Core
