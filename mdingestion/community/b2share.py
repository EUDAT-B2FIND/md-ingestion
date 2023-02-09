from .base import Repository


class BaseB2Share(Repository):
    GROUP = 'b2share'
    GROUP_TITLE = 'B2SHARE'
    PRODUCTIVE = True
    DATE = '2022-07-14'
    DESCRIPTION = 'B2SHARE is a user-friendly, reliable and trustworthy way for researchers, scientific communities and citizen scientists to store, publish and share research data in a FAIR way. B2SHARE is a solution that facilitates research data storage, guarantees long-term persistence of data and allows data, results or ideas to be shared worldwide. B2SHARE supports community domains with metadata extensions, access rules and publishing workflows. EUDAT offers communities and organisations customised instances and/or access to repositories supporting large datasets.'
    LOGO = 'http://b2find.dkrz.de/images/communities/b2share_logo.png'

    # TODO: adding all repos that use b2share here for overview: EUDAT, FMI
    # EUDAT = eudat_csc, eudat_fzj
    # TOAR
