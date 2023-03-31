from .base import Repository


class BaseRDG(Repository):
    GROUP = 'rdg'
    GROUP_TITLE = 'Recherche Data Gouv'
    PRODUCTIVE = True
    DATE = '2023-03-31'
    DESCRIPTION = 'The Research Data Gouv repository allows the French scientific community to publish public research data. It is based on the open source research data repository software "Dataverse". This multidisciplinary repository is a sovereign publishing solution for sharing and opening up data for communities which are yet to set up their own recognised thematic repository.'
    LOGO = 'https://nextcloud.inrae.fr/s/3JsYRnHLc8P8p3Q/download/Logo%20RDG.png'
    LINK = 'https://recherche.data.gouv.fr'

    # TODO: adding all repos that are part of rdg
    # INRAE
    # 