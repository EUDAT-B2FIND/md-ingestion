from .base import Repository
import pandas as pd
import os


CFG_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', '..', 'etc', 'Community')
FNAME = os.path.join(CFG_DIR, 'INRAE_MappingSubject__Discipline.csv')
DF = pd.read_csv(open(FNAME))


class BaseRDG(Repository):
    GROUP = 'recherchedatagouv'
    GROUP_TITLE = 'Recherche Data Gouv'
    PRODUCTIVE = True
    DATE = '2023-03-31'
    DESCRIPTION = """The Research Data Gouv repository allows the French scientific community to publish public research data. It is based on the open source research data repository software "Dataverse". This multidisciplinary repository is a sovereign publishing solution for sharing and opening up data for communities which are yet to set up their own recognised thematic repository."""
    LOGO = 'https://nextcloud.inrae.fr/s/3JsYRnHLc8P8p3Q/download/Logo%20RDG.png'
    LINK = 'https://recherche.data.gouv.fr'
    REPOSITORY_ID = 're3data:r3d100013931'
    REPOSITORY_NAME = 'Recherche Data Gouv France'

    # TODO: adding all repos that are part of recherche-data-gouv
    # INRAE
    # École des Ponts-ParisTech
    # Université de Strasbourg
    # Data Repository Grenoble Alpes
    # Université de Lille
    # Arts et Métiers Institute of Technology
    # Inria
    # Institut Pasteur
    # Université Gustave Eiffel
    # Université de Montpellier
    # Sorbonne Université

    def update(self, doc):
        doc.discipline = self.discipline_mapping(doc, doc.keywords)

    def discipline_mapping(self, doc, subjects):
        values = []
        _subjects = subjects.copy()
        if "Health and Life Sciences" in _subjects:
            if "Medicine" in _subjects:
                _subjects.remove("Medicine")
        for subject in _subjects:
            result = DF.loc[DF.Subject == subject]
            result_disciplines = list(result.Discipline.to_dict().values())
            if result_disciplines:
                values.extend(result_disciplines[0].split(';'))
            else:
                values.extend(self.discipline(doc, [subject]))
        return values
