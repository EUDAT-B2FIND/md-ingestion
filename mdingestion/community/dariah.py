from .base import Repository
from ..service_types import SchemaType, ServiceType


class Dariah(Repository):
    IDENTIFIER = 'dariah'
    URL = 'https://repository.de.dariah.eu/1.0/oaipmh/oai?'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = False
#    DATE = '2023-01-10'
#    CRON_DAILY = False
#    LOGO = "http://b2find.dkrz.de/images/communities/darus_logo.png"
    DESCRIPTION = """

    https://de.dariah.eu/web/guest/startseite

    DARIAH-DE

    DARIAH-DE (Digital Research Infrastructure for the Arts and Humanities) ist eine Initiative zur Schaffung einer digitalen Forschungsinfrastruktur für die Geistes- und Kulturwissenschaften. Zu diesem Zweck unterstützt DARIAH-DE die mit digitalen Methoden und Verfahren arbeitende Forschung in den Geistes- und Kulturwissenschaften mit einer Forschungsinfrastruktur aus vier Säulen: Lehre, Forschung, Forschungsdaten und technische Komponenten. Als Partner in DARIAH-EU trägt DARIAH-DE ferner dazu bei, europaweit state-of-the-art Aktivitäten der Digitalen Geisteswissenschaften zu bündeln und zu vernetzen.
    """

#    def update(self, doc):

