from .base import Community
from ..service_types import SchemaType, ServiceType


class SDRDublinCore(Community):
    NAME = 'sdr'
    IDENTIFIER = NAME
    URL = 'https://repository.surfsara.nl/api/oai2'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
#   PRODUCTIVE = True
#   DATE = '2022-03-20'

    def update(self, doc):
        doc.contact = self.contact(doc)
        doc.keywords = self.keywords_(doc)
        doc.publication_year = self.publication_year(doc)
        doc.discipline = self.discipline(doc)

    def contact(self, doc):
        contacts = []
#        contacts.extend(doc.contact) TODO: keep double publisher info? see dc-reader: contact = publisher
        for relation in self.find('relation'):
            if '@' in relation:
                contacts.append(relation)
        return contacts

    def keywords_(self, doc):
        keywords = []
        keywords.extend(doc.keywords)
        arrow = chr(8594)
        for relation in self.find('relation'):
            if arrow in relation:
                new_keywords = relation.split(arrow)[1:]
                lc_keywords = [k.lower() for k in new_keywords]
                keywords.extend(lc_keywords)
        return keywords

    def discipline(self, doc):
        disciplines = doc.discipline
        if 'astrophysics' in doc.keywords:
            if 'Other' in disciplines:
                disciplines = []
            disciplines.append('Astrophysics and Astronomy')
        return disciplines

    def publication_year(self,doc):
        pubyear = doc.publication_year
        embargo = 'info:eu-repo/date/embargoEnd/'
        for date in self.find('date'):
            if embargo in date:
                pubyear = date.split(embargo)[1]
                break
        return pubyear
