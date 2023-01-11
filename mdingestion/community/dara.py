from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class BaseDara(Repository):
    GROUP = 'dara'
    GROUP_TITLE = 'da|ra'
    PRODUCTIVE = True
    DATE = '2020-07-10'
    DESCRIPTION = 'da|ra is the registration agency for social science and economic data jointly run by GESIS and ZBW. In keeping with the ideals of good scientific practice there is a demand for open access to existing primary data so as to not only have the final research results but also be able to reconstruct the entire research process. GESIS and ZBW therefore offer a registration service for social and economic research data.'
    LOGO = ''
    URL = 'https://www.da-ra.de/oaip/oai'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'

    def update(self, doc):
        pass


class DaraGESIS(BaseDara):
    IDENTIFIER = 'gesis'
    TITLE = 'GESIS Data Archive'
    OAI_SET = '1'  # GESIS Data Archive, 7783 records

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Social Sciences')
        doc.rights = self.find('rights', attrs={'xml:lang': 'en'})
        doc.places = format_value(self.find('coverage'))


class DaraRKI(BaseDara):
    IDENTIFIER = 'rki'
    TITLE = 'RKI - Robert Koch Institut'
    OAI_SET = '10'  # RKI Robert Koch-Institut, 15 records

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Public Health, Health Services Research, Social Medicine')


class DaraIHI(BaseDara):
    IDENTIFIER = 'ihi'
    TITLE = 'IHI - Ifakara Health Institute'
    OAI_SET = '105'  # Ifakara Health Institute, 20 records

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Public Health, Health Services Research, Social Medicine')


class DaraDIPF(BaseDara):
    IDENTIFIER = 'dipf'
    TITLE = 'DIPF - Leibniz Institute for Research and Information in Education'
    OAI_SET = '11'  # DIPF Leibniz Institute for Research and Information in Education, 901 records

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Education Sciences')


class DaraBIFIE(BaseDara):
    IDENTIFIER = 'bifie'
    TITLE = 'BIFIE - Federal Institute for Education Research, Austria'
    OAI_SET = '112'  # BIFIE (Federal Institute for Education Research, Austria), 138 records

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Education Sciences')


class DaraZfKD(BaseDara):
    IDENTIFIER = 'zfkd'
    TITLE = 'ZfKD - German Center for Cancer Registry Data at the RKI'
    OAI_SET = '114'  # ZfKD German Center for Cancer Registry Data at the RKI, 10 records

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Medicine')


class DaraZBW(BaseDara):
    IDENTIFIER = 'zbw'
    TITLE = 'ZBW Journal Data Archive'
    OAI_SET = '118'  # ZBW Journal Data Archive, 153 records

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Economics')


class DaraSRDA(BaseDara):
    IDENTIFIER = 'srda'
    TITLE = 'SRDA - Survey Research Data Archive Taiwan'
    OAI_SET = '128'  # SRDA - Survey Research Data Archive Taiwan, 2680

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Empirical Social Research')


class DaraEBDC(BaseDara):
    IDENTIFIER = 'ebdc'
    TITLE = 'EBDC - Economics & Business Data Center (LMU/ifo)'
    OAI_SET = '13'  # LMU-ifo Economics & Business Data Center (EBDC), 131 records

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Economics')


class DaraIWH(BaseDara):
    IDENTIFIER = 'iwh'
    TITLE = 'IWH - The Halle Institute for Economic Research'
    OAI_SET = '130'  # IWH - The Halle Institute for Economic Research, 7 records

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Economics')


class DaraAHRI(BaseDara):
    IDENTIFIER = 'ahri'
    TITLE = 'AHRI - Africa Health Research Institute'
    OAI_SET = '132'  # AHRI Africa Health Research Institute, 82 records

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Public Health, Health Services Research, Social Medicine')


class DaraRDC(BaseDara):
    IDENTIFIER = 'rdc'
    TITLE = 'RDC - Federal Statistical Office, Germany'
    OAI_SET = '136'  # RDC of the Federal Statistical Office and the statistical offices of the LÃ¤nder, 1843 records

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Empirical Social Research')


class DaraDZHW(BaseDara):
    IDENTIFIER = 'dzhw'
    TITLE = 'DZHW - German Centre for Higher Education Research and Science Studies'
    OAI_SET = '138'  # German Centre for Higher Education Research and Science Studies (DZHW)

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Education Sciences')


# class DaraICPSR(BaseDara):
#    GROUP = 'icpsr'
#    IDENTIFIER = GROUP
#    OAI_SET = '39'  # ICPSR - Interuniversity Consortium for Political and Social Research, 39419 records

#    def update(self, doc):
#        doc.discipline = self.discipline(doc, 'Social Sciences')


# class DaraXHUB(BaseDara):
#    IDENTIFIER = 'dara_xhub'
#    OAI_SET = '139'  # xhub, 46 records
#
#    def update(self, doc):
#        doc.discipline = self.discipline(doc, 'Social Sciences')


# class DaraManifesto(BaseDara):
#    IDENTIFIER = 'dara_manifesto'
#    OAI_SET = '143'  # Manifesto Project, 25 records

#    def update(self, doc):
#        doc.discipline = self.discipline(doc, 'Political Science')


# class DaraDJI(BaseDara):
#    IDENTIFIER = 'dara_dji'
#    OAI_SET = '148'  # DJI - The German Youth Institute, 3 records#

#    def update(self, doc):
#        doc.discipline = self.discipline(doc, 'Social Sciences')


# class DaraIAB(BaseDara):
#     IDENTIFIER = 'dara_iab'
#     OAI_SET = '151'  # The Research Data Centre (FDZ) of the German Federal Employment Agency (BA) at the Institute for Employment Research (IAB), 101 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Public Administration')


# class DaraDeutscheBundesbank(BaseDara):
#     IDENTIFIER = 'dara_deutschebundesbank'
#     OAI_SET = '21'  # Deutsche Bundesbank, 76 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Economics')


# class DaraLIfBi(BaseDara):
#     IDENTIFIER = 'dara_lifbi'
#     OAI_SET = '3'  # LIfBi Leibniz Institute for Educational Trajectories, 203 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Education Sciences')


# class DaraCSES(BaseDara):
#     IDENTIFIER = 'dara_cses'
#     OAI_SET = '32'  # CSES - Comparative Study of Electoral Systems, 16 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Political Science')


# # class DaraGESISDatorium(BaseDara):
#   #  IDENTIFIER = 'dara_gesisdatorium'
#   #  OAI_SET = '33'  # GESIS Data Archive, 221 records

#   #  def update(self, doc):
#   #      doc.discipline = self.discipline(doc, 'Empirical Social Research')


# class DaraRWI(BaseDara):
#     IDENTIFIER = 'dara_rwi'
#     OAI_SET = '35'  # RWI Leibniz Institute for Economic Research, 215 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Economics')


# #class DaraSHAREERIC(BaseDara):
#  #   IDENTIFIER = 'dara_shareeric'
#  #   OAI_SET = '37'  # SHARE-ERIC, 80 records

#  #   def update(self, doc):
#  #       doc.discipline = self.discipline(doc, 'Public Health, Health Services Research, Social Medicine')


# class DaraZPID(BaseDara):
#     IDENTIFIER = 'dara_zpid'
#     OAI_SET = '4'  # ZPID Leibniz Institute for Psychology Information, 63 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Psychology')


# class DaraHSRC(BaseDara):
#     IDENTIFIER = 'dara_hsrc'
#     OAI_SET = '46'  # HSRC - Human Science Research Council SA, 226 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Social Sciences')


# class DaraSOEP(BaseDara):
#     IDENTIFIER = 'dara_soep'
#     OAI_SET = '5'  # SOEP Socio-Economic Panel Study, 60 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Empirical Social Research')


# class DaraDZA(BaseDara):
#     IDENTIFIER = 'dara_dza'
#     OAI_SET = '6'  # DZA The German Centre of Gerontology, 80 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Gerontology and Geriatric Medicine')


# class DaraIQB(BaseDara):
#     IDENTIFIER = 'dara_iqb'
#     OAI_SET = '77'  # IQB - Institute for Educational Quality Improvement, 98 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Education Sciences')


# class DaraRDCBO1(BaseDara):
#     IDENTIFIER = 'dara_rdcbo1'
#     OAI_SET = '8'  # Research Data Center for Business and Organizational Data at DIW (FDZ-BO), 18 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Business Administration')


# class DaraRDCBO2(BaseDara):
#     IDENTIFIER = 'dara_rdcbo2'
#     OAI_SET = '170'  # Research Data Center for Business and Organizational Data at DIW (FDZ-BO), 2 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Business Administration')


# class DaraCSDA(BaseDara):
#     IDENTIFIER = 'dara_csda'
#     OAI_SET = '86'  # csda - Czech Social Science Data Archive, 475 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Social Sciences')


# class DaraINDEPTH(BaseDara):
#     IDENTIFIER = 'dara_indepth'
#     OAI_SET = '9'  # INDEPTH Network, 152 records

#     def update(self, doc):
#         doc.discipline = self.discipline(doc, 'Public Health, Health Services Research, Social Medicine')
