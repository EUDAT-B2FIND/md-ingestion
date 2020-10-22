from .base import Community
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class BaseDara(Community):
    NAME = 'dara'
    URL = 'https://www.da-ra.de/oaip/oai'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'

    def update(self, doc):
        pass


class DaraGESIS(BaseDara):
    IDENTIFIER = 'dara_gesis'
    OAI_SET = '1'  # GESIS Data Archive, 7783 records

    def update(self, doc):
        doc.discipline = 'Social Sciences'
        doc.rights = self.find('rights', attrs={'xml:lang': 'en'})
        doc.places = format_value(self.find('coverage'))


class DaraRKI(BaseDara):
    IDENTIFIER = 'dara_rki'
    OAI_SET = '10'  # RKI Robert Koch-Institut, 15 records

    def update(self, doc):
        doc.discipline = 'Public Health'


class DaraIHI(BaseDara):
    IDENTIFIER = 'dara_ihi'
    OAI_SET = '105'  # Ifakara Health Institute, 20 records


class DaraDIPF(BaseDara):
    IDENTIFIER = 'dara_dipf'
    OAI_SET = '11'  # DIPF Leibniz Institute for Research and Information in Education, 901 records


class DaraBIFIE(BaseDara):
    IDENTIFIER = 'dara_bifie'
    OAI_SET = '112'  # BIFIE (Federal Institute for Education Research, Austria), 138 records


class DaraZfKD(BaseDara):
    IDENTIFIER = 'dara_zfkd'
    OAI_SET = '114'  # ZfKD German Center for Cancer Registry Data at the RKI, 10 records


class DaraZBW(BaseDara):
    IDENTIFIER = 'dara_zbw'
    OAI_SET = '118'  # ZBW Journal Data Archive, 153 records


class DaraSRDA(BaseDara):
    IDENTIFIER = 'dara_srda'
    OAI_SET = '128'  # SRDA - Survey Research Data Archive Taiwan, 2680


class DaraEBDC(BaseDara):
    IDENTIFIER = 'dara_ebdc'
    OAI_SET = '13'  # LMU-ifo Economics & Business Data Center (EBDC), 131 records


class DaraIWH(BaseDara):
    IDENTIFIER = 'dara_iwh'
    OAI_SET = '130'  # IWH - The Halle Institute for Economic Research, 7 records


class DaraAHRI(BaseDara):
    IDENTIFIER = 'dara_ahri'
    OAI_SET = '132'  # AHRI Africa Health Research Institute, 82 records


class DaraRDC(BaseDara):
    IDENTIFIER = 'dara_rdc'
    OAI_SET = '136'  # RDC of the Federal Statistical Office and the statistical offices of the LÃ¤nder, 1843


class DaraDZHW(BaseDara):
    IDENTIFIER = 'dara_dzhw'
    OAI_SET = '138'  # German Centre for Higher Education Research and Science Studies (DZHW)


class DaraXHUB(BaseDara):
    IDENTIFIER = 'dara_xhub'
    OAI_SET = '139'  # xhub, 46 records


class DaraManifesto(BaseDara):
    IDENTIFIER = 'dara_manifesto'
    OAI_SET = '143'  # Manifesto Project, 25 records


class DaraDJI(BaseDara):
    IDENTIFIER = 'dara_dji'
    OAI_SET = '148'  # DJI - The German Youth Institute, 3 records


class DaraIAB(BaseDara):
    IDENTIFIER = 'dara_iab'
    OAI_SET = '151'  # The Research Data Centre (FDZ) of the German Federal Employment Agency (BA) at the Institute for Employment Research (IAB), 101 records


class DaraDeutscheBundesbank(BaseDara):
    IDENTIFIER = 'dara_deutschebundesbank'
    OAI_SET = '21'  # Deutsche Bundesbank, 76 records


class DaraLIfBi(BaseDara):
    IDENTIFIER = 'dara_lifbi'
    OAI_SET = '3'  # LIfBi Leibniz Institute for Educational Trajectories, 203 records


class DaraCSES(BaseDara):
    IDENTIFIER = 'dara_cses'
    OAI_SET = '32'  # CSES - Comparative Study of Electoral Systems, 16 records


class DaraGESISDatorium(BaseDara):
    IDENTIFIER = 'dara_gesisdatorium'
    OAI_SET = '33'  # GESIS Data Archive, 221 records


class DaraRWI(BaseDara):
    IDENTIFIER = 'dara_rwi'
    OAI_SET = '35'  # RWI Leibniz Institute for Economic Research, 215 records


class DaraSHAREERIC(BaseDara):
    IDENTIFIER = 'dara_shareeric'
    OAI_SET = '37'  # SHARE-ERIC, 80 records


class DaraICPSR(BaseDara):
    IDENTIFIER = 'dara_icpsr'
    OAI_SET = '39'  # ICPSR - Interuniversity Consortium for Political and Social Research, 39419 records


class DaraZPID(BaseDara):
    IDENTIFIER = 'dara_zpid'
    OAI_SET = '4'  # ZPID Leibniz Institute for Psychology Information, 63 records


class DaraHSRC(BaseDara):
    IDENTIFIER = 'dara_hsrc'
    OAI_SET = '46'  # HSRC - Human Science Research Council SA, 226 records


class DaraSOEP(BaseDara):
    IDENTIFIER = 'dara_soep'
    OAI_SET = '5'  # SOEP Socio-Economic Panel Study, 60 records


class DaraDZA(BaseDara):
    IDENTIFIER = 'dara_dza'
    OAI_SET = '6'  # DZA The German Centre of Gerontology, 80 records


class DaraIQB(BaseDara):
    IDENTIFIER = 'dara_iqb'
    OAI_SET = '77'  # IQB - Institute for Educational Quality Improvement, 98 records


class DaraRDCBO1(BaseDara):
    IDENTIFIER = 'dara_rdcbo1'
    OAI_SET = '8'  # Research Data Center for Business and Organizational Data at DIW (FDZ-BO), 18 records


class DaraRDCBO2(BaseDara):
    IDENTIFIER = 'dara_rdcbo2'
    OAI_SET = '170'  # Research Data Center for Business and Organizational Data at DIW (FDZ-BO), 2 records


class DaraCSDA(BaseDara):
    IDENTIFIER = 'dara_csda'
    OAI_SET = '86'  # csda - Czech Social Science Data Archive, 475 records


class DaraINDEPTH(BaseDara):
    IDENTIFIER = 'dara_indepth'
    OAI_SET = '9'  # INDEPTH Network, 152 records
