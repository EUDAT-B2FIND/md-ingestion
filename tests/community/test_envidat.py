import os

from mdingestion.community.envidat_iso19139 import Envidat

from tests.common import TESTDATA_DIR


def test_iso19139():
    xmlfile = os.path.join(TESTDATA_DIR, 'envidat-iso19139', 'SET_1', 'xml', '2eff7dec-8e24-5046-b690-e87840ce65ac.xml')  # noqa
    reader = Envidat()
    doc = reader.read(xmlfile)
    assert 'https://doi.org/10.16904/envidat.lwf.38' == doc.doi
    assert '' == doc.pid
    assert 'https://www.envidat.ch/dataset/envidat-lwf-38' == doc.source
    assert 'https://www.wsl.ch/en/forest/forest-development-and-monitoring/long-term-forest-ecosystem-research-lwf.html' in doc.related_identifier  # noqa
    assert 'Environmental Research' == doc.discipline[0]
    assert 'Symptoms of O3 injuries LWF' in doc.title
    assert 'Measuring air pollutants in forests' in doc.description[0]
    assert 'AMBIENT AIR QUALITY' in doc.keywords
    assert 'Marcus Schaub' in doc.creator
    assert 'marcus.schaub(at)wsl.ch' in doc.contact
    assert 'Swiss Federal Institute for Forest' in doc.publisher[0]
    # assert 'EnviDat' in doc.contributor
    assert '2019' in doc.publication_year
    assert 'Other (Specified in the description)' in doc.rights
    assert 'English' in doc.language
    assert [] == doc.resource_type
    assert 'URL' in doc.format
    assert '' == doc.version
    assert '' == doc.temporal_coverage_begin_date
    assert '' == doc.spatial_coverage
