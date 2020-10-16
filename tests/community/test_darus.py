import os

from mdingestion.community.darus import DarusDatacite

from tests.common import TESTDATA_DIR


def test_datacite():
    xmlfile = os.path.join(TESTDATA_DIR, 'darus', 'raw', '02baec53-8e79-5611-981e-11df59b824e4.xml')
    reader = DarusDatacite()
    doc = reader.read(xmlfile)
    assert 'Deep enzymology data' in doc.title[0]
    assert 'https://doi.org/10.18419/darus-629' == doc.doi
    assert 'https://darus.uni-stuttgart.de/oai?verb=GetRecord&metadataPrefix=oai_datacite&identifier=doi:10.18419/darus-629' == doc.metadata_access  # noqa
    assert 'Medicine' in doc.keywords
    assert 'Health and Life Sciences' in doc.keywords
    assert 'Medicine' == doc.discipline
    assert 'Jeltsch, Albert (Universität Stuttgart)' in doc.contact
    assert 'application/pdf' in doc.format
    assert len(doc.format) == 2
    assert doc.open_access is True
    assert '2020-01-30T00:00:00Z' in doc.temporal_coverage_begin_date
