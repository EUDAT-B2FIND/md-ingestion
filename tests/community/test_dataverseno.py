import pytest

import os

from mdingestion.community.dataverseno import DataverseNODatacite

from tests.common import TESTDATA_DIR


def test_datacite_geometry():
    xmlfile = os.path.join(TESTDATA_DIR, 'dataverseno', 'raw', '0387599f-59ca-5d1e-8e87-ded33fe39051.xml')
    reader = DataverseNODatacite()
    doc = reader.read(xmlfile)
    assert "(19.217W, 69.583S, 19.217E, 69.583N); Tromsø Geophysical Observatory's Ramfjordmoen field station" == doc.spatial_coverage
