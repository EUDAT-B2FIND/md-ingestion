import pytest

from mdingestion import util


def test_remove_duplicates_from_list():
    assert util.remove_duplicates_from_list([1, 2, 2, 3]) == [1, 2, 3]


def test_is_valid_url():
    assert util.is_valid_url('http://localhost:5000/csw') is True
    assert util.is_valid_url('http://localhost:5000/csw &bla') is False
