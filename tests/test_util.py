import pytest

from mdingestion import util
from mdingestion.format import format_datetime


def test_remove_duplicates_from_list():
    assert util.remove_duplicates_from_list([1, 2, 2, 3]) == [1, 2, 3]


# @pytest.mark.xfail(reason='fails on travis')
def test_utc2seconds():
    assert 63734079600 == util.utc2seconds(format_datetime('2020-08-27'))
    assert 63715939200 == util.utc2seconds(format_datetime('2020-01-30'))
    assert 63713433600 == util.utc2seconds(format_datetime('2020-01-01'))
    assert 62135600400 == util.utc2seconds(format_datetime("1970-01-01T01:00:00Z"))
    assert 62103978000 == util.utc2seconds(format_datetime("1968-12-31T01:00:00Z"))
    assert 59926651199 == util.utc2seconds(format_datetime("1900-01-01T11:59:59Z"))
    assert 59926651199 == util.utc2seconds(format_datetime('1900-01-01'))
    assert 59926651199 == util.utc2seconds(format_datetime('1900-01-01T00:00:00Z'))
    assert 59895115199 == util.utc2seconds(format_datetime('1899-01-01'))
    assert 58664347199 == util.utc2seconds(format_datetime('1860-01-01'))
    assert 58553841599 == util.utc2seconds(format_datetime("1856-07-01T11:59:59Z"))
    assert 52400260799 == util.utc2seconds(format_datetime("1661-07-01T11:59:59Z"))
    assert 50459543999 == util.utc2seconds(format_datetime('1600-01-01'))
    assert 25213982399 == util.utc2seconds(format_datetime('0800-01-01'))
    assert 43199 == util.utc2seconds(format_datetime('0001-01-01'))


def test_is_valid_url():
    assert util.is_valid_url('http://localhost:5000/csw') is True
    assert util.is_valid_url('http://localhost:5000/csw &bla') is False
