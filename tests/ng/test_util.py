from mdingestion.ng import util
from mdingestion.ng.format import format_datetime


def test_remove_duplicates_from_list():
    assert util.remove_duplicates_from_list([1, 2, 2, 3]) == [1, 2, 3]


def test_utc2seconds():
    assert 63734079600 == util.utc2seconds(format_datetime('2020-08-27'))
    assert 59926651199 == util.utc2seconds(format_datetime('1900-01-01'))
    assert 58664347199 == util.utc2seconds(format_datetime('1860-01-01'))
    assert 50459543999 == util.utc2seconds(format_datetime('1600-01-01'))
    assert 25213982399 == util.utc2seconds(format_datetime('0800-01-01'))
    assert 43199 == util.utc2seconds(format_datetime('0001-01-01'))


def test_is_valid_url():
    assert util.is_valid_url('http://localhost:5000/csw') is True
    assert util.is_valid_url('http://localhost:5000/csw &bla') is False
