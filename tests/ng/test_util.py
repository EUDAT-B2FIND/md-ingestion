from mdingestion.ng import util


def test_remove_duplicates_from_list():
    assert util.remove_duplicates_from_list([1, 2, 2, 3]) == [1, 2, 3]
