from mdingestion.ng import format


def test_format_string():
    assert 'hello' == format.format_string('hello')
    assert 'alice' == format.format_string(' alice ')
    assert '' == format.format_string(None)
    assert '123.4' == format.format_string(123.4)


def test_format_date():
    assert '2020-05-19' == format.format_date('2020-05-19')


def test_format_date_year():
    assert '2020' == format.format_date_year('2020-05-20')


def test_format_value():
    assert ['hello'] == format.format_value('hello')
    assert ['alice'] == format.format_value(' alice ')
    assert [] == format.format_value(None)
    assert ['123.4'] == format.format_value(123.4)
    assert ['2020-05-19'] == format.format_value('2020-05-19', type='date')
    assert ['2020'] == format.format_value('2020-05-19', type='date_year')
