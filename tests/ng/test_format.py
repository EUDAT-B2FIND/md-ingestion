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
