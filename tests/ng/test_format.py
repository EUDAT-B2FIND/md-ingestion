from mdingestion.ng import format


def test_format_string():
    assert 'hello' == format.format_string('hello')
    assert 'alice' == format.format_string(' alice ')
    assert '' == format.format_string(None)
    assert '123.4' == format.format_string(123.4)


def test_format_string_words():
    assert 'one two three' == format.format_string_words('one. two , three')
    assert '' == format.format_string_words(None)
    assert 'what' == format.format_string_words('.  what ;-')


def test_format_string_word():
    assert 'one' == format.format_string_word('one;  two , three')
    assert '' == format.format_string_word(None)


def test_format_datetime():
    assert '2020-05-19T00:00:00Z' == format.format_datetime('2020-05-19')


def test_format_date():
    assert '2020-05-19' == format.format_date('2020-05-19')


def test_format_date_year():
    assert '2020' == format.format_date_year('2020-05-20')


def test_format_url():
    assert 'http://localhost/alice/in/wonderland' == format.format_url('http://localhost/alice/in/wonderland')
    assert 'https://nbn-resolving.org/urn:nbn:de:hbz:5-59155' == format.format_url('urn:nbn:de:hbz:5-59155')
    assert 'https://doi.org/10.22000/152' == format.format_url('doi:10.22000/152')
    assert 'https://doi.org/10.22000/152' == format.format_url('10.22000/152')
    assert 'http://alice.org/in/wonderland' == format.format_url('\nhttp://alice.org/in/wonderland\n')


def test_format_url_special():
    assert '' == format.format_url(
        'https://map.geo.admin.ch/?topic=swisstopo&amp;X=127951&amp;Y=613350&amp;zoom=11&amp;lang=de&amp;bgLay er=ch.swisstopo.pixelkarte-farbe&amp;crosshair=marker')  # noqa


def test_format_value():
    assert ['hello'] == format.format_value('hello')
    assert ['alice'] == format.format_value(' alice ')
    assert [] == format.format_value(None)
    assert ['123.4'] == format.format_value(123.4)
    assert ['2020-05-19'] == format.format_value('2020-05-19', type='date')
    assert ['2020'] == format.format_value('2020-05-19', type='date_year')
    assert 'https://alice.org/in/wonderland' == format.format_value(
        'https://alice.org/in/wonderland', type='url', one=True)
