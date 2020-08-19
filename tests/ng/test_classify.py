from mdingestion.ng import classify


def test_load_list():
    classifier = classify.Classify()
    assert '1#Humanities#Humanities' in classifier.disctab


def test_map_discipline():
    classifier = classify.Classify()
    assert classifier.map_discipline('Humanities') == ('Humanities', ['1', 'Humanities', 'Humanities'])
    assert classifier.map_discipline('Engineering') == \
        ('Engineering', ['5.5.6', 'Construction Engineering and Architecture', 'Engineering'])
    assert classifier.map_discipline(['Engineering', 'Scientific satellites', 'Aerospace telemetry']) == \
        ('Engineering', ['4.1', 'Natural Sciences', 'Chemistry'])
    assert classifier.map_discipline(['Antarctica', 'Sampling drilling ice']) == ('Various', [])
