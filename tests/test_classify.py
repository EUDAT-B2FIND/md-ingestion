from mdingestion import classify
import Levenshtein as lvs


def test_levenshtein_similarity():
    assert lvs.ratio('Social Sciences', 'Social Sciences') == 1.0
    assert round(lvs.ratio('social sciences', 'Social Sciences'), 2) == 0.87
    assert round(lvs.ratio('Social', 'Social Sciences'), 2) == 0.57
    assert round(lvs.ratio('Sciences', 'Social Sciences'), 2) == 0.7
    assert round(lvs.ratio('Computer and Information Science', 'Computer Information Science'), 2) == 0.93


def test_similarity():
    assert classify.similarity('Social Sciences', 'Social Sciences') == 1.0
    assert round(classify.similarity('social sciences', 'Social Sciences'), 2) == 1.0
    assert round(classify.similarity('Social', 'Social Sciences'), 2) == 0.57
    assert round(classify.similarity('Sciences', 'Social Sciences'), 2) == 0.7
    assert round(classify.similarity(
        'Computer and Information Science', 'Computer Information Science'), 2) == 0.93
    assert round(classify.similarity(
        'rock mechanics', 'Mechanics'), 2) == 0.78
    assert round(classify.similarity(
        'mechanics', 'Mechanics'), 2) == 1.0
    assert round(classify.similarity(
        'Earth and Environmental Sciences', 'Earth System Research'), 2) == 0.45
    assert round(classify.similarity(
        'Earth and Environmental Sciences', 'Environmental Research'), 2) == 0.63


def test_load_list():
    classifier = classify.Classify()
    assert 'Humanities' in classifier.discipines


def test_tokenize():
    assert classify.tokenize('Social Sciences') == \
        ['Social', 'Sciences', 'Social Sciences']
    assert classify.tokenize(['Engineering', 'Scientific satellites', 'Aerospace telemetry']) == \
        ['Engineering',
         'Scientific', 'Satellites', 'Scientific Satellites',
         'Aerospace', 'Telemetry', 'Aerospace Telemetry']
    assert classify.tokenize(['Humanities', 'Engineering']) == \
        ['Humanities', 'Engineering']


def test_map_discipline():
    classifier = classify.Classify()
    assert classifier.map_discipline('Humanities') == \
        ('Humanities', ['1', 'Humanities', 'Humanities'])
    assert classifier.map_discipline('Engineering') == \
        ('Engineering', ['5.5.6', 'Construction Engineering and Architecture', 'Engineering'])
    assert classifier.map_discipline(['Engineering', 'Scientific satellites', 'Aerospace telemetry']) == \
        ('Engineering', ['5.5.6', 'Construction Engineering and Architecture', 'Engineering'])
    assert classifier.map_discipline(['Antarctica', 'Sampling drilling ice']) == \
        ('Other', [])
    assert classifier.map_discipline(['Humanities', 'Engineering']) == \
        ('Humanities;Engineering', ['1', 'Humanities', 'Humanities'])
    assert classifier.map_discipline('Earth and Environmental Sciences') == \
        ('Earth and Environmental Science', ['4.4.7.02', 'Environmental Research', 'Earth and Environmental Science'])


def test_map_discipline_darus():
    classifier = classify.Classify()
    assert classifier.map_discipline([
        'Computer and Information Science',
        'Earth and Environmental Sciences',
        'Engineering',
        'Carrara marble',
        'micro X ray computed tomography',
        'cracks', 'fractures', 'rock mechanics']) == \
        ('Engineering;Mechanics;Earth and Environmental Science',
            ['5.5.6', 'Construction Engineering and Architecture', 'Engineering'])
    assert classifier.map_discipline([
        'Agricultural Sciences',
        'Computer and Information Science',
        'Earth and Environmental Sciences']) == \
        ('Agricultural Sciences;Earth and Environmental Science',
         ['3.3.1.11', 'Agriculture, Forestry, Horticulture, Aquaculture', 'Agricultural Sciences'])
    assert classifier.map_discipline(
        ['Arts and Humanities',
         'Computer and Information Science',
         'JSON', 'schema', 'process metadata']) == \
        ('Humanities', ['1', 'Humanities', 'Humanities'])
    assert classifier.map_discipline(['Medicine Health and Life Sciences', 'protein domain']) == \
        ('Medicine', ['3.2.2', 'Medicine', 'Medicine'])
    assert classifier.map_discipline(['Chemistry']) == \
        ('Chemistry', ['4.1', 'Natural Sciences', 'Chemistry'])
    assert classifier.map_discipline('Social Sciences') == \
        ('Social Sciences', ['2.3', 'Social and Behavioural Sciences', 'Social Sciences'])
    assert classifier.map_discipline(['Social Sciences']) == \
        ('Social Sciences', ['2.3', 'Social and Behavioural Sciences', 'Social Sciences'])
    assert classifier.map_discipline('other') == \
        ('Other', [])
