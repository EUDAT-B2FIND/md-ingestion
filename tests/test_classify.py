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
    assert classify.tokenize('Social Sciences') == [
        'sciences',
        'social',
        'social sciences'
    ]
    assert classify.tokenize(['Scientific satellites']) == [
        'satellites',
        'scientific',
        'scientific satellites'
    ]
    assert classify.tokenize(['Humanities', 'Engineering']) == [
        'engineering',
        'humanities'
    ]


def test_map_discipline():
    classifier = classify.Classify()
    assert classifier.map_discipline('Humanities') == ["Humanities"]
    assert classifier.map_discipline('Astrophysics') == ['Other']
    assert classifier.map_discipline('Astrophysics and Astronomy') == [
        'Astrophysics and Astronomy',
        'Natural Sciences',
        'Physics',
    ]
    assert classifier.map_discipline('Engineering') == [
        'Construction Engineering and Architecture',
        'Engineering',
        'Engineering Sciences',
    ]
    assert classifier.map_discipline(['Engineering', 'Scientific satellites', 'Aerospace telemetry']) == [
        'Construction Engineering and Architecture',
        'Engineering',
        'Engineering Sciences',
    ]
    assert classifier.map_discipline(['Antarctica', 'Sampling drilling ice']) == ["Other"]
    assert classifier.map_discipline('Antarctica', default="not avaiable") == ["not avaiable"]
    assert classifier.map_discipline(['Humanities', 'Engineering']) == [
        'Construction Engineering and Architecture',
        'Engineering',
        'Engineering Sciences',
        "Humanities",
    ]
    assert classifier.map_discipline('Earth and Environmental Sciences') == [
        'Earth and Environmental Science',
        'Environmental Research',
        'Geosciences',
        'Natural Sciences',
    ]
    assert classifier.map_discipline(["Medicine", 'Health and Life Sciences']) == [
        'Life Sciences',
        'Medicine'
    ]
    assert classifier.map_discipline(["Chemistry", "Life Science"]) == [
        'Chemistry',
        'Life Sciences',
        'Natural Sciences',
    ]
