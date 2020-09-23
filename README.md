# Readme Next Generation

## Installation

Create conda env:
```
$ conda env create -f environment.yml
$ conda activate b2f
```

Install mdingestion:
```
$ python setup.py develop
```

## Example with Darus Community

List available communities:
```
$ b2f list
```

Harvest:
```
$ b2f harvest -c darus
```

Files are written to `oaidata/darus/raw`.

Map:
```
$ b2f map -c darus
```

Files are written to `oaidata/darus/ckan`.

Check the validation result:
```
$ less oaidata/darus/ckan/summary.json
```

Upload:
```
$ b2f upload -c darus -i CKAN_HOST --auth AUTH_KEY
```

## Run tests

Install pytest:
```
$ conda install pytest
```

Run all tests:
```
$ pytest -v tests/
```

Run single test:
```
$ pytest -v tests/ng/community/test_darus.py
```
