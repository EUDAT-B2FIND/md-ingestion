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

Harvest:
```
$ b2f -l etc/harvest_list_cm harvest -c darus
```

Files are written to `oaidata/darus-oai_datacite/SET_1/xml`.

Map:
```
$ b2f -l etc/harvest_list_cm map -c darus
```

Files are written to `oaidata/darus-oai_datacite/SET_1/ckan`.

Check the validation result:
```
$ less summary.json
```

Upload:
```
$ b2f -l etc/harvest_list_cm upload -c darus -i CKAN_HOST --auth AUTH_KEY
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
