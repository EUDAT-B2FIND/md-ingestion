# Readme Next Generation

## Installation

Create conda env:
```
$ conda create -n b2f python=3.6 pytest
$ conda activate b2f
```

Install mdingestion:
```
$ python setup.py develop
```

## Example

Harvest herbadrop:
```
$ mgr -l etc/harvest_list_cm harvest -c herbadrop
```

Files are written to `oaidata/herbadrop-json/SET_1/hjson`.

Map herbadrop:
```
$ mgr -l etc/harvest_list_cm map -c herbadrop --mdprefix hjson
```

Files are written to `oaidata/herbadrop-json/SET_1/json`.

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
$ pytest -v tests/ng/community/test_herbadrop.py
```
