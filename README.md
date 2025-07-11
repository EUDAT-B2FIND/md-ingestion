# 🚨 Repository Moved

> ⚠️ **Important:** This repository moved to EUDAT GitLab.  
> 👉 New location: https://gitlab.eudat.eu/b2find/md-ingestion.git

# B2FIND Metadata Integration Tool

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Description

Python library to harvest, map and upload research community specific metadata into B2FIND CKAN portal. Currently, supported generic metadata schemas are DublinCore, DataCite, DDI2.5, ISO 19139 and EUDAT Core.

## Preparation

Install Conda, see: https://github.com/conda-forge/miniforge.
```
$ wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh
$ bash Miniforge3-Linux-x86_64.sh
$ source .bashrc
```

## Installation

Clone from git
```
git clone https://github.com/EUDAT-B2FIND/md-ingestion.git
cd md-ingestion
```

Create conda env:
```
$ conda env create -f environment.yml
$ conda activate b2f
```

Install mdingestion:
```
$ pip install -e .
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
$ less summary/darus/2025-06-16_darus_summary.json
```

Upload:
```
$ b2f upload -c darus -i CKAN_HOST --auth CKAN_AUTH_KEY
```

Combine
```
$ b2f combine -c darus --clean -i CKAN_HOST --auth CKAN_AUTH_KEY
```

Purge
```
$ b2f purge -c darus -i CKAN_HOST --auth CKAN_AUTH_KEY
```

## Run tests

Install pytest:
```
$ conda install pytest
```

Run all tests:
```
$ pytest tests/
```

Run single test:
```
$ pytest tests/community/test_darus.py
```
## Update b2f list

Update PRODUCTIVE=True in Community mapfiles
```
b2f list -o b2flist.csv
```
## Create cronjob file
```
b2f cron --auth CKAN_AUTH_KEY -o b2f.cron
```
Copy `b2f.cron` file as sudo to `/etc/cron.d/b2f`
```
$ sudo cp b2f.cron /etc/cron.d/b2f
```
### Edit cronjob file
```
$ vim /etc/cron.d/b2f
```
