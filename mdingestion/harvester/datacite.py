import requests
import json
import logging
from .base import Harvester


class DataCiteHarvester(Harvester):
    def __init__(self, repo, url, filter, fromdate, clean, limit, outdir, verify):
        super().__init__(
            repo=repo,
            url=url,
            fromdate=fromdate,
            clean=clean,
            limit=limit,
            outdir=outdir,
            verify=verify,
        )
        self.ext = "json"
        self.filter = filter
        self.headers = {"Accept": "application/vnd.api+json"}
        logging.captureWarnings(True)

    def identifier(self, record):
        return f"datacite-{self.repo}-{record['id']}"

    def matches(self):
        query_params = {"query": self.filter, "page[size]": 1}
        response = requests.get(f"{self.url}/dois", params=query_params, headers=self.headers, verify=self.verify)
        
        if not response.ok:
            logging.error(f"Error fetching record count: {response.status_code} {response.text}")
            return 0
        
        try:
            return int(response.json().get("meta", {}).get("total", 0))
        except (ValueError, TypeError):
            logging.error("Unexpected response format from DataCite API")
            return 0

    def get_records(self):
        query_params = {
            "consortium-id": self.filter,
            "resource-type-id": "dataset",
            "page[size]": 10,
            "page[number]": 1,
        }
        total_fetched = 0
        
        while True:
            response = requests.get(f"{self.url}/dois", params=query_params, headers=self.headers, verify=self.verify)
            
            if not response.ok:
                logging.error(f"Error fetching records: {response.status_code} {response.text}")
                return
            
            try:
                data = response.json()
                items = data.get("data", [])
            except json.JSONDecodeError:
                logging.error("Invalid JSON response from DataCite API")
                return

            for item in items:
                yield item
                total_fetched += 1
                if self.limit and total_fetched >= self.limit:
                    return

            if len(items) < query_params["page[size]"]:
                break  # No more records left to fetch
            
            query_params["page[number]"] += 1

    def _write_record(self, fp, record, pretty_print=True):
        json.dump(record, fp, indent=4, sort_keys=True, ensure_ascii=False)
