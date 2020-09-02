import uuid
import pathlib

from ..exceptions import HarvesterError

import logging


class Harvester(object):
    def __init__(self, community, url, mdprefix, mdsubset, fromdate, limit, outdir, verify=True):
        self.community = community
        self.url = url
        self.mdprefix = mdprefix
        self.mdsubset = mdsubset
        self.fromdate = fromdate
        self.limit = limit or -1
        self.outdir = outdir
        self.verify = verify
        self.format = 'xml'
        self.ext = 'xml'

    def identifier(self, record):
        raise NotImplementedError

    def matches(self):
        return self.limit

    def total(self, limited=True):
        try:
            if limited and self.limit >= 0:
                total = min(self.limit, self.matches())
            else:
                total = self.matches()
        except Exception as e:
            msg = f"Harvester failed: {e}. url={self.url}, mdprefix={self.mdprefix}, mdsubset={self.mdsubset}"
            logging.critical(msg, exc_info=True)
            raise HarvesterError(f"Harvester failed: {e}")
        return total

    def uid(self, record):
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, self.identifier(record)))

    def filename(self, record):
        out = pathlib.Path(
            self.outdir,
            f"{self.community}-{self.mdprefix}",
            self.mdsubset,
            self.format,
            f"{self.uid(record)}.{self.ext}")
        return out

    def harvest(self):
        count = 0
        try:
            for record in self.get_records():
                count += 1
                if self.limit >= 0 and count > self.limit:
                    break
                yield record
        except Exception as e:
            msg = f"Harvester failed: {e}. url={self.url}, mdprefix={self.mdprefix}, mdsubset={self.mdsubset}"
            logging.critical(msg, exc_info=True)
            raise HarvesterError(f"Harvester failed: {e}")

    def get_records(self):
        raise NotImplementedError

    def write_record(self, record, pretty_print=True):
        out = self.filename(record)
        try:
            out.parent.mkdir(parents=True, exist_ok=True)
            with out.open(mode='w') as outfile:
                self._write_record(outfile, record, pretty_print)
                logging.debug(f'record written to {out}')
        except Exception:
            logging.warning(f"Could not write record {out}", exc_info=True)

    def _write_record(self, fp, record):
        raise NotImplementedError
