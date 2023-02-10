from .oai import OAIHarvester
from lxml import etree
import logging


class OAIHarvesterIvoa(OAIHarvester):
    def _write_record(self, fp, record, pretty_print=True):
        xml = etree.tostring(record.xml, pretty_print=pretty_print).decode('utf8')
        if '<eudc:resourceType>Other</eudc:resourceType>' in xml:
            logging.warning('skipped resourceType Other')
        elif '<eudc:resourceType>Text</eudc:resourceType>' in xml:
            logging.warning('skipped resourceType Text')
        else:
            fp.write(xml)
