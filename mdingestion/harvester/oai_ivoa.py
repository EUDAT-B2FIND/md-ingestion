from .oai import OAIHarvester


class OAIHarvesterIvoa(OAIHarvester):
    def _write_record(self, fp, record, pretty_print=True):
        xml = etree.tostring(record.xml, pretty_print=pretty_print).decode('utf8')
        fp.write(xml)
