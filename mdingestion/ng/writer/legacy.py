from .ckan import CKANWriter


class LegacyWriter(CKANWriter):
    format = 'json'

    def json(self, doc):
        data = self._ckan_fields(doc)
        data.update(self._extra_fields(doc))
        data.update(self._oai_fields(doc))
        return data
