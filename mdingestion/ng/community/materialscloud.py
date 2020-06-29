from ..reader import DublinCoreReader


class MaterialsCloudDublinCore(DublinCoreReader):
    def update(self, doc):
        doc.doi = self.doi(doc)
        doc.source = self.source(doc)
        doc.open_access = True
        doc.discipline = 'Materials Science and Engineering'
        doc.contact = 'info@materialscloud.org'

    def doi(self, doc):
        dois = [id.text for id in self.parser.doc.metadata.find_all('identifier') if id.text.startswith('doi:')]
        if dois:
            url = f"https://doi.org/{dois[0][4:]}"
        else:
            url = ''
        return url

    def source(self, doc):
        urls = [id.text for id in self.parser.doc.metadata.find_all('identifier') if 'archive.materialscloud.org' in id.text]
        if urls:
            url = urls[0]
        else:
            url = ''
        return url
