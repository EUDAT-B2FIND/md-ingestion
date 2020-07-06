from ..reader import DublinCoreReader


class MaterialsCloudDublinCore(DublinCoreReader):
    def update(self, doc):
        doc.doi = self.doi(doc)
        doc.source = self.source(doc)
        doc.open_access = True
        doc.discipline = 'Materials Science and Engineering'
        doc.contact = 'info@materialscloud.org'

    def doi(self, doc):
        dois = [id for id in self.parser.find('metadata.identifier') if id.startswith('doi:')]
        if dois:
            url = f"https://doi.org/{dois[0][4:]}"
        else:
            url = ''
        return url

    def source(self, doc):
        urls = [url for url in self.parser.find('metadata.identifier') if 'archive.materialscloud.org' in url]
        return urls
