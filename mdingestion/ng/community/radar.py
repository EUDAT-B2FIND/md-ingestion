from ..reader import DublinCoreReader


class RadarDublinCore(DublinCoreReader):
    def update(self, doc):
        doc.contributor = ['Radar']
