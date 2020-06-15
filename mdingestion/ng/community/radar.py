from ..schema import DublinCore

class RadarDublinCore(DublinCore):

    @property
    def contributor(self):
        return ['Radar']