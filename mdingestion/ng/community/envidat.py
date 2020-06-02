from ..schema import DataCite, ISO19139


class EnvidatDatacite(DataCite):
    @property
    def contributor(self):
        return 'EnviDat'


class EnvidatISO19139(ISO19139):
    @property
    def contributor(self):
        return 'EnviDat'
