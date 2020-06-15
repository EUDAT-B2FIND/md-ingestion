class DocParser(object):
    def __init__(self, filename):
        self.filename = filename
        self._doc = None

    @property
    def doc(self):
        if self._doc is None:
            self._doc = self.parse_doc()
        return self._doc

    def parse_doc(self):
        raise NotImplementedError
