class DocParser(object):
    def __init__(self, filename):
        self.filename = filename
        self._doc = None

    def read(self, filename):
        self.filename = filename

    @property
    def doc(self):
        if self._doc is None:
            if self.filename is not None:
                self._doc = self.parse_doc()
        return self._doc

    def parse_doc(self):
        raise NotImplementedError
