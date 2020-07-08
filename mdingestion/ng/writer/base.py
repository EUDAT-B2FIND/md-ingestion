class Writer(object):
    def write(self, doc, filename):
        raise NotImplementedError

    def json(self, doc):
        raise NotImplementedError
