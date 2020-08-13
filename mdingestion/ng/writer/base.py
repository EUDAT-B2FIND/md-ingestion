def clean_fields(data):
    new_data = dict()
    for key, value in data.items():
        if value:
            new_data[key] = value
    return new_data


class Writer(object):
    def write(self, doc, filename):
        raise NotImplementedError

    def json(self, doc):
        raise NotImplementedError
