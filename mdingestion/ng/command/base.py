class Command(object):
    def __init__(self, community=None, url=None, outdir=None, verify=True):
        self.community = community
        self.url = url
        self.outdir = outdir
        self.verify = verify
