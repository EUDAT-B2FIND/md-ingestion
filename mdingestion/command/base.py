import os


class Command(object):
    def __init__(self, community=None, url=None, outdir=None, verify=True):
        self.community = community
        self.url = url
        self.outdir = outdir or os.path.curdir
        self.datadir = os.path.join(self.outdir, 'oaidata')
        self.summary_dir = os.path.join(self.outdir, 'summary')
        self.verify = verify
