from ..config import get_source


class Command(object):
    def __init__(self, sources, community,
                 url=None, verb=None, mdprefix=None, mdsubset=None,
                 outdir=None, verify=True):
        source = get_source(sources, community)
        self.community = community
        self.url = url or source.get('url')
        self.verb = verb or source.get('verb')
        self.mdprefix = mdprefix or source.get('mdprefix')
        mdsubset = mdsubset or source.get('mdsubset')
        self.mdsubset = mdsubset or 'SET_1'
        self.outdir = outdir
        self.verify = verify
