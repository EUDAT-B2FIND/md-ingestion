from mdingestion.uploading import Uploader as LegacyUploader
from mdingestion.uploading import CKAN_CLIENT as LegacyCKANClient
from mdingestion.settings import init as legacy_init_upload

from .base import Command
from ..util import parse_source_list


class Upload(Command):
    def __init__(self, outdir=None, source_list=None, iphost=None, auth=None):
        ckan = LegacyCKANClient(iphost, auth)
        self.wrapped = LegacyUploader(
            OUT=None,
            CKAN=ckan,
            ckan_check=None,
            HandleClient=None,
            cred=None,
            base_outdir=outdir,
            fromdate=None,
            iphost=iphost)
        self.sources = parse_source_list(source_list)

    def upload(self, community):
        legacy_init_upload()
        source = self.sources.get(community, dict())
        url = source.get('url')
        verb = None
        mdprefix = source.get('mdprefix')
        request = [
            community,
            url,
            verb,
            mdprefix,
        ]
        self.wrapped.upload(request)
