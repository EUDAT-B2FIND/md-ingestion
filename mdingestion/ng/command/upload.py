from mdingestion.uploading import Uploader as LegacyUploader
from mdingestion.uploading import CKAN_CLIENT as LegacyCKANClient
from mdingestion.settings import init as legacy_init_upload

from .base import Command


class Upload(Command):
    def upload(self, iphost=None, auth=None):
        legacy_init_upload()
        ckan = LegacyCKANClient(iphost, auth)
        wrapped = LegacyUploader(
            OUT=None,
            CKAN=ckan,
            ckan_check=None,
            HandleClient=None,
            cred=None,
            base_outdir=self.outdir,
            fromdate=None,
            iphost=iphost)
        request = [
            self.community,
            self.url,
            None,
            self.mdprefix,
        ]
        wrapped.upload(request)
