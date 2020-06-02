from mdingestion.uploading import Uploader as LegacyUploader
from mdingestion.uploading import CKAN_CLIENT as LegacyCKANClient

from .command import Command


class Uploader(Command):
    def __init__(self, ckan_check=None, cred=None, outdir=None, fromdate=None, iphost=None, auth=None):
        ckan = LegacyCKANClient(iphost, auth)
        self.wrapped = LegacyUploader(
            OUT=None,
            CKAN=ckan,
            ckan_check=ckan_check,
            HandleClient=None,
            cred=cred,
            base_outdir=outdir,
            fromdate=fromdate,
            iphost=iphost)

    def upload(self, community, url=None, mdprefix=None, mdsubset=None, target_mdschema=None):
        pass
