from .dans import BaseDans


class DSSsh(BaseDans):
    IDENTIFIER = 'ds_ssh'
    TITLE = 'DANS Data Station Social Sciences and Humanities'
    URL = 'https://ssh.datastations.nl/oai'
    DESCRIPTION = 'A domain-specific repository for Social Sciences and Humanities data, primarily aimed at the Netherlands, but not exclusively.'
    REPOSITORY_ID = 're3data:r3d100014195'
    REPOSITORY_NAME = 'DANS Data Station Social Sciences and Humanities'
