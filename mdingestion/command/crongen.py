import pandas as pd

from .base import Command
from ..community import repo, repos

from jinja2 import Environment, PackageLoader, select_autoescape
env = Environment(
    loader=PackageLoader("mdingestion"),
    autoescape=select_autoescape()
)


class CronGen(Command):

    def run(self, name=None, out=None):
        name = name or 'all'

        repo_list = []
        for identifier in repos(name):
            a_repo = repo(identifier)
            if a_repo.PRODUCTIVE:
                repo_list.append(a_repo)
        b2f_cron = env.get_template("b2f_cron")
        b2f_cron.render(repos=repo_list)
        with open(out, mode="w", encoding="utf-8") as fp:
            fp.write(b2f_cron.render(repos=repo_list))