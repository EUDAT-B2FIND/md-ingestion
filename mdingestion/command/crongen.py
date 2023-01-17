import datetime as dt

from .base import Command
from ..community import repo, repos

from jinja2 import Environment, PackageLoader, select_autoescape
env = Environment(
    loader=PackageLoader("mdingestion"),
    autoescape=select_autoescape()
)


class CronGen(Command):

    def run(self, name=None, auth=None, out=None):
        name = name or 'all'

        t = dt.datetime(2023, 1, 1, 0, 0)
        delta = dt.timedelta(minutes=10)

        repo_list = []
        for identifier in repos(name):
            a_repo = repo(identifier)
            if a_repo.PRODUCTIVE:
                t = t + delta
                cron_info = {
                    'identifier': a_repo.identifier,
                    'day': '*' if a_repo.CRON_DAILY else '0',
                    'hour': t.hour,
                    'minute': t.minute,
                }
                repo_list.append(cron_info)
        b2f_cron = env.get_template("b2f_cron")
        with open(out, mode="w", encoding="utf-8") as fp:
            fp.write(b2f_cron.render(repos=repo_list, auth=auth))
