
from .base import Command
from ..community import community, communities


class List(Command):

    def run(self, name=None, groups=False, all=False, summary=False):
        name = name or 'all'
        coms = {}
        for identifier in communities(name):
            com = community(identifier)
            if com.NAME not in coms:
                coms[com.NAME] = {}
            coms[com.NAME][com.IDENTIFIER] = {
                "url": com.URL, "schema": com.SCHEMA, "service_type": com.SERVICE_TYPE}
        if all:
            for name in coms:
                for identifier in coms[name]:
                    details = coms[name][identifier]
                    print(f"{name}, {identifier}, {details['url']}, {details['schema']}, {details['service_type']}")
        elif groups:
            for name in coms:
                for identifier in coms[name]:
                    print(f"{identifier}")
        elif summary:
            groups = 0
            for com in coms:
                groups += len(coms[com])
            print(f"Communities: {len(coms.keys())}, Groups: {groups}")
        else:
            for name in coms:
                print(f"{name}")
