from ckanapi import RemoteCKAN, NotFound, NotAuthorized

from .base import Command
# from ..community import repo, repos


agent = 'b2f'


def get_dataset_list(group, iphost, https):
    '''get_dataset_list'''
    dataset_list = []
    proto = 'https' if https else 'http'

    with RemoteCKAN(f"{proto}://{iphost}", user_agent=agent) as ckan:
        packages = ckan.action.member_list(id=group, object_type='package')
        for p in packages:
            dataset_list.append(p[0])

    return dataset_list


def purge_dataset_list(dataset_list, iphost, apikey, https):
    '''purge_dataset_list'''
    proto = 'https' if https else 'http'
    with RemoteCKAN(f"{proto}://{iphost}", apikey=apikey, user_agent=agent) as ckan:
        for dataset_id in dataset_list:
            try:
                ckan.action.dataset_purge(id=dataset_id)
                print('Dataset \'' + dataset_id + '\' purged')
            except NotAuthorized:
                print('Dataset \'' + dataset_id + '\' not authorized')
            except NotFound:
                print('Dataset \'' + dataset_id + '\' not found')
    return True


class Purge(Command):
    def run(self, iphost=None, dataset=None, auth=None, https=False, verify=True,
            silent=False):
        if self.repo:
            datasets = get_dataset_list(self.repo, iphost=iphost, https=https)
        else:
            datasets = [dataset]
        purge_dataset_list(datasets, iphost=iphost, apikey=auth, https=https)
