import argparse
from ckanapi import RemoteCKAN, NotAuthorized, NotFound

agent = 'purge_dataset.py/1.0'
instance = 'http://eudatmd2.dkrz.de'
api_key = ""


def get_dataset_list(group):
    '''get_dataset_list'''
    dataset_list = []

    with RemoteCKAN(instance, user_agent=agent) as ckan:
        packages = ckan.action.member_list(id=group, object_type='package')
        for p in packages:
            dataset_list.append(p[0])

    return dataset_list

def purge_dataset_list(dataset_list):
    '''purge_dataset_list'''
    with RemoteCKAN(instance, apikey=api_key, user_agent=agent) as ckan:
        for dataset_id in dataset_list:
            try:
                pkg = ckan.action.dataset_purge(id=dataset_id)
                print('Dataset \'' + dataset_id + '\' purged')

            except NotAuthorized:
                print('Dataset \'' + dataset_id + '\' not authorized')
            except NotFound:
                print('Dataset \'' + dataset_id + '\' not found')
    return True

def main():
    '''main'''
    datasets = []

    parser = argparse.ArgumentParser(description='Purge B2FIND dataset(s)')
#    parser.add_argument('--instance', help='B2FIND instance', required=True)
    parser_group = parser.add_mutually_exclusive_group(required=True)
    parser_group.add_argument('--dataset', help='purge dataset')
    parser_group.add_argument('--group', help='purge all datasets in group')
    args = parser.parse_args()

    if args.group:
        datasets = get_dataset_list(args.group)
    elif args.dataset:
        datasets.append(args.dataset)

    purge_dataset_list(datasets)

#    for y in datasets:
#        print(y)

if __name__ == "__main__":
    main()
