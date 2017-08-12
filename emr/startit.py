import sys, os,csv
from os.path import expanduser

from util import *


if __name__ == "__main__":
    home = expanduser("~")
    import argparse

    parser = argparse.ArgumentParser(description='Terminate cluster(s).')
    parser.add_argument('--clusters', default='', help='comma seperated names of clusters to delete')
    parser.add_argument('--key', required=True)

    args = parser.parse_args()

    key = args.key

    if not args.clusters:
        names = list(get_clusters().keys())
    else:
        names = args.clusters.split(',')

    for name in names:
        cluster_id = get_clusters()[name]
        cmd = ['aws', 'emr', 'put',
               '--cluster-id', cluster_id,
               '--key-pair-file', os.path.join(home, '{}.pem'.format(args.key)),
               '--src', 'startit.sh',
               '--dest', '/home/hadoop/startit.sh']
        run(cmd)

        cmd = ['aws', 'emr', 'ssh',
               '--cluster-id', cluster_id,
               '--key-pair-file', os.path.join(home, '{}.pem'.format(args.key)),
               '--command', '/home/hadoop/startit.sh']
        run(cmd)
