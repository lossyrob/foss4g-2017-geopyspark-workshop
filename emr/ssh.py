import sys, os,csv
from os.path import expanduser
from subprocess import Popen, PIPE

from util import *

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Sets up the proxy.')
    parser.add_argument('--key', required=True)
    parser.add_argument('--cluster', default='', help='Specific cluster name')

    args = parser.parse_args()

    cluster_ids = list(get_clusters().values())
    if len(cluster_ids) < 1:
        raise Exception("No clusters are recorded.")
    if args.cluster:
        cluster_id = args.cluster
    else:
        cluster_id = cluster_ids[0]

    home = expanduser("~")

    cmd = ['aws', 'emr', 'ssh',
           '--region','us-east-1',
           '--cluster-id',cluster_id,
           '--key-pair-file',os.path.join(home, '{}.pem'.format(args.key))]

    print(' '.join(cmd))
    # run(cmd)
