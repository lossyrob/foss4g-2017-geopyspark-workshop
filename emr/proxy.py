import sys, os,csv
from os.path import expanduser
from subprocess import Popen, PIPE

from util import *

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Sets up the proxy.')
    parser.add_argument('--key', required=True)

    args = parser.parse_args()

    cluster_ids = list(get_clusters().values())
    if len(cluster_ids) < 1:
        raise Exception("No clusters are recorded.")
    cluster_id = cluster_ids[0] # Any of them will do.
    home = expanduser("~")

    cmd = ['aws', 'emr', 'socks',
           '--cluster-id',cluster_id,
           '--key-pair-file',os.path.join(home, '{}.pem'.format(args.key))]
    run(cmd)
