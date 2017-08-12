import sys, os,csv
from subprocess import Popen, PIPE

from util import *

def run_terminate_cluster(name):
    cmd = ['aws', 'emr', 'terminate-clusters',
           '--cluster-ids', get_clusters()[name]]

    return run(cmd)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Terminate cluster(s).')
    parser.add_argument('--clusters', default='', help='comma seperated names of clusters to delete')

    args = parser.parse_args()

    if not args.clusters:
        names = list(get_clusters().keys())
    else:
        names = args.clusters.split(',')

    for name in names:
        run_terminate_cluster(name)
        delete_cluster(name)
