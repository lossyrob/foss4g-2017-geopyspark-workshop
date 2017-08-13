import sys, os, json
from os.path import expanduser

from util import *

if __name__ == "__main__":
    home = expanduser("~")
    import argparse

    parser = argparse.ArgumentParser(description='Download notebooks to workshop clusters, with modification.')
    parser.add_argument('--key', required=True)
    parser.add_argument('--cluster', default='', help='Specific cluster name')

    args = parser.parse_args()

    key_path = args.key
    cluster_ids = list(get_clusters().values())
    if len(cluster_ids) < 1:
        raise Exception("No clusters are recorded.")
    if args.cluster:
        cluster_id = args.cluster
    else:
        cluster_id = cluster_ids[0]

    cluster_js = json.loads((run(['aws', 'emr', 'describe-cluster',  '--cluster-id', cluster_id])))
    master_dns = cluster_js['Cluster']['MasterPublicDnsName']

    def run_cmd(s):
        run(['ssh', '-o', 'StrictHostKeyChecking=no', '-i', os.path.join(home, '{}.pem'.format(args.key)),
             'hadoop@{}'.format(master_dns),
             s])

    def get(path):
        full_source = os.path.join('/home/hadoop/notebooks/', path)
        full_target = os.path.join('../downloaded', path)
        target_dir = os.path.dirname(full_target)
        run(['mkdir', '-p',  "{}".format(os.path.abspath(target_dir))])

        run(['scp', '-o', 'StrictHostKeyChecking=no', '-i', os.path.join(home, '{}.pem'.format(args.key)),
             'hadoop@{}:{}'.format(master_dns,full_source.replace(" ", "\ ")),
             full_target
        ])


    targets = [#'exercise2/Reference 2 - Exploring the Landsat Layer.ipynb',
               #'exercise2/Solution 2 - Working with Landsat and NDVI.ipynb',
               'exercise2/Reference 2 - Exploring the Landsat Layer.ipynb']

    for target in targets:
        get(target)
