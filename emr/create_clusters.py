import sys, os,csv
from subprocess import Popen, PIPE

from util import *

def run_create_cluster(name,
                       s3_uri,
                       key,
                       subnet,
                       master_bid_price,
                       master_instance_type,
                       worker_bid_price,
                       worker_instance_type,
                       worker_count):
    if master_bid_price:
        master_bid_price = "BidPrice={},".format(master_bid_price)
    else:
        master_bid_price = ""

    if worker_bid_price:
        worker_bid_price = "BidPrice={},".format(worker_bid_price)
    else:
        worker_bid_price = ""

    cmd = ['aws', 'emr', 'create-cluster',
           '--name', "{}".format(name),
           '--release-label', 'emr-5.7.0',
           '--output', 'text',
           '--use-default-roles',
           '--log-uri', '{}/logs'.format(s3_uri),
           '--ec2-attributes', 'KeyName={},SubnetId={}'.format(key, subnet),
           '--applications', 'Name=Hadoop', 'Name=Spark', 'Name=Ganglia',
           '--instance-groups',
           'Name=Master,{}InstanceCount=1,InstanceGroupType=MASTER,InstanceType={}'.format(master_bid_price,
                                                                                         master_instance_type),
           'Name=Workers,{}InstanceCount={},InstanceGroupType=CORE,InstanceType={}'.format(worker_bid_price,
                                                                                           worker_count,
                                                                                           worker_instance_type),
           '--bootstrap-actions', 'Name=GeoPySpark,Path={}/bootstrap-geopyspark-docker.sh'.format(s3_uri)]
    return run(cmd)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Create clusters for workshop.')
    parser.add_argument('cluster_count', metavar='CLUSTER_COUNT', type=int)
    parser.add_argument('--s3', required=True)
    parser.add_argument('--key', required=True)
    parser.add_argument('--subnet', required=True)
    parser.add_argument('--master_bid_price', type=float, default=None)
    parser.add_argument('--master_instance_type', required=True)
    parser.add_argument('--worker_bid_price', type=float, default=None)
    parser.add_argument('--worker_instance_type', required=True)
    parser.add_argument('--worker_count', type=int, required=True)

    args = parser.parse_args()

    existing = set(list(get_clusters().keys()))

    available_states = set(state_ids) - existing

    names = list(map(lambda state: (state, "FOSS4G Workshop Cluster - %s" % state), list(state_ids)[:args.cluster_count]))

    num_names = len(names)
    if num_names < args.cluster_count:
        print("Not enough states to create {} new clusters, can only create {} more".format(args.cluster_count,
                                                                                            num_names))
        sys.exit(1)

    sys.stdout.write("Create {} clusters? [y/N] ".format(num_names))
    choice = input().lower()
    if choice != 'y':
        print("Not gonna do it.")
        sys.exit(0)

    for state, name in names:
        cluster_id = run_create_cluster(name,
                                        args.s3,
                                        args.key,
                                        args.subnet,
                                        args.master_bid_price,
                                        args.master_instance_type,
                                        args.worker_bid_price,
                                        args.worker_instance_type,
                                        args.worker_count).strip()
        print(" CREATED CLUSTER NAME {} AT {}".format(name, cluster_id))
        add_cluster(state, cluster_id)
