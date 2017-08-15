import sys, os, json
from os.path import expanduser

from util import *

def get_code(name, dns):
    return ['# Set up our spark context \n',
            'conf = gps.geopyspark_conf(appName="{}") \\\n'.format(name),
            '          .setMaster("yarn") \\\n',
            '          .set("spark.dynamicAllocation.enabled", "false") \\\n',
            '          .set("spark.executor.instances", "33") \\\n',
            '          .set("spark.executor.memory", "4800M") \\\n',
            '          .set("spark.executor.cores", "2") \\\n',
            '          .set("spark.ui.enabled","true") \\\n',
            '          .set("spark.hadoop.yarn.timeline-service.enabled", False) \n',
            'sc = SparkContext(conf=conf)\n',
            'print("Access UI at http://' + dns + ':20888/proxy/{}/jobs/".format(sc.applicationId))']

if __name__ == "__main__":
    home = expanduser("~")
    import argparse

    parser = argparse.ArgumentParser(description='Upload notebooks to workshop clusters, with modification.')
    parser.add_argument('--key', required=True)
    parser.add_argument('--exercise', default='all', help='One of [exercise1, exercise2, exercise3, exercise4]')
    parser.add_argument('--clusters', default='all', help='Clusters to upload to.')
    parser.add_argument('part')

    args = parser.parse_args()

    key_path = args.key
    exercise = args.exercise
    part = args.part

    if args.clusters == 'all':
        names = list(get_clusters().keys())
    else:
        names = args.clusters.split(',')


    for name in names:
#    for name, cluster_id in get_clusters().items():
        cluster_id = get_clusters()[name]
#        print(['aws', 'emr', 'describe-cluster',  '--cluster-id', cluster_id])
        cluster_js = json.loads((run(['aws', 'emr', 'describe-cluster',
                                      '--region', 'us-east-1',
                                      '--cluster-id', cluster_id])))
        master_dns = cluster_js['Cluster']['MasterPublicDnsName']
        state_abbv = name[-2:]
        (state, county_name) = state_by_abbv[state_abbv]

        def run_cmd(s):
            run(['ssh', '-o', 'StrictHostKeyChecking=no', '-i', os.path.join(home, '{}.pem'.format(args.key)),
                 'hadoop@{}'.format(master_dns),
                 s])

        def put(path, target):
            full_target = os.path.join('/home/hadoop/notebooks/', target)
            target_dir = os.path.dirname(full_target)
            run_cmd("mkdir -p {}".format(target_dir))

            run(['scp', '-o', 'StrictHostKeyChecking=no', '-i', os.path.join(home, '{}.pem'.format(args.key)),
                 path,
                 'hadoop@{}:{}'.format(master_dns,full_target.replace(" ", "\ ")),
            ])


        def put_notebook(path, target_dir):
            sc_tag = "# Set up our spark context"
            state_tag = "# Grab data for"
            basename =  os.path.basename(path)
            name = "{} - {}".format(os.path.splitext(basename)[0], state)
            code = get_code(name, master_dns)
            tmp_path = name + ".tmp"
            nb = json.loads(open(path).read())
            for cell in nb['cells']:
                source = cell['source']
                if len(source) > 1 and source[0].startswith(sc_tag):
                    cell['source'] = code
                elif len(source) > 1 and source[0].startswith(state_tag):
                    source[0] = "# Grab data for {}\n".format(state)
                    source[1] = 'state_name, county_name = "{}", "{}"\n'.format(state_abbv, county_name)
            print("Writing {}".format(tmp_path))
            print(target_dir)
            print(os.path.join(target_dir, basename))
            open(tmp_path, 'w').write(json.dumps(nb))
            put(tmp_path, os.path.join(target_dir, basename))
            os.remove(tmp_path)

        exercise_dir = os.path.abspath(os.path.join('..', 'exercises'))
        if exercise != 'all':
            exercise_dir = os.path.join(exercise_dir, exercise)
        if not os.path.isdir(exercise_dir):
            print("Not a dir {}".format(exercise_dir))
            sys.exit(1)

        for root, folders, files in os.walk(exercise_dir):
            basename = os.path.basename(root)
            if not basename.startswith('exercise') or basename == 'exercises':
                continue

            target_dir = basename
            for f in files:
                if part == 'A':
                    if f.startswith('Reference') or f.startswith('Exercise'):
                        put_notebook(os.path.join(root, f), target_dir)
                if part == 'B':
                    if f.startswith('Solution'):
                        put_notebook(os.path.join(root, f), target_dir)
                if f.endswith('.png') or f.endswith('.jpg'):
                    put(os.path.join(root, f), os.path.join(target_dir, f))

        run_cmd('sudo chown -R hadoop:hadoop /home/hadoop/notebooks')
        run_cmd('sudo chmod -R a+rw /home/hadoop/notebooks')
