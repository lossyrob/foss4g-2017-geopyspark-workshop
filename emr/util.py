import sys, os, csv
from subprocess import Popen, PIPE

state_info = {
    "Alabamba": ("AL", "Autauga"),
    "Arizona" : ("AZ", "Yavapai"),
    "Arkansas": ("AR","Pulaski"),
    "California": ("CA", "Merced"),
    "Colorado": ("CO","Elbert"),
    "Florida": ("FL", "Marion"),
    "Georgia": ("GA", "Laurens"),
    "Idaho": ("ID", "Butte"),
    "Illinois": ("IL", "Fayette"),
    "Iowa": ("IA","Butler"),
    "Kansas": ("KS","Barton"),
    "Kentucky": ("KY","Bourbon"),
    "Louisiana": ("LA", "Winn"),
    "Maryland": ("MD", "Howard"),
    "Massachusetts": ("MA", "Plymouth"),
    "Mississippi": ("MS", "Scott"),
    "Missouri": ("MO","Miller"),
    "Montana": ("MT","Cascade"),
    "Nebraska": ("NE", "Hall"),
    "Nevada": ("NV","Mineral"),
    "New Mexico": ("NM", "Colfax"),
    "New York" : ("NY", "Albany"),
    "North Carolina": ("NC", "Randolph"),
    "North Dakota": ("ND", "Kidder"),
    "Ohio": ("OH", "Ross"),
    "Oklahoma": ("OK","Canadian"),
    "Oregon": ("OR", "Wheeler"),
    "Pennsylvania" : ("PA", "Lancaster"),
    "Rhode Island": ("RI", "Kent"),
    "South Carolina": ("SC", "Lee"),
    "South Dakota": ("SD","Buffalo"),
    "Tennessee": ("TN","Coffee"),
    "Texas": ("TX","Bell"),
    "Utah": ("UT","Sanpete"),
    "Virginia": ("VA","Amelia"),
    "Wisconsin": ("WI","Columbia"),
    "Wyoming": ("WY","Platte")
}

state_ids = list(map(lambda x: x[0], state_info.values()))

state_by_abbv = { k: (v1, v2) for (v1, (k, v2)) in state_info.items() }

def run(cmd):
    print(' '.join(cmd))
    process = Popen(cmd, stdout=PIPE)
    (output, err) = process.communicate()
    exit_code = process.wait()
    if exit_code == 0:
        return output.decode("utf-8")
    else:
        raise Exception("{} exited with error code {}".format(output, exit_code))

def get_clusters():
    if not os.path.exists('clusters.csv'):
        return {}
    with open('clusters.csv', 'r') as csvfile:
        return {key : value.strip() for key, value in csv.reader(csvfile)}

def write_clusters(d):
    with open('clusters.csv', 'w') as csvfile:
        w = csv.writer(csvfile)
        for k in d:
            w.writerow([k, d[k]])

def add_cluster(name, cluster_id):
    d = get_clusters()
    if name in d:
        raise Exception("Cluster with name {} already exists".format(name))
    d[name] = cluster_id
    write_clusters(d)

def delete_cluster(name):
    d = get_clusters()
    if not name in d:
        raise Exception("Cluster with name {} does not exist.".format(name))
    del d[name]
    write_clusters(d)
