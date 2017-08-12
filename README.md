# FOSS4G 2017 Workshop: Analyzing large raster data in a Jupyter notebook with GeoPySpark on AWS

This repository has the following components:

- `ingest`: Scala GeoTrellis code that was used to ingest the landsat, CDL and NLCD data
used in the workshop, along with the Makefile to run the ingests on EMR.
- `emr`: The Makefiles and code to work with the workshop clusters, including modifying
and uploading the notebooks for the workshop.
- `exercises`: Reference, Exercise and Solution notebooks that are run as part of the workshop.

## Running an Workshop EMR cluster

To run this, you'll have need awscli installed,
with the `AWS_PROFILE` account being able to interact with EMR through `aws emr`.

Configure `emr/config-aws.mk`, based on `emr/config-aws.mk.template`.

### Create clusters

Run `make create-clusters`. This will create some number of clusters,
the count of whic can be controlled by the `CLUSTER_COUNT` property in
`emr/config-emr.mk`.

### Copying Notebooks to your EMR cluster

Use the Makefile here for copying up the workshop notebooks.

```
# Copy the reference notebooks and exercies
make copy-references
# Copy the exercise 1 solution
make copy-exercise-1-solution
# Copy the exercise 2 solution
make copy-exercise-2-solution
# Copy the exercise 3 solution
make copy-exercise-3-solution
```

### Other makefile commands

See `emr/Makefile` for other commands to temrinate the cluster, etc.
