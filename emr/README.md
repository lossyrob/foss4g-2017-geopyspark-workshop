# Step 1

Modify config-aws.mk.template. Move to config-aws.mk

Also make sure AWS_PROFILE is set correctly.

# Step 2

make upload-code

# Step 3

Modify config-emr.mk to use spot instances and cluster count of 1 (or whatever you need).

# Step 4

make create-clusters

# Step 5

make upload-notebooks
