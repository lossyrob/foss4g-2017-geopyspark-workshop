include config-aws.mk     # Vars related to AWS credentials and services used
include config-emr.mk     # Vars related to type and size of EMR cluster

ifeq ($(USE_SPOT),true)
MASTER_BID_PRICE:=--master_bid_price ${MASTER_PRICE}
WORKER_BID_PRICE:=--worker_bid_price ${WORKER_PRICE}
endif

create-clusters:
	python create_clusters.py \
		${CLUSTER_COUNT} \
		--s3 ${S3_URI} \
		--key ${EC2_KEY} \
		--subnet ${SUBNET_ID} \
		${MASTER_BID_PRICE} \
		--master_instance_type ${MASTER_INSTANCE} \
		${WORKER_BID_PRICE} \
		--worker_instance_type ${WORKER_INSTANCE} \
		--worker_count ${WORKER_COUNT}

terminate-cluster:
	python terminate_clusters.py --clusters "${NAMES}"

startit:
	python startit.py --clusters "${NAMES}" --key ${EC2_KEY}

proxy:
	python proxy.py --key ${EC2_KEY}

ssh:
	python ssh.py --key ${EC2_KEY} --cluster "${NAME}"

download-notebooks:
	python download-notebooks.py --key ${EC2_KEY} --cluster "${NAME}"

upload-code:
	@aws s3 cp bootstrap-geopyspark-docker.sh ${S3_URI}/bootstrap-geopyspark-docker.sh

upload-exercises:
	python upload-notebooks.py --key ${EC2_KEY} A

upload-single-exercise:
	python upload-notebooks.py --key ${EC2_KEY} A --exercise exercise${EXERCISE}

upload-e1-solution:
	python upload-notebooks.py --key ${EC2_KEY} --exercise exercise1 B

upload-e2-solution:
	python upload-notebooks.py --key  ${EC2_KEY} --exercise exercise2 B

upload-e3-solution:
	python upload-notebooks.py --key ${EC2_KEY} --exercise exercise3 B

upload-notebooks: upload-exercises upload-e1-solution upload-e2-solution upload-e3-solution
