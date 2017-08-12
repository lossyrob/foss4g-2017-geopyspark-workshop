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

NAMES ?=
terminate-cluster:
	python terminate_clusters.py --clusters "${NAMES}"

startit:
	python startit.py --clusters "${NAMES}" --key ${EC2_KEY}

proxy:
	python proxy.py --key ${EC2_KEY}

ssh:
	python ssh.py --key ${EC2_KEY} --cluster "${NAME}"


upload-code:
	@aws s3 cp bootstrap-geopyspark-docker.sh ${S3_URI}/bootstrap-geopyspark-docker.sh

upload-section1-template:
	python upload-notebooks.py --key ${EC2_KEY} section1 A

upload-section1-solution:
	python upload-notebooks.py --key ${EC2_KEY} section1 B

upload-section2-template:
	python upload-notebooks.py --key ${EC2_KEY} section2 A

upload-section2-solution:
	python upload-notebooks.py --key  ${EC2_KEY} section2 B

upload-section3-template:
	python upload-notebooks.py --key ${EC2_KEY} section3 A

upload-section3-solution:
	python upload-notebooks.py --key ${EC2_KEY} section3 B

upload-section4:
	python upload-notebooks.py --key ${EC2_KEY} section4 C

upload-notebooks: upload-section1-template upload-section1-solution upload-section2-template upload-section2-solution upload-section3-template upload-section3-solution upload-section4
