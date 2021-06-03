import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join, first

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## READ TAGS DATASET
tags_dataset_path = "s3://mz-unibg-data-tcm2021/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

print("tags_dataset ready!\n")

tag_description_dataset_path = "s3://mz-unibg-data-tcm2021/tag_description_dataset.csv"
tag_description_dataset = spark.read.option("header","true").option("multiLine","true").csv(tag_description_dataset_path)

tag_description_dataset = tag_description_dataset.groupBy(col("tag").alias("tag_ref")).agg(first("description").alias("description"))

print("tag_description_dataset ready!\n")

# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("tag")).agg(collect_list("idx").alias("idxs"))

print("tags_dataset_agg ready!\n")

tags_dataset_final = tags_dataset_agg.join(tag_description_dataset, tags_dataset_agg.tag == tag_description_dataset.tag_ref, "left") \
    .drop("tag_ref") \
    .select(col("tag").alias("_id"), col("*"))
    
print("tag_dataset_final ready!\n")

mongo_uri = "mongodb://cluster0-shard-00-00.a9mo1.mongodb.net:27017,cluster0-shard-00-01.a9mo1.mongodb.net:27017,cluster0-shard-00-02.a9mo1.mongodb.net:27017"
mongo_url = "https://cloud.mongodb.com/v2/605c4969fbda5e38d3bc5531#metrics/replicaSet/609677258979dd2966e7bf8f/explorer/unibg_tedx_2021/tedx_data/find"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2021",
    "collection": "tedx_tag_data",
    "username": "MicheleAdmin",
    "password": "1234",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tags_dataset_final, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)

print(f"Done! Check: {mongo_url}")
