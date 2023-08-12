import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityaugustbucket/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node Filter-Opt in
FilterOptin_node1691809910724 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="FilterOptin_node1691809910724",
)

# Script generated for node Customer Trusted
CustomerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=FilterOptin_node1691809910724,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacityaugustbucket/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node3",
)

job.commit()
