import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityaugustbucket/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1691811475660 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityaugustbucket/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1691811475660",
)

# Script generated for node Join
Join_node1691811446475 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerLanding_node1691811475660,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1691811446475",
)

# Script generated for node Drop Fields
DropFields_node1691814017058 = DropFields.apply(
    frame=Join_node1691811446475,
    paths=[
        "shareWithFriendsAsOfDate",
        "phone",
        "lastUpdateDate",
        "email",
        "customerName",
        "registrationDate",
        "shareWithPublicAsOfDate",
        "serialNumber",
        "birthDay",
    ],
    transformation_ctx="DropFields_node1691814017058",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1691811545507 = glueContext.getSink(
    path="s3://udacityaugustbucket/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1691811545507",
)
AccelerometerTrusted_node1691811545507.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1691811545507.setFormat("json")
AccelerometerTrusted_node1691811545507.writeFrame(DropFields_node1691814017058)
job.commit()
