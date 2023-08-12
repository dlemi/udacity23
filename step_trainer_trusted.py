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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1691815108497 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityaugustbucket/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1691815108497",
)

# Script generated for node Customers Curated
CustomersCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityaugustbucket/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomersCurated_node1",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1691815307107 = ApplyMapping.apply(
    frame=StepTrainerLanding_node1691815108497,
    mappings=[
        ("sensorReadingTime", "long", "right_sensorReadingTime", "long"),
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("distanceFromObject", "int", "right_distanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1691815307107",
)

# Script generated for node Join
Join_node1691815025392 = Join.apply(
    frame1=CustomersCurated_node1,
    frame2=RenamedkeysforJoin_node1691815307107,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1691815025392",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.getSink(
    path="s3://udacityaugustbucket/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node3",
)
StepTrainerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node3.setFormat("json")
StepTrainerTrusted_node3.writeFrame(Join_node1691815025392)
job.commit()
