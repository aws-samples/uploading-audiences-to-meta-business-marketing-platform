import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Added parameters 
args = getResolvedOptions(sys.argv, ["JOB_NAME","sourcebucket","targetbucket","sourcetable","targettable","targetcatalogdb", "targetcatalogtable"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# set parameters
sourcebucket=str(args["sourcebucket"])
targetbucket=str(args["targetbucket"])
sourcetable=str(args["sourcetable"])
targettable=str(args["targettable"])
targetcatalogdb=str(args["targetcatalogdb"])
targetcatalogtable=str(args["targetcatalogtable"])

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": False,
        "separator": "|",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            f"s3://{sourcebucket}/{sourcetable}/"
        ]
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node normalize
SqlQuery552 = """

select 
    trim(col1) as cust_id,
    trim(lower(col8)) as fn,
    trim(lower(col9)) as ln,
    col11 as dob_d,
    col12 as dob_m, 
    col13 as dob_y,
    trim(lower(col16)) as em
from myDataSource
    where col8 != '' 
    and col9 != '' 
    and col11 != '' 
    and col12 != '' 
    and col13 != '' 
    and col16 != '' 
"""
normalize_node1661904142447 = sparkSqlQuery(
    glueContext,
    query=SqlQuery552,
    mapping={"myDataSource": S3bucket_node1},
    transformation_ctx="normalize_node1661904142447",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path=f"s3://{targetbucket}/{targettable}/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase=targetcatalogdb,
    catalogTableName=targetcatalogtable,
)
S3bucket_node3.setFormat("csv")
S3bucket_node3.writeFrame(normalize_node1661904142447)
job.commit()
