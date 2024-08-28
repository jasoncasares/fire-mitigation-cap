import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import to_date, year, month, dayofmonth
import requests
from dotenv import load_dotenv
import os

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 path for your Iceberg data
s3_bucket = "fire-mitigation-data"
s3_path = f"s3://{s3_bucket}/nasa_firms"

nasa_firms_url = "https://firms.modaps.eosdis.nasa.gov/data/country/modis/2023/modis_2023_United_States.csv"

# Download the data
response = requests.get(nasa_firms_url)
data = response.text

# Load environment variables from .env file
load_dotenv()

# Get AWS credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://files.polygon.io/")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")


# Download the data
response = requests.get(nasa_firms_url)
data = response.text

# Read the CSV data into a Spark DataFrame
df = spark.read.csv(sc.parallelize(data.split('\n')), header=True, inferSchema=True)


# Process the data: convert date string to date type and extract year, month, day
df = df.withColumn("acq_date", to_date("acq_date", "yyyy-MM-dd")) \
       .withColumn("year", year("acq_date")) \
       .withColumn("month", month("acq_date")) \
       .withColumn("day", dayofmonth("acq_date"))

spark.sql(f"""
CREATE OR REPLACE TABLE {output_table} (
    latitude DOUBLE,
    longitude DOUBLE,
    brightness DOUBLE,
    scan DOUBLE,
    track DOUBLE,
    acq_date DATE,
    acq_time STRING,
    satellite STRING,
    instrument STRING,
    confidence INT,
    version DOUBLE,
    bright_t31 DOUBLE,
    frp DOUBLE,
    daynight STRING,
    type INT
)
USING iceberg
PARTITIONED BY (acq_date)
""")

df = df.withColumn("acq_date", to_date("acq_date", "yyyy-MM-dd")) \
       .withColumn("year", year("acq_date")) \
       .withColumn("month", month("acq_date")) \
       .withColumn("day", dayofmonth("acq_date"))

# Show the first 5 rows of the DataFrame
df.printSchema()

df.collect()

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
job.commit()