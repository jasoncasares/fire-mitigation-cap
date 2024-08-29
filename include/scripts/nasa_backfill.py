import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_date, year, month, dayofmonth
import requests
import boto3

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'output_table', 's3_bucket'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Print Spark configurations and job arguments for debugging
print("Spark Configurations:")
print(f"spark.sql.extensions: {spark.conf.get('spark.sql.extensions')}")
print(f"spark.sql.catalog.iceberg: {spark.conf.get('spark.sql.catalog.iceberg')}")
print(f"spark.sql.catalog.iceberg.catalog-impl: {spark.conf.get('spark.sql.catalog.iceberg.catalog-impl')}")
print(f"spark.sql.catalog.iceberg.warehouse: {spark.conf.get('spark.sql.catalog.iceberg.warehouse')}")
print(f"spark.sql.defaultCatalog: {spark.conf.get('spark.sql.defaultCatalog')}")
print(f"spark.sql.shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

print("\nJob Arguments:")
print(f"JOB_NAME: {args['JOB_NAME']}")
print(f"output_table: {args['output_table']}")
print(f"s3_bucket: {args['s3_bucket']}")

# Get output table and S3 bucket from job arguments
output_table = args['output_table']
s3_bucket = args['s3_bucket']

# Set S3 output path
s3_output_path = f's3://{s3_bucket}/nasa_firms'

nasa_firms_url = "https://firms.modaps.eosdis.nasa.gov/data/country/modis/2023/modis_2023_United_States.csv"

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

print(f"Writing data to table: {output_table}")
print(f"S3 output path: {s3_output_path}")

# Ensure the output_table is in the correct format (catalog.database.table)
table_parts = output_table.split('.')
if len(table_parts) == 2:
    # Insert default database name 'default' if not provided
    catalog, database, table = table_parts[0], 'default', table_parts[1]
elif len(table_parts) == 3:
    catalog, database, table = table_parts
else:
    raise ValueError(f"Invalid output_table format: {output_table}. Expected format: catalog.database.table or catalog.table")

print(f"Catalog: {catalog}, Database: {database}, Table: {table}")

# Create the database if it doesn't exist
glue_client = boto3.client('glue')
try:
    glue_client.get_database(Name=database)
    print(f"Database {database} already exists.")
except glue_client.exceptions.EntityNotFoundException:
    print(f"Creating database {database}")
    glue_client.create_database(DatabaseInput={'Name': database})

# Write the data to the Iceberg table
df.writeTo(f"{catalog}.{database}.{table}") \
  .tableProperty("write.format.default", "parquet") \
  .partitionedBy("year", "month", "day") \
  .createOrReplace()

# Show the first 5 rows of the DataFrame
df.show(5)

job.commit()