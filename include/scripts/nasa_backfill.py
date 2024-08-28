from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, month, dayofmonth
import requests
from io import StringIO

# S3 path for Iceberg data
s3_bucket = "fire-mitigation-data"
s3_path = f"s3://{s3_bucket}/iceberg-data"

spark = SparkSession.builder \
   .appName("NASA FIRMS Data Processing") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.aws", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.aws.type", "hadoop") \
    .config("spark.sql.catalog.aws.warehouse", s3_path) \
    .config("spark.sql.catalog.aws.s3.endpoint", "s3.amazonaws.com") \
    .getOrCreate()



nasa_firms_url = "https://firms.modaps.eosdis.nasa.gov/data/country/modis/2023/modis_2023_United_States.csv"

# Download the CSV file
response = requests.get(nasa_firms_url)
csv_data = StringIO(response.text)

# Read the CSV data into a Spark DataFrame
df = spark.read.csv(csv_data, header=True, inferSchema=True)

# Convert the timestamp column to a date
df = df.withColumn("acq_date", to_date("acq_date", "yyyy-MM-dd")) \
       .withColumn("year", year("acq_date")) \
       .withColumn("month", month("acq_date")) \
       .withColumn("day", dayofmonth("acq_date"))

# Write the data to an Iceberg table
table_name = "aws.db.nasa_firms_data"
(df.writeTo(table_name)
   .partitionBy("year", "month", "day")
   .tableProperty("write.format.default", "parquet")
   .tableProperty("write.parquet.compression-codec", "snappy")
   .createOrReplace())

# Stop the Spark session
spark.stop()
