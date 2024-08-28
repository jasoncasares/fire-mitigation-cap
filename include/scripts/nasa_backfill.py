from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, month, dayofmonth
import requests
import tempfile
import os
from io import StringIO

# S3 path for Iceberg data
s3_bucket = "fire-mitigation-data"
s3_path = f"s3://{s3_bucket}/nasa_firms"

spark = SparkSession.builder \
    .appName("NASA FIRMS Data Processing") \
    .getOrCreate()



nasa_firms_url = "https://firms.modaps.eosdis.nasa.gov/data/country/modis/2023/modis_2023_United_States.csv"

# Download the data and save it to a temporary file
response = requests.get(nasa_firms_url)
with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', mode='wb') as temp_file:
    temp_file.write(response.content)
    temp_file_path = temp_file.name

print(f"Temporary file path: {temp_file_path}")

# Read the CSV data into a Spark DataFrame
df = spark.read.csv(temp_file_path, header=True, inferSchema=True)


# Convert the timestamp column to a date
df = df.withColumn("acq_date", to_date("acq_date", "yyyy-MM-dd")) \
       .withColumn("year", year("acq_date")) \
       .withColumn("month", month("acq_date")) \
       .withColumn("day", dayofmonth("acq_date"))

# Write the data to an Iceberg table
table_name = "iceberg.nasa_firms_data"
(df.writeTo(table_name)
   .partitionedBy("year", "month", "day")
   .tableProperty("write.format.default", "parquet")
   .tableProperty("write.parquet.compression-codec", "snappy")
   .createOrReplace())

# Delete the temporary file
os.unlink(temp_file_path)

# Read back and show some data to verify
spark.table(table_name).show(5)

# Stop the Spark session
spark.stop()
