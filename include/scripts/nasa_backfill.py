from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, month, dayofmonth
import requests
from io import StringIO

spark = SparkSession.builder.appName("NASA Backfill").getOrCreate()


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
# Write data to parquet
output_path = "s3://fire-mitigation-data/nasa_firms/backfill/nasa_firms_2023_United_States.parquet"
df.write \
    .partitionBy("year", "month", "day") \
    .parquet(output_path, mode="overwrite")

# Stop the Spark session
spark.stop()
