# Reads the country codes data , cleans the data 
# Write the  data  to S3 in parquet

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType,LongType
from datetime import datetime, timedelta, date
from pyspark.sql.functions import udf, regexp_replace,dayofmonth, min, max,count,dayofweek, date_format, trim

df_country=spark.read.format("csv").option("header","true").option("inferSchema","true").load('s3://udacity-immigration-project/input/country_codes.csv').drop_duplicates()

df_country = df_country.withColumn('Country_code',df_country.Country_code.cast(IntegerType())).drop_duplicates(subset=['Country_code'])

df_country.write.parquet('s3://udacity-immigration-project/output/country_codes', 'overwrite')