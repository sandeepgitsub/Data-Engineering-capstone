# Reads the city demographics  data , cleans the data 
# Write the  data  to S3 in parquet

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType,LongType
from datetime import datetime, timedelta, date
from pyspark.sql.functions import udf, regexp_replace,dayofmonth, min, max,count,dayofweek, date_format, trim

df_cities_demo=spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").\
load('s3://udacity-immigration-project/input/us-cities-demographics.csv').drop_duplicates()

df_cities_demo  = df_cities_demo.withColumn('State Code',regexp_replace(df_cities_demo['State Code'], "[^A-Z]", ""))

df_cities_demo= df_cities_demo.select('City','State',
	df_cities_demo['State Code'].alias('State_Code'),
	df_cities_demo['Median Age'].alias('Median_Age'),
	df_cities_demo['Male Population'].alias('Male_Population'),
	df_cities_demo['Female Population'].alias('Female_Population'),
	df_cities_demo['Total Population'].alias('Total_Population'),
	df_cities_demo['Number of Veterans'].alias('Number_of_Veterans'),
	df_cities_demo['Average Household Size'].alias('Average_Household_Size')).\
	drop_duplicates(subset=['City','State'])

df_cities_demo.write.parquet('s3://udacity-immigration-project/output/cities_demographics', 'overwrite')