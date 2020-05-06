## Collect the stats by joining twith fact table and write to S3 in parquet

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType,LongType
from datetime import datetime, timedelta, date
from pyspark.sql.functions import udf, regexp_replace,dayofmonth, min, max,count,dayofweek, date_format, trim, isnull,sum,desc

path ='s3://udacity-immigration-project/output/'


df_immigration = spark.read.parquet(path  + 'immigration_fact')

## Read dates , gather stats and write the data in parquet
df_dates = spark.read.parquet(path + 'arrival_dates')
## Weekk day Stats
weekday_stats = df_immigration.join(df_dates, df_immigration.arrival_date ==  df_dates.arrival_date,'left_outer').groupBy(df_dates.arrival_date_year,'weekday').\
agg(sum('count').alias('sum_count')).orderBy(desc('sum_count'))

weekday_stats.write.partitionBy('arrival_date_year').parquet('s3://udacity-immigration-project/summary-stats/weekday', 'append')

## Read Visas , gather stats and write the data in parquet

df_visas = spark.read.parquet(path + 'US_Visas')

visa_stats = df_immigration.join(df_visas, df_immigration.visatype ==  df_visas.Visa_Type,'left_outer').groupBy('arrival_date_year','Visa_Type','Visa_Purpose').\
agg(sum('count').alias('sum_count')).orderBy(desc('sum_count'))

visa_stats.write.partitionBy('arrival_date_year').parquet('s3://udacity-immigration-project/summary-stats/visa', 'append')


## Read Country Codes , gather stats and write the data in parquet
df_country_codes = spark.read.parquet(path + 'country_codes')

country_stats = df_immigration.join(df_country_codes, df_immigration.country_of_residence_cd ==  df_country_codes.Country_code,'left_outer').groupBy('arrival_date_year','Country').\
agg(sum('count').alias('sum_count')).orderBy(desc('sum_count'))

country_stats.write.partitionBy('arrival_date_year').parquet('s3://udacity-immigration-project/summary-stats/country', 'append')

## Get the city stats

city_stats = df_immigration.groupBy('arrival_date_year','City','State').\
agg(sum('count').alias('sum_count')).orderBy(desc('sum_count'))

city_stats.write.partitionBy('arrival_date_year').parquet('s3://udacity-immigration-project/summary-stats/city', 'append')