# Reads the immigration data , cleans the data 
# using the immigration data create the dates dataframe
# Join immigration data with airport data to get the city and states
# Write the imigration data and dates to S3 in parquet

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType,LongType
from datetime import datetime, timedelta, date
from pyspark.sql.functions import udf, regexp_replace,dayofmonth, min, max,count,dayofweek, date_format, trim
import calendar

# Function to convert the sas date to normal date format
convert_sasdt_dt = udf(lambda x : str(date(1960,1,1) + timedelta(days=x)))

# Function to categoize the age in to differnet groups
def categorizer(age):
    if age:
        if age < 1:
            return "infant"
        elif age < 13:
            return "children"    
        elif age <= 18:
            return "teens"
        elif age < 65:
            return "adult"
        else: 
            return "elderly"
    else:
        return 'NA'
bucket_udf = udf(categorizer, StringType())

# Read the airport file, clean it
df_airports=spark.read.format("csv").option("header","true").option("inferSchema","true").load('s3://udacity-immigration-project/input/port_of_entry.csv').drop_duplicates()
df_airports= df_airports.withColumn('Code',regexp_replace(df_airports['Code'], "[^A-Z0-9]", "")).\
withColumn('City',regexp_replace(df_airports['City'], "[^A-Z,a-z ()0-9]", "")).\
withColumn('State',regexp_replace(df_airports['State'], "[^A-Za-z 0-9]", ""))


df_airports = df_airports.withColumn('City',regexp_replace(df_airports["City"], "  ", " ")).\
withColumn('State',regexp_replace(df_airports["State"], "  ", " "))

df_airports = df_airports.select('Code',trim(df_airports.City).alias('City'),trim(df_airports.State).alias('State')).drop_duplicates(subset=['Code'])


# Read the immigration for each month of the year passed , clean the data , creates dates df , join with airport and write the dates and immigration to S3 in parquet format
for month in [calendar.month_name[i][:3].lower() for i in range(1,13)]:
    df_spark =spark.read.format('com.github.saurfang.sas.spark').load('s3://udacity-immigration-project/input/immigration-data/'+ str(year) +'/i94_' + month + str(year[2:]) +'_sub.sas7bdat')

    df_spark= df_spark.withColumn('i94mode', df_spark["i94mode"].cast(IntegerType())).filter(df_spark.i94mode == 1)

    df_spark =df_spark.select('i94res','i94port','arrdate','i94yr','i94mon','i94bir','visatype')

    df_spark  = df_spark.drop_duplicates().withColumn('i94port',regexp_replace(df_spark['i94port'], "[^A-Z0-9]", ""))\
    .withColumn('visatype',regexp_replace(df_spark['visatype'], "[^A-Z0-9]", ""))

    df_spark= df_spark.withColumn('country_of_residence_cd', df_spark["i94res"].cast(IntegerType()))\
    .withColumn('port_of_entry', df_spark["i94port"].cast(StringType()))\
    .withColumn('arrival_date_year', df_spark["i94yr"].cast(IntegerType()))\
    .withColumn('arrival_date_month', df_spark["i94mon"].cast(IntegerType()))\
    .withColumn('arrdate', df_spark["arrdate"].cast(IntegerType()))\
    .withColumn('i94bir', df_spark["i94bir"].cast(IntegerType()))

    df_spark =df_spark.select('country_of_residence_cd','port_of_entry','arrdate','i94bir','visatype','arrival_date_year','arrival_date_month')

    df_spark= df_spark.withColumn('arrival_date', convert_sasdt_dt(df_spark['arrdate']))


    df_spark= df_spark.withColumn('age_group', bucket_udf(df_spark['i94bir']))

    ## Create the date dataframe and write to S3
    dates_dt=df_spark.select('arrival_date','arrival_date_year','arrival_date_month').drop_duplicates(subset=['arrival_date'])

    dates_dt = dates_dt.select('arrival_date','arrival_date_year','arrival_date_month',dayofmonth('arrival_date').alias('arrival_date_day'),
                                                                date_format('arrival_date','E').alias('weekday'))

    dates_dt.write.parquet('s3://udacity-immigration-project/output/arrival_dates', 'append')

    ## Join with airport df and write to S3
    df_spark = df_spark.join(df_airports, (df_spark.port_of_entry ==df_airports.Code),'left_outer' )

    final_df = df_spark.groupBy('country_of_residence_cd','port_of_entry','visatype','arrival_date','arrival_date_year','arrival_date_month','age_group','City','State').count()

    final_df.write.partitionBy('arrival_date_year','arrival_date_month').parquet('s3://udacity-immigration-project/output/immigration_fact', 'append')