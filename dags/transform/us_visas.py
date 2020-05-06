# Reads the us visa  data , cleans the data 
# Write the  data  to S3 in parquet

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType,LongType
from datetime import datetime, timedelta, date
from pyspark.sql.functions import udf, regexp_replace,dayofmonth, min, max,count,dayofweek, date_format, trim

# removes any non ascii characters
def only_ascii(column):
    return column.encode('ascii','ignore').decode('ascii')
ascii_udf = udf(only_ascii)


df_visas=spark.read.format("csv").option("header","true").option("inferSchema","true").load('s3://udacity-immigration-project/input/US_Visas.csv').drop_duplicates()

df_visas= df_visas.withColumn('Visa_Type',regexp_replace(df_visas['Visa Type'],'[^A-Z0-9]',''))

df_visas = df_visas.withColumn('Visa_Purpose',ascii_udf(df_visas.Purpose))

df_visas=df_visas.select('Visa_Type','Visa_Purpose').drop_duplicates(subset=['Visa_Type'])

df_visas.write.parquet('s3://udacity-immigration-project/output/US_Visas', 'overwrite')